"""Integration tests for the Hierarchical Snorkel Pipeline.

Uses the device classification concept from rules_and_lfs.yml as reference:
  - Root concept: "Devices"
  - Level 1 CVs: STATIC, LAPTOP, PHONE (flat classification)
  - Level 2 CVs (hypothetical): PERSONAL_LAPTOP, WORK_LAPTOP (under LAPTOP)

Tests exercise the full hierarchical API:
  Phase 1 — CV tree (parent_cv_id, tree endpoint)
  Phase 2 — SQL + derived indexes, SnorkelJob JSONB storage
  Phase 3 — Derived index API (create, filter, entities)
  Phase 5 — Staleness and cascade invalidation

NOTE: Dagster materializations are mocked via mock_dagster fixture.
      Snorkel jobs are set to COMPLETED directly in the DB for
      downstream derived-index tests.
"""
import pytest
from datetime import datetime, timezone


# ============================================================================
# Helpers
# ============================================================================

def _create_concept(client, name="Devices", description="Device classification"):
    """Create a concept and return its response dict."""
    resp = client.post("/concepts/", json={"name": name, "description": description})
    assert resp.status_code == 201, resp.text
    return resp.json()


def _create_cv(client, c_id, name, parent_cv_id=None, level=1):
    """Create a concept value and return its response dict."""
    body = {"name": name, "level": level}
    if parent_cv_id is not None:
        body["parent_cv_id"] = parent_cv_id
    resp = client.post(f"/concepts/{c_id}/values", json=body)
    assert resp.status_code == 201, resp.text
    return resp.json()


def _create_db_connection(client, name="tippers_test_conn"):
    """Create a database connection and return its response dict."""
    resp = client.post("/database-connections", json={
        "name": name,
        "connection_type": "postgresql",
        "host": "localhost",
        "port": 5432,
        "database": "tippers",
        "user": "postgres",
        "password": "postgres",
    })
    assert resp.status_code == 201, resp.text
    return resp.json()


def _create_sql_index(client, c_id, conn_id, name="device_mac_index"):
    """Create a SQL index referencing the device mac_address query."""
    resp = client.post(f"/concepts/{c_id}/indexes", json={
        "name": name,
        "conn_id": conn_id,
        "sql_query": "SELECT DISTINCT mac_address FROM user_location_trajectory LIMIT 1000",
    })
    assert resp.status_code == 201, resp.text
    return resp.json()


def _mark_index_materialized(db, index_id):
    """Mark an index as materialized directly in the DB.

    The rules router requires `is_materialized=True` on the parent index
    before allowing rule creation.  In integration tests we skip the actual
    Dagster materialization, so we set the flag manually.
    """
    from backend.db.models import ConceptIndex
    idx = db.query(ConceptIndex).filter(ConceptIndex.index_id == index_id).first()
    idx.is_materialized = True
    idx.storage_path = f"indexes/index_{index_id}.parquet"
    idx.row_count = 100
    db.flush()


def _create_rule(client, c_id, index_id, name, sql_query):
    """Create a rule and return its response dict."""
    resp = client.post(f"/concepts/{c_id}/rules", json={
        "name": name,
        "index_id": index_id,
        "sql_query": sql_query,
        "index_column": "mac_address",
    })
    assert resp.status_code == 201, resp.text
    return resp.json()


def _create_lf(client, c_id, rule_id, name, cv_ids, code):
    """Create a labeling function and return its response dict."""
    resp = client.post(f"/concepts/{c_id}/labeling-functions", json={
        "name": name,
        "rule_id": rule_id,
        "applicable_cv_ids": cv_ids,
        "code": code,
        "allowed_imports": [],
    })
    assert resp.status_code == 201, resp.text
    return resp.json()


# ============================================================================
# Phase 1 — CV Tree
# ============================================================================

class TestConceptValueTree:
    """Tests for hierarchical concept values with parent_cv_id."""

    def test_create_flat_cvs(self, client):
        """Level-1 CVs (no parent) should be created normally."""
        concept = _create_concept(client)
        c_id = concept["c_id"]

        static = _create_cv(client, c_id, "STATIC")
        laptop = _create_cv(client, c_id, "LAPTOP")
        phone = _create_cv(client, c_id, "PHONE")

        assert static["parent_cv_id"] is None
        assert laptop["parent_cv_id"] is None
        assert phone["parent_cv_id"] is None

    def test_create_nested_cvs(self, client):
        """Level-2 CVs under LAPTOP should reference parent_cv_id."""
        concept = _create_concept(client)
        c_id = concept["c_id"]

        laptop = _create_cv(client, c_id, "LAPTOP")
        personal = _create_cv(client, c_id, "PERSONAL_LAPTOP", parent_cv_id=laptop["cv_id"], level=2)
        work = _create_cv(client, c_id, "WORK_LAPTOP", parent_cv_id=laptop["cv_id"], level=2)

        assert personal["parent_cv_id"] == laptop["cv_id"]
        assert work["parent_cv_id"] == laptop["cv_id"]
        assert personal["level"] == 2
        assert work["level"] == 2

    def test_invalid_parent_cv_id_rejected(self, client):
        """Parent CV must exist in the same concept."""
        concept = _create_concept(client)
        c_id = concept["c_id"]

        resp = client.post(f"/concepts/{c_id}/values", json={
            "name": "ORPHAN",
            "parent_cv_id": 99999,
        })
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()

    def test_tree_endpoint_flat(self, client):
        """GET /values/tree returns flat CVs as root-level nodes."""
        concept = _create_concept(client)
        c_id = concept["c_id"]

        _create_cv(client, c_id, "STATIC")
        _create_cv(client, c_id, "LAPTOP")
        _create_cv(client, c_id, "PHONE")

        resp = client.get(f"/concepts/{c_id}/values/tree")
        assert resp.status_code == 200

        tree = resp.json()
        assert len(tree) == 3
        # All are root nodes — no children
        for node in tree:
            assert node["children"] == []

    def test_tree_endpoint_nested(self, client):
        """GET /values/tree returns nested structure for hierarchical CVs."""
        concept = _create_concept(client)
        c_id = concept["c_id"]

        static = _create_cv(client, c_id, "STATIC")
        laptop = _create_cv(client, c_id, "LAPTOP")
        phone = _create_cv(client, c_id, "PHONE")
        _create_cv(client, c_id, "PERSONAL_LAPTOP", parent_cv_id=laptop["cv_id"], level=2)
        _create_cv(client, c_id, "WORK_LAPTOP", parent_cv_id=laptop["cv_id"], level=2)

        resp = client.get(f"/concepts/{c_id}/values/tree")
        assert resp.status_code == 200

        tree = resp.json()
        # 3 root nodes: STATIC, LAPTOP, PHONE
        assert len(tree) == 3

        # Find LAPTOP node and check children
        laptop_node = next(n for n in tree if n["name"] == "LAPTOP")
        assert len(laptop_node["children"]) == 2
        child_names = {c["name"] for c in laptop_node["children"]}
        assert child_names == {"PERSONAL_LAPTOP", "WORK_LAPTOP"}

        # STATIC and PHONE have no children
        static_node = next(n for n in tree if n["name"] == "STATIC")
        phone_node = next(n for n in tree if n["name"] == "PHONE")
        assert static_node["children"] == []
        assert phone_node["children"] == []


# ============================================================================
# Phase 2 — SQL Index + Derived Index Basics
# ============================================================================

class TestSQLIndex:
    """Tests for SQL index creation with the new source_type field."""

    def test_sql_index_has_source_type_sql(self, client):
        """SQL indexes should get source_type='sql' by default."""
        concept = _create_concept(client)
        conn = _create_db_connection(client)
        idx = _create_sql_index(client, concept["c_id"], conn["conn_id"])

        assert idx["source_type"] == "sql"
        assert idx["sql_query"] is not None
        assert idx["cv_id"] is None
        assert idx["parent_index_id"] is None
        assert idx["parent_snorkel_job_id"] is None

    def test_sql_index_requires_conn_id(self, client):
        """SQL indexes must have conn_id."""
        concept = _create_concept(client)

        resp = client.post(f"/concepts/{concept['c_id']}/indexes", json={
            "name": "bad_index",
            "sql_query": "SELECT 1",
        })
        assert resp.status_code == 400
        assert "conn_id" in resp.json()["detail"].lower()

    def test_sql_index_requires_sql_query(self, client):
        """SQL indexes must have sql_query."""
        concept = _create_concept(client)
        conn = _create_db_connection(client)

        resp = client.post(f"/concepts/{concept['c_id']}/indexes", json={
            "name": "bad_index",
            "conn_id": conn["conn_id"],
        })
        assert resp.status_code == 400
        assert "sql_query" in resp.json()["detail"].lower()


# ============================================================================
# Phase 2+3 — Derived Indexes
# ============================================================================

class TestDerivedIndex:
    """Tests for derived index creation (root and child)."""

    def test_root_derived_index(self, client):
        """A root derived index points to a SQL index (no label_filter)."""
        concept = _create_concept(client)
        c_id = concept["c_id"]
        conn = _create_db_connection(client)
        sql_idx = _create_sql_index(client, c_id, conn["conn_id"])
        cv = _create_cv(client, c_id, "ALL_DEVICES")

        resp = client.post(f"/concepts/{c_id}/indexes/derived", json={
            "name": "root_derived_devices",
            "cv_id": cv["cv_id"],
            "parent_index_id": sql_idx["index_id"],
        })
        assert resp.status_code == 201

        derived = resp.json()
        assert derived["source_type"] == "derived"
        assert derived["cv_id"] == cv["cv_id"]
        assert derived["parent_index_id"] == sql_idx["index_id"]
        assert derived["parent_snorkel_job_id"] is None
        assert derived["label_filter"] is None
        assert derived["is_materialized"] is True  # derived = always materialized

    def test_derived_index_requires_cv_in_concept(self, client):
        """cv_id must belong to the same concept."""
        concept1 = _create_concept(client, name="Concept1")
        concept2 = _create_concept(client, name="Concept2")
        conn = _create_db_connection(client)
        sql_idx = _create_sql_index(client, concept1["c_id"], conn["conn_id"])
        cv = _create_cv(client, concept2["c_id"], "WRONG_CONCEPT_CV")

        resp = client.post(f"/concepts/{concept1['c_id']}/indexes/derived", json={
            "name": "bad_derived",
            "cv_id": cv["cv_id"],
            "parent_index_id": sql_idx["index_id"],
        })
        assert resp.status_code == 404
        assert "not found" in resp.json()["detail"].lower()

    def test_derived_index_requires_exactly_one_parent(self, client):
        """Must set either parent_index_id or parent_snorkel_job_id, not both."""
        concept = _create_concept(client)
        c_id = concept["c_id"]
        cv = _create_cv(client, c_id, "CV1")

        # Neither parent
        resp = client.post(f"/concepts/{c_id}/indexes/derived", json={
            "name": "no_parent",
            "cv_id": cv["cv_id"],
        })
        assert resp.status_code == 400

        # Both parents
        resp = client.post(f"/concepts/{c_id}/indexes/derived", json={
            "name": "both_parents",
            "cv_id": cv["cv_id"],
            "parent_index_id": 1,
            "parent_snorkel_job_id": 1,
        })
        assert resp.status_code == 400

    def test_child_derived_requires_completed_snorkel_job(self, client, db):
        """Child derived indexes need a COMPLETED parent Snorkel job."""
        from backend.db.models import SnorkelJob

        concept = _create_concept(client)
        c_id = concept["c_id"]
        conn = _create_db_connection(client)
        sql_idx = _create_sql_index(client, c_id, conn["conn_id"])
        cv = _create_cv(client, c_id, "STATIC")

        # Create an incomplete Snorkel job directly in DB
        job = SnorkelJob(
            c_id=c_id,
            index_id=sql_idx["index_id"],
            status="RUNNING",  # Not completed!
            output_type="softmax",
        )
        db.add(job)
        db.flush()

        resp = client.post(f"/concepts/{c_id}/indexes/derived", json={
            "name": "premature_derived",
            "cv_id": cv["cv_id"],
            "parent_snorkel_job_id": job.job_id,
        })
        assert resp.status_code == 400
        assert "completed" in resp.json()["detail"].lower()


# ============================================================================
# Phase 2 — Rules (referencing rules_and_lfs.yml)
# ============================================================================

class TestRulesFromYAML:
    """Tests that rules matching rules_and_lfs.yml can be created."""

    def test_create_all_three_rules(self, client, db):
        """Create the 3 device rules from rules_and_lfs.yml."""
        concept = _create_concept(client)
        c_id = concept["c_id"]
        conn = _create_db_connection(client)
        sql_idx = _create_sql_index(client, c_id, conn["conn_id"])
        idx_id = sql_idx["index_id"]
        _mark_index_materialized(db, idx_id)

        # Rule 1: static_short_irregular_sessions_rule
        r1 = _create_rule(client, c_id, idx_id, "static_short_irregular_sessions_rule",
            "WITH connections AS (SELECT mac_address, SUM(CASE WHEN EXTRACT(EPOCH FROM (end_time - start_time)) / 3600 <= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS connection_count, COUNT(DISTINCT(space_id)) AS distinct_ap FROM user_location_trajectory WHERE mac_address IN (:index_values) GROUP BY mac_address) SELECT mac_address, connection_count, distinct_ap FROM connections;"
        )
        assert r1["name"] == "static_short_irregular_sessions_rule"
        assert r1["index_column"] == "mac_address"

        # Rule 2: laptop_total_connected_duration_rule
        r2 = _create_rule(client, c_id, idx_id, "laptop_total_connected_duration_rule",
            "WITH total_sessions AS (SELECT * FROM user_location_trajectory WHERE mac_address IN (:index_values)), valid_connection_percentage AS (SELECT mac_address, COUNT(*) AS percentage FROM total_sessions GROUP BY mac_address) SELECT mac_address, percentage FROM valid_connection_percentage;"
        )
        assert r2["name"] == "laptop_total_connected_duration_rule"

        # Rule 3: phone_1_hour_average_rule
        r3 = _create_rule(client, c_id, idx_id, "phone_1_hour_average_rule",
            "WITH total_sessions AS (SELECT * FROM user_location_trajectory WHERE mac_address IN (:index_values)), qualified_users AS (SELECT mac_address, COUNT(DISTINCT DATE(start_time)) AS total_days FROM total_sessions GROUP BY mac_address) SELECT mac_address, total_days FROM qualified_users;"
        )
        assert r3["name"] == "phone_1_hour_average_rule"


# ============================================================================
# Phase 2 — Labeling Functions
# ============================================================================

class TestLabelingFunctions:
    """Tests that LFs matching rules_and_lfs.yml can be created and approved."""

    def test_create_and_approve_lf(self, client, db):
        """Create a LF with custom code and approve it."""
        concept = _create_concept(client)
        c_id = concept["c_id"]
        conn = _create_db_connection(client)
        sql_idx = _create_sql_index(client, c_id, conn["conn_id"])
        _mark_index_materialized(db, sql_idx["index_id"])

        static_cv = _create_cv(client, c_id, "STATIC")
        laptop_cv = _create_cv(client, c_id, "LAPTOP")
        phone_cv = _create_cv(client, c_id, "PHONE")
        all_cv_ids = [static_cv["cv_id"], laptop_cv["cv_id"], phone_cv["cv_id"]]

        rule = _create_rule(client, c_id, sql_idx["index_id"],
            "static_short_irregular_sessions_rule",
            "SELECT mac_address, 0.5 AS connection_count, 1 AS distinct_ap FROM user_location_trajectory WHERE mac_address IN (:index_values) LIMIT 10"
        )

        lf = _create_lf(client, c_id, rule["r_id"],
            "static_short_irregular_sessions_lf",
            all_cv_ids,
            "STATIC = 1\nABSTAIN = -1\n\ndef labeling_function(row):\n    if row['connection_count'] >= 0.8 and row['distinct_ap'] == 1:\n        return STATIC\n    return ABSTAIN\n"
        )

        assert lf["name"] == "static_short_irregular_sessions_lf"
        assert lf["applicable_cv_ids"] == all_cv_ids
        assert lf["lf_type"] == "custom"

        # Approve the LF
        resp = client.post(f"/concepts/{c_id}/labeling-functions/{lf['lf_id']}/approve")
        assert resp.status_code == 200
        approved = resp.json()
        assert approved["requires_approval"] is False


# ============================================================================
# Phase 2 — Snorkel Job (DB-level, mock Dagster)
# ============================================================================

class TestSnorkelJobSetup:
    """Tests for Snorkel job creation — validates that Level 1 must complete
    before Level 2 derived indexes can be created."""

    def test_snorkel_run_requires_materialized_assets(self, client, db, mock_dagster):
        """Snorkel training rejects unmaterialized rules.

        The index must be materialized for rule creation, but
        rules themselves are NOT materialized — Snorkel should reject.
        """
        concept = _create_concept(client)
        c_id = concept["c_id"]
        conn = _create_db_connection(client)
        sql_idx = _create_sql_index(client, c_id, conn["conn_id"])
        _mark_index_materialized(db, sql_idx["index_id"])

        static_cv = _create_cv(client, c_id, "STATIC")
        laptop_cv = _create_cv(client, c_id, "LAPTOP")
        phone_cv = _create_cv(client, c_id, "PHONE")

        rule = _create_rule(client, c_id, sql_idx["index_id"],
            "test_rule", "SELECT mac_address FROM user_location_trajectory WHERE mac_address IN (:index_values)")

        lf = _create_lf(client, c_id, rule["r_id"], "test_lf",
            [static_cv["cv_id"], laptop_cv["cv_id"], phone_cv["cv_id"]],
            "ABSTAIN=-1\ndef labeling_function(row):\n    return ABSTAIN\n")

        # Approve the LF first
        client.post(f"/concepts/{c_id}/labeling-functions/{lf['lf_id']}/approve")

        # Try to run Snorkel — rules are NOT materialized
        resp = client.post(f"/concepts/{c_id}/snorkel/run", json={
            "selectedIndex": sql_idx["index_id"],
            "selectedRules": [rule["r_id"]],
            "selectedLFs": [lf["lf_id"]],
            "snorkel": {"epochs": 10, "lr": 0.01, "output_type": "softmax"},
        })
        assert resp.status_code == 400
        assert "not materialized" in resp.json()["detail"].lower()

    def test_ordering_constraint_level1_before_level2(self, client, db):
        """Level 2 derived index CANNOT be created until Level 1 Snorkel job
        is COMPLETED. This is the key ordering constraint."""
        from backend.db.models import SnorkelJob

        concept = _create_concept(client)
        c_id = concept["c_id"]
        conn = _create_db_connection(client)
        sql_idx = _create_sql_index(client, c_id, conn["conn_id"])

        # Level 1 CVs
        static_cv = _create_cv(client, c_id, "STATIC")
        laptop_cv = _create_cv(client, c_id, "LAPTOP")

        # Level 2 CVs
        personal_cv = _create_cv(client, c_id, "PERSONAL_LAPTOP",
                                  parent_cv_id=laptop_cv["cv_id"], level=2)

        # Create a PENDING (not completed) Snorkel job
        pending_job = SnorkelJob(
            c_id=c_id,
            index_id=sql_idx["index_id"],
            status="PENDING",
            output_type="softmax",
        )
        db.add(pending_job)
        db.flush()

        # Attempt Level 2 derived index with the PENDING job → should FAIL
        resp = client.post(f"/concepts/{c_id}/indexes/derived", json={
            "name": "laptop_derived_idx",
            "cv_id": personal_cv["cv_id"],
            "parent_snorkel_job_id": pending_job.job_id,
        })
        assert resp.status_code == 400
        assert "completed" in resp.json()["detail"].lower()

        # Now complete the Snorkel job
        pending_job.status = "COMPLETED"
        pending_job.completed_at = datetime.now(timezone.utc)
        pending_job.result_path = "snorkel_jobs/job_test.parquet"
        pending_job.cv_id_to_index = {"1": 0, "2": 1}
        pending_job.cv_id_to_name = {"1": "STATIC", "2": "LAPTOP"}
        db.flush()

        # Now Level 2 derived index should succeed
        resp = client.post(f"/concepts/{c_id}/indexes/derived", json={
            "name": "laptop_derived_idx",
            "cv_id": personal_cv["cv_id"],
            "parent_snorkel_job_id": pending_job.job_id,
            "label_filter": {"labels": {str(laptop_cv["cv_id"]): {"min_confidence": 0.8}}},
        })
        assert resp.status_code == 201

        derived = resp.json()
        assert derived["source_type"] == "derived"
        assert derived["parent_snorkel_job_id"] == pending_job.job_id
        assert derived["label_filter"] is not None


# ============================================================================
# Phase 3 — Label Filter Updates
# ============================================================================

class TestLabelFilterUpdate:
    """Tests for PATCH /{c_id}/indexes/{id}/filter endpoint."""

    def test_update_label_filter_on_derived_index(self, client, db):
        """Updating label_filter should succeed on derived indexes with parent_snorkel_job_id."""
        from backend.db.models import SnorkelJob

        concept = _create_concept(client)
        c_id = concept["c_id"]
        conn = _create_db_connection(client)
        sql_idx = _create_sql_index(client, c_id, conn["conn_id"])
        cv = _create_cv(client, c_id, "STATIC")

        # Create a completed Snorkel job
        job = SnorkelJob(
            c_id=c_id,
            index_id=sql_idx["index_id"],
            status="COMPLETED",
            output_type="softmax",
            result_path="snorkel_jobs/job_filter_test.parquet",
            cv_id_to_index={"1": 0},
            cv_id_to_name={"1": "STATIC"},
        )
        db.add(job)
        db.flush()

        # Create derived index
        resp = client.post(f"/concepts/{c_id}/indexes/derived", json={
            "name": "static_derived",
            "cv_id": cv["cv_id"],
            "parent_snorkel_job_id": job.job_id,
            "label_filter": {"labels": {str(cv["cv_id"]): {"min_confidence": 0.5}}},
        })
        assert resp.status_code == 201
        derived_id = resp.json()["index_id"]

        # Update the filter to raise confidence threshold
        resp = client.patch(f"/concepts/{c_id}/indexes/{derived_id}/filter", json={
            "label_filter": {"labels": {str(cv["cv_id"]): {"min_confidence": 0.9}}},
        })
        assert resp.status_code == 200
        updated = resp.json()
        assert updated["label_filter"]["labels"][str(cv["cv_id"])]["min_confidence"] == 0.9

    def test_label_filter_rejected_on_sql_index(self, client):
        """Cannot set label_filter on a SQL index."""
        concept = _create_concept(client)
        c_id = concept["c_id"]
        conn = _create_db_connection(client)
        sql_idx = _create_sql_index(client, c_id, conn["conn_id"])

        resp = client.patch(f"/concepts/{c_id}/indexes/{sql_idx['index_id']}/filter", json={
            "label_filter": {"labels": {"1": {"min_confidence": 0.5}}},
        })
        assert resp.status_code == 400
        assert "derived" in resp.json()["detail"].lower()


# ============================================================================
# Phase 2 — SnorkelJob JSONB Storage
# ============================================================================

class TestSnorkelJobJSONBStorage:
    """Tests that Snorkel job metadata is stored in JSONB columns."""

    def test_snorkel_job_jsonb_columns_populated(self, client, db):
        """JSONB columns on snorkel_jobs should be queryable."""
        from backend.db.models import SnorkelJob

        concept = _create_concept(client)
        c_id = concept["c_id"]
        conn = _create_db_connection(client)
        sql_idx = _create_sql_index(client, c_id, conn["conn_id"])

        # Simulate a completed Snorkel job with JSONB metadata
        job = SnorkelJob(
            c_id=c_id,
            index_id=sql_idx["index_id"],
            status="COMPLETED",
            output_type="softmax",
            result_path="snorkel_jobs/job_999.parquet",
            lf_summary=[
                {"name": "static_lf", "coverage": 0.45, "accuracy": 0.82},
                {"name": "laptop_lf", "coverage": 0.38, "accuracy": 0.78},
            ],
            class_distribution={"STATIC": 312, "LAPTOP": 245, "PHONE": 443},
            overall_stats={"total_samples": 1000, "labeled": 820, "abstain": 180},
            cv_id_to_index={"1": 0, "2": 1, "3": 2},
            cv_id_to_name={"1": "STATIC", "2": "LAPTOP", "3": "PHONE"},
        )
        db.add(job)
        db.flush()

        # Verify via GET endpoint
        resp = client.get(f"/concepts/{c_id}/snorkel/jobs/{job.job_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "COMPLETED"
        assert data["result_path"].endswith(".parquet")

    def test_snorkel_results_reads_jsonb(self, client, db):
        """GET results endpoint should read metadata from JSONB columns."""
        from backend.db.models import SnorkelJob

        concept = _create_concept(client)
        c_id = concept["c_id"]
        conn = _create_db_connection(client)
        sql_idx = _create_sql_index(client, c_id, conn["conn_id"])

        job = SnorkelJob(
            c_id=c_id,
            index_id=sql_idx["index_id"],
            status="COMPLETED",
            output_type="softmax",
            result_path="snorkel_jobs/job_jsonb_test.parquet",
            lf_summary=[{"name": "test_lf", "coverage": 0.5, "accuracy": 0.9}],
            class_distribution={
                "label_matrix": {"A": 100, "B": 200},
                "model": {"A": 110, "B": 190},
            },
            overall_stats={"total_samples": 300},
            cv_id_to_index={"1": 0, "2": 1},
            cv_id_to_name={"1": "A", "2": "B"},
        )
        db.add(job)
        db.flush()

        resp = client.get(f"/concepts/{c_id}/snorkel/jobs/{job.job_id}/results")
        assert resp.status_code == 200
        results = resp.json()

        # Should return JSONB metadata without needing S3 download
        assert "lf_summary" in results
        assert "label_matrix_class_distribution" in results
        assert results["label_matrix_class_distribution"]["A"] == 100
        assert results["overall_stats"]["total_samples"] == 300


# ============================================================================
# Phase 5 — Staleness Detection
# ============================================================================

class TestStaleness:
    """Tests for pipeline staleness and cascade invalidation."""

    def test_staleness_endpoint_returns_structure(self, client):
        """GET staleness should return a structured response."""
        concept = _create_concept(client)
        c_id = concept["c_id"]

        resp = client.get(f"/concepts/{c_id}/pipeline/staleness")
        assert resp.status_code == 200

        data = resp.json()
        assert "indexes" in data
        assert "rules" in data

    def test_staleness_with_sql_index(self, client):
        """An unmaterialized SQL index should appear in staleness report."""
        concept = _create_concept(client)
        c_id = concept["c_id"]
        conn = _create_db_connection(client)
        sql_idx = _create_sql_index(client, c_id, conn["conn_id"])

        resp = client.get(f"/concepts/{c_id}/pipeline/staleness")
        assert resp.status_code == 200

        data = resp.json()
        assert len(data["indexes"]) == 1
        assert data["indexes"][0]["index_id"] == sql_idx["index_id"]

    def test_cascade_invalidation(self, client, db):
        """POST invalidate should cascade to downstream assets."""
        from backend.db.models import ConceptIndex, ConceptRule

        concept = _create_concept(client)
        c_id = concept["c_id"]
        conn = _create_db_connection(client)
        sql_idx = _create_sql_index(client, c_id, conn["conn_id"])
        _mark_index_materialized(db, sql_idx["index_id"])

        # Create a rule referencing the index
        rule = _create_rule(client, c_id, sql_idx["index_id"],
            "test_rule", "SELECT mac_address FROM some_table WHERE mac_address IN (:index_values)")

        # Mark rule as materialized directly in DB (simulates completed materialization)
        db_rule = db.query(ConceptRule).filter(ConceptRule.r_id == rule["r_id"]).first()
        db_rule.is_materialized = True
        db.flush()

        # Invalidate from the index — should cascade to the rule
        resp = client.post(f"/concepts/{c_id}/pipeline/invalidate/{sql_idx['index_id']}")
        assert resp.status_code == 200

        data = resp.json()
        assert data["invalidated_count"] >= 1

    def test_execute_pipeline_with_mock_dagster(self, client, mock_dagster):
        """POST execute should trigger Dagster jobs (mocked)."""
        concept = _create_concept(client)
        c_id = concept["c_id"]
        conn = _create_db_connection(client)
        sql_idx = _create_sql_index(client, c_id, conn["conn_id"])

        resp = client.post(
            f"/concepts/{c_id}/pipeline/execute",
            params={"index_id": sql_idx["index_id"]}
        )
        assert resp.status_code == 200

        data = resp.json()
        assert data["concept_id"] == c_id
        assert data["total_triggered"] >= 1

        # Verify Dagster was called
        assert mock_dagster.submit_job_execution.called


# ============================================================================
# End-to-End: Full Hierarchical Flow (API-level)
# ============================================================================

class TestEndToEndHierarchicalFlow:
    """Simulates the complete hierarchical pipeline setup via API:

    1. Create "Devices" concept
    2. Create Level 1 CVs: STATIC, LAPTOP, PHONE
    3. Create Level 2 CVs under LAPTOP: PERSONAL_LAPTOP, WORK_LAPTOP
    4. Create SQL index + DB connection
    5. Create root derived index → points to SQL index
    6. Create 3 rules (from rules_and_lfs.yml)
    7. Create 3 LFs and approve them
    8. Simulate Level 1 Snorkel job completing
    9. Create Level 2 derived index (LAPTOP branch) from Snorkel results
    10. Verify tree, derived indexes, and staleness all consistent
    """

    def test_full_hierarchical_setup(self, client, db):
        from backend.db.models import SnorkelJob, ConceptIndex

        # --- Step 1: Create concept ---
        concept = _create_concept(client, name="Devices", description="WiFi device classification")
        c_id = concept["c_id"]

        # --- Step 2: Create Level 1 CVs ---
        static_cv = _create_cv(client, c_id, "STATIC")
        laptop_cv = _create_cv(client, c_id, "LAPTOP")
        phone_cv = _create_cv(client, c_id, "PHONE")
        all_cv_ids = [static_cv["cv_id"], laptop_cv["cv_id"], phone_cv["cv_id"]]

        # --- Step 3: Create Level 2 CVs ---
        personal_cv = _create_cv(client, c_id, "PERSONAL_LAPTOP",
                                  parent_cv_id=laptop_cv["cv_id"], level=2)
        work_cv = _create_cv(client, c_id, "WORK_LAPTOP",
                              parent_cv_id=laptop_cv["cv_id"], level=2)

        # Verify tree
        tree_resp = client.get(f"/concepts/{c_id}/values/tree")
        assert tree_resp.status_code == 200
        tree = tree_resp.json()
        assert len(tree) == 3  # 3 root nodes
        laptop_node = next(n for n in tree if n["name"] == "LAPTOP")
        assert len(laptop_node["children"]) == 2

        # --- Step 4: Create DB connection + SQL index ---
        conn = _create_db_connection(client, name="e2e_tippers_conn")
        sql_idx = _create_sql_index(client, c_id, conn["conn_id"], name="device_macs")
        assert sql_idx["source_type"] == "sql"
        _mark_index_materialized(db, sql_idx["index_id"])

        # --- Step 5: Create root derived index ---
        root_cv = _create_cv(client, c_id, "ALL_DEVICES")  # Meta-CV for root
        root_derived_resp = client.post(f"/concepts/{c_id}/indexes/derived", json={
            "name": "root_all_devices",
            "cv_id": root_cv["cv_id"],
            "parent_index_id": sql_idx["index_id"],
        })
        assert root_derived_resp.status_code == 201
        root_derived = root_derived_resp.json()
        assert root_derived["source_type"] == "derived"
        assert root_derived["parent_index_id"] == sql_idx["index_id"]

        # --- Step 6: Create 3 rules ---
        r1 = _create_rule(client, c_id, sql_idx["index_id"],
            "static_sessions_rule",
            "SELECT mac_address, 0.9 AS connection_count, 1 AS distinct_ap FROM user_location_trajectory WHERE mac_address IN (:index_values)")
        r2 = _create_rule(client, c_id, sql_idx["index_id"],
            "laptop_duration_rule",
            "SELECT mac_address, 0.85 AS percentage FROM user_location_trajectory WHERE mac_address IN (:index_values)")
        r3 = _create_rule(client, c_id, sql_idx["index_id"],
            "phone_avg_rule",
            "SELECT mac_address, 5 AS total_days FROM user_location_trajectory WHERE mac_address IN (:index_values)")

        # --- Step 7: Create 3 LFs and approve ---
        lf1 = _create_lf(client, c_id, r1["r_id"], "static_lf", all_cv_ids,
            f"STATIC={static_cv['cv_id']}\nABSTAIN=-1\ndef labeling_function(row):\n    if row['connection_count']>=0.8 and row['distinct_ap']==1: return STATIC\n    return ABSTAIN\n")
        lf2 = _create_lf(client, c_id, r2["r_id"], "laptop_lf", all_cv_ids,
            f"LAPTOP={laptop_cv['cv_id']}\nABSTAIN=-1\ndef labeling_function(row):\n    if row['percentage']>=0.8: return LAPTOP\n    return ABSTAIN\n")
        lf3 = _create_lf(client, c_id, r3["r_id"], "phone_lf", all_cv_ids,
            f"PHONE={phone_cv['cv_id']}\nABSTAIN=-1\ndef labeling_function(row):\n    if row['total_days']>=1: return PHONE\n    return ABSTAIN\n")

        for lf in [lf1, lf2, lf3]:
            resp = client.post(f"/concepts/{c_id}/labeling-functions/{lf['lf_id']}/approve")
            assert resp.status_code == 200

        # --- Step 8: Simulate Level 1 Snorkel completing ---
        l1_job = SnorkelJob(
            c_id=c_id,
            index_id=sql_idx["index_id"],
            rule_ids=[r1["r_id"], r2["r_id"], r3["r_id"]],
            lf_ids=[lf1["lf_id"], lf2["lf_id"], lf3["lf_id"]],
            status="COMPLETED",
            output_type="softmax",
            result_path="snorkel_jobs/job_e2e_l1.parquet",
            lf_summary=[
                {"name": "static_lf", "coverage": 0.45, "accuracy": 0.82},
                {"name": "laptop_lf", "coverage": 0.38, "accuracy": 0.78},
                {"name": "phone_lf", "coverage": 0.52, "accuracy": 0.85},
            ],
            class_distribution={
                "STATIC": 312,
                "LAPTOP": 245,
                "PHONE": 443,
            },
            overall_stats={"total_samples": 1000, "labeled": 820, "abstain": 180},
            cv_id_to_index={
                str(static_cv["cv_id"]): 0,
                str(laptop_cv["cv_id"]): 1,
                str(phone_cv["cv_id"]): 2,
            },
            cv_id_to_name={
                str(static_cv["cv_id"]): "STATIC",
                str(laptop_cv["cv_id"]): "LAPTOP",
                str(phone_cv["cv_id"]): "PHONE",
            },
        )
        db.add(l1_job)
        db.flush()

        # Verify job is accessible via API
        job_resp = client.get(f"/concepts/{c_id}/snorkel/jobs/{l1_job.job_id}")
        assert job_resp.status_code == 200
        assert job_resp.json()["status"] == "COMPLETED"

        # --- Step 9: Create Level 2 derived index (LAPTOP branch) ---
        laptop_derived_resp = client.post(f"/concepts/{c_id}/indexes/derived", json={
            "name": "laptop_branch_idx",
            "cv_id": personal_cv["cv_id"],
            "parent_snorkel_job_id": l1_job.job_id,
            "label_filter": {
                "labels": {
                    str(laptop_cv["cv_id"]): {"min_confidence": 0.8}
                }
            },
        })
        assert laptop_derived_resp.status_code == 201
        laptop_derived = laptop_derived_resp.json()
        assert laptop_derived["source_type"] == "derived"
        assert laptop_derived["parent_snorkel_job_id"] == l1_job.job_id
        assert laptop_derived["label_filter"]["labels"][str(laptop_cv["cv_id"])]["min_confidence"] == 0.8

        # --- Step 10: Verify tree and staleness consistency ---
        # Tree should still be correct
        tree_resp2 = client.get(f"/concepts/{c_id}/values/tree")
        assert tree_resp2.status_code == 200

        # Staleness check
        stale_resp = client.get(f"/concepts/{c_id}/pipeline/staleness")
        assert stale_resp.status_code == 200
        stale_data = stale_resp.json()
        assert "indexes" in stale_data
        assert "rules" in stale_data

        # Verify indexes list includes both SQL and derived
        all_idx_resp = client.get(f"/concepts/{c_id}/indexes")
        assert all_idx_resp.status_code == 200
        all_indexes = all_idx_resp.json()
        source_types = {idx["source_type"] for idx in all_indexes}
        assert "sql" in source_types
        assert "derived" in source_types

        # Verify the derived index count — 2 derived (root + laptop branch)
        derived_count = sum(1 for idx in all_indexes if idx["source_type"] == "derived")
        assert derived_count == 2
