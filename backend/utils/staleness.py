"""Staleness detection and cascade invalidation for the Snorkel pipeline.

Walks the dependency graph (index → rules → snorkel → derived indexes) to
determine which assets are stale and need re-materialization.
"""
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)


def compute_staleness(c_id: int, db: Session) -> Dict[str, Any]:
    """Compute staleness status for all assets in a concept.

    Returns a dict with per-asset staleness info:
    {
        "indexes": [{"index_id": 1, "name": ..., "stale": bool, "reason": ...}, ...],
        "rules": [...],
        "snorkel_jobs": [...],
    }

    Staleness rules:
    - SQL index: stale if updated_at > materialized_at (definition changed)
    - Rule: stale if parent index materialized_at > rule materialized_at
    - Derived index: stale if parent snorkel job completed_at > derived index created_at
    - Snorkel job: stale if any input rule materialized_at > job created_at
    """
    result = {"indexes": [], "rules": [], "snorkel_jobs": []}

    # Load all indexes for this concept
    indexes = db.execute(
        text("""
        SELECT index_id, name, source_type, is_materialized, materialized_at,
               updated_at, parent_index_id, parent_snorkel_job_id
        FROM concept_indexes WHERE c_id = :c_id
        """),
        {"c_id": c_id},
    ).fetchall()

    index_materialized = {}
    for idx in indexes:
        index_materialized[idx.index_id] = idx.materialized_at
        stale = False
        reason = None

        if idx.source_type == "sql":
            if not idx.is_materialized:
                stale = True
                reason = "Not materialized"
            elif idx.updated_at and idx.materialized_at and idx.updated_at > idx.materialized_at:
                stale = True
                reason = "Definition updated after materialization"
        elif idx.source_type == "derived":
            if idx.parent_snorkel_job_id:
                job_row = db.execute(
                    text("SELECT completed_at FROM snorkel_jobs WHERE job_id = :jid"),
                    {"jid": idx.parent_snorkel_job_id},
                ).fetchone()
                if job_row and job_row.completed_at and idx.materialized_at:
                    if job_row.completed_at > idx.materialized_at:
                        stale = True
                        reason = "Parent Snorkel job re-completed"

        result["indexes"].append({
            "index_id": idx.index_id,
            "name": idx.name,
            "source_type": idx.source_type,
            "is_materialized": idx.is_materialized,
            "stale": stale,
            "reason": reason,
        })

    # Load all rules for this concept
    rules = db.execute(
        text("""
        SELECT r_id, name, index_id, is_materialized, materialized_at
        FROM concept_rules WHERE c_id = :c_id
        """),
        {"c_id": c_id},
    ).fetchall()

    for rule in rules:
        stale = False
        reason = None

        if not rule.is_materialized:
            stale = True
            reason = "Not materialized"
        else:
            parent_mat = index_materialized.get(rule.index_id)
            if parent_mat and rule.materialized_at and parent_mat > rule.materialized_at:
                stale = True
                reason = "Parent index re-materialized"

        result["rules"].append({
            "r_id": rule.r_id,
            "name": rule.name,
            "index_id": rule.index_id,
            "is_materialized": rule.is_materialized,
            "stale": stale,
            "reason": reason,
        })

    # Load completed snorkel jobs for this concept
    jobs = db.execute(
        text("""
        SELECT job_id, index_id, rule_ids, status, created_at, completed_at
        FROM snorkel_jobs WHERE c_id = :c_id AND status = 'COMPLETED'
        """),
        {"c_id": c_id},
    ).fetchall()

    rule_materialized = {r.r_id: r.materialized_at for r in rules}

    for job in jobs:
        stale = False
        reason = None

        if job.rule_ids:
            for rid in job.rule_ids:
                rule_mat = rule_materialized.get(rid)
                if rule_mat and job.created_at and rule_mat > job.created_at:
                    stale = True
                    reason = f"Rule {rid} re-materialized after job creation"
                    break

        # Also check if parent index was re-materialized
        parent_mat = index_materialized.get(job.index_id)
        if parent_mat and job.created_at and parent_mat > job.created_at:
            stale = True
            reason = "Parent index re-materialized after job creation"

        result["snorkel_jobs"].append({
            "job_id": job.job_id,
            "index_id": job.index_id,
            "status": job.status,
            "stale": stale,
            "reason": reason,
        })

    return result


def cascade_invalidate(entity_type: str, entity_id: int, db: Session) -> List[Dict[str, Any]]:
    """Walk the dependency tree and set is_materialized=False on all downstream assets.

    Returns a list of invalidated entities.

    Cascade rules:
    - Index invalidated → all rules referencing it, all snorkel jobs using those rules
    - Rule invalidated → all snorkel jobs using it
    - Snorkel job invalidated → all derived indexes referencing it
    """
    invalidated = []

    if entity_type == "index":
        # Find all rules that reference this index
        rules = db.execute(
            text("SELECT r_id FROM concept_rules WHERE index_id = :idx_id AND is_materialized = true"),
            {"idx_id": entity_id},
        ).fetchall()

        for rule in rules:
            db.execute(
                text("UPDATE concept_rules SET is_materialized = false WHERE r_id = :rid"),
                {"rid": rule.r_id},
            )
            invalidated.append({"type": "rule", "id": rule.r_id})
            # Cascade from rule
            invalidated.extend(cascade_invalidate("rule", rule.r_id, db))

        # Derived indexes that point to this index
        derived = db.execute(
            text("SELECT index_id FROM concept_indexes WHERE parent_index_id = :idx_id"),
            {"idx_id": entity_id},
        ).fetchall()
        for d in derived:
            invalidated.append({"type": "derived_index", "id": d.index_id})

    elif entity_type == "rule":
        # Find snorkel jobs that use this rule
        jobs = db.execute(
            text("SELECT job_id FROM snorkel_jobs WHERE :rid = ANY(rule_ids) AND status = 'COMPLETED'"),
            {"rid": entity_id},
        ).fetchall()
        for job in jobs:
            invalidated.append({"type": "snorkel_job", "id": job.job_id, "note": "stale"})
            invalidated.extend(cascade_invalidate("snorkel_job", job.job_id, db))

    elif entity_type == "snorkel_job":
        # Find derived indexes that reference this snorkel job
        derived = db.execute(
            text("SELECT index_id FROM concept_indexes WHERE parent_snorkel_job_id = :jid"),
            {"jid": entity_id},
        ).fetchall()
        for d in derived:
            invalidated.append({"type": "derived_index", "id": d.index_id})

    db.commit()
    return invalidated
