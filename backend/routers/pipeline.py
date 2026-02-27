"""Pipeline orchestration endpoints — staleness, invalidation, execution."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Dict, Any
from backend.db.session import get_db
from backend.db.models import Concept, ConceptIndex, ConceptRule, SnorkelJob
from backend.utils.staleness import compute_staleness, cascade_invalidate

router = APIRouter()


@router.get("/{c_id}/pipeline/staleness")
async def get_pipeline_staleness(
    c_id: int,
    db: Session = Depends(get_db)
):
    """
    Get staleness status for all assets in a concept's pipeline.

    Returns per-asset staleness info including whether each asset needs
    re-materialization and the reason why.

    - **c_id**: Concept ID
    """
    concept = db.query(Concept).filter(Concept.c_id == c_id).first()
    if not concept:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Concept with ID {c_id} not found"
        )

    return compute_staleness(c_id, db)


@router.post("/{c_id}/pipeline/invalidate/{index_id}")
async def invalidate_downstream(
    c_id: int,
    index_id: int,
    db: Session = Depends(get_db)
):
    """
    Cascade invalidation from an index through all downstream assets.

    Marks downstream rules, snorkel jobs, and derived indexes as stale.

    - **c_id**: Concept ID
    - **index_id**: Index ID to start invalidation from
    """
    index = db.query(ConceptIndex).filter(
        ConceptIndex.c_id == c_id,
        ConceptIndex.index_id == index_id
    ).first()

    if not index:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Index with ID {index_id} not found for concept {c_id}"
        )

    invalidated = cascade_invalidate("index", index_id, db)

    return {
        "index_id": index_id,
        "invalidated_count": len(invalidated),
        "invalidated": invalidated,
    }


@router.post("/{c_id}/pipeline/execute")
async def execute_pipeline(
    c_id: int,
    index_id: int = None,
    db: Session = Depends(get_db)
):
    """
    Trigger full pipeline execution for a concept.

    If index_id is provided, runs the pipeline for that specific index.
    Otherwise, finds stale assets and re-materializes them in dependency order.

    - **c_id**: Concept ID
    - **index_id**: Optional specific index to execute pipeline for
    """
    concept = db.query(Concept).filter(Concept.c_id == c_id).first()
    if not concept:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Concept with ID {c_id} not found"
        )

    from backend.utils.dagster_client import get_dagster_client

    dagster_client = get_dagster_client()
    triggered_runs = []

    if index_id:
        # Execute pipeline for a specific index
        index = db.query(ConceptIndex).filter(
            ConceptIndex.c_id == c_id,
            ConceptIndex.index_id == index_id,
            ConceptIndex.source_type == "sql"
        ).first()

        if not index:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"SQL index {index_id} not found for concept {c_id}"
            )

        # Step 1: Materialize index
        try:
            result = dagster_client.submit_job_execution(
                job_name="materialize_index_job",
                run_config={"ops": {"materialized_index": {"config": {"index_id": index_id}}}}
            )
            triggered_runs.append({
                "step": "materialize_index",
                "index_id": index_id,
                "dagster_run_id": result["run_id"],
            })
        except Exception as e:
            triggered_runs.append({
                "step": "materialize_index",
                "index_id": index_id,
                "error": str(e),
            })

        # Step 2: Materialize all rules for this index
        rules = db.query(ConceptRule).filter(
            ConceptRule.c_id == c_id,
            ConceptRule.index_id == index_id
        ).all()

        for rule in rules:
            try:
                result = dagster_client.submit_job_execution(
                    job_name="materialize_rule_job",
                    run_config={"ops": {"materialized_rule": {"config": {"rule_id": rule.r_id}}}}
                )
                triggered_runs.append({
                    "step": "materialize_rule",
                    "rule_id": rule.r_id,
                    "dagster_run_id": result["run_id"],
                })
            except Exception as e:
                triggered_runs.append({
                    "step": "materialize_rule",
                    "rule_id": rule.r_id,
                    "error": str(e),
                })
    else:
        # Find and execute stale assets
        staleness = compute_staleness(c_id, db)

        # Materialize stale indexes first
        for idx_info in staleness["indexes"]:
            if idx_info["stale"] and idx_info["source_type"] == "sql":
                try:
                    result = dagster_client.submit_job_execution(
                        job_name="materialize_index_job",
                        run_config={"ops": {"materialized_index": {"config": {"index_id": idx_info["index_id"]}}}}
                    )
                    triggered_runs.append({
                        "step": "materialize_index",
                        "index_id": idx_info["index_id"],
                        "dagster_run_id": result["run_id"],
                    })
                except Exception as e:
                    triggered_runs.append({
                        "step": "materialize_index",
                        "index_id": idx_info["index_id"],
                        "error": str(e),
                    })

        # Then materialize stale rules
        for rule_info in staleness["rules"]:
            if rule_info["stale"]:
                try:
                    result = dagster_client.submit_job_execution(
                        job_name="materialize_rule_job",
                        run_config={"ops": {"materialized_rule": {"config": {"rule_id": rule_info["r_id"]}}}}
                    )
                    triggered_runs.append({
                        "step": "materialize_rule",
                        "rule_id": rule_info["r_id"],
                        "dagster_run_id": result["run_id"],
                    })
                except Exception as e:
                    triggered_runs.append({
                        "step": "materialize_rule",
                        "rule_id": rule_info["r_id"],
                        "error": str(e),
                    })

    return {
        "concept_id": c_id,
        "triggered_runs": triggered_runs,
        "total_triggered": len(triggered_runs),
    }
