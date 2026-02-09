"""Dagster run status endpoints."""
from fastapi import APIRouter, HTTPException, status
from backend.schemas import DagsterRunStatusResponse
from typing import Optional
from datetime import datetime

router = APIRouter()


@router.get("/runs/{run_id}", response_model=DagsterRunStatusResponse)
async def get_run_status(run_id: str):
    """
    Check Dagster job status.

    This endpoint queries the Dagster GraphQL API to get the current
    status of a job run.

    - **run_id**: Dagster run ID

    Returns:
    - Run status (PENDING, STARTED, SUCCESS, FAILURE, CANCELLED)
    - Start and end timestamps
    - Error message if failed
    """
    # TODO: Implement Dagster GraphQL client
    # from backend.utils.dagster_client import get_dagster_client
    #
    # dagster_client = get_dagster_client()
    # run_status = dagster_client.get_run_status(run_id)
    #
    # return DagsterRunStatusResponse(
    #     run_id=run_id,
    #     status=run_status['status'],
    #     start_time=run_status.get('start_time'),
    #     end_time=run_status.get('end_time'),
    #     error_message=run_status.get('error_message')
    # )

    # Placeholder response
    if run_id.startswith("placeholder"):
        return DagsterRunStatusResponse(
            run_id=run_id,
            status="SUCCESS",
            start_time=datetime.utcnow(),
            end_time=datetime.utcnow(),
            error_message=None
        )
    else:
        return DagsterRunStatusResponse(
            run_id=run_id,
            status="PENDING",
            start_time=None,
            end_time=None,
            error_message=None
        )


@router.get("/runs")
async def list_runs(
    limit: int = 20,
    status_filter: Optional[str] = None
):
    """
    List recent Dagster runs.

    - **limit**: Maximum number of runs to return (default 20)
    - **status_filter**: Filter by status (PENDING, STARTED, SUCCESS, FAILURE, CANCELLED)

    Returns list of recent runs with their status.
    """
    # TODO: Implement Dagster GraphQL client
    # from backend.utils.dagster_client import get_dagster_client
    #
    # dagster_client = get_dagster_client()
    # runs = dagster_client.list_runs(limit=limit, status_filter=status_filter)
    #
    # return {
    #     "runs": [
    #         {
    #             "run_id": run['run_id'],
    #             "job_name": run['job_name'],
    #             "status": run['status'],
    #             "start_time": run.get('start_time'),
    #             "end_time": run.get('end_time')
    #         }
    #         for run in runs
    #     ]
    # }

    # Placeholder response
    return {
        "runs": [],
        "message": "Dagster client not implemented yet"
    }


@router.post("/runs/{run_id}/cancel")
async def cancel_run(run_id: str):
    """
    Cancel a running Dagster job.

    - **run_id**: Dagster run ID

    Returns:
    - Success message if cancelled
    - Error if run cannot be cancelled
    """
    # TODO: Implement Dagster GraphQL client
    # from backend.utils.dagster_client import get_dagster_client
    #
    # dagster_client = get_dagster_client()
    #
    # # Check if run is in cancellable state
    # run_status = dagster_client.get_run_status(run_id)
    # if run_status['status'] not in ['PENDING', 'STARTED']:
    #     raise HTTPException(
    #         status_code=status.HTTP_400_BAD_REQUEST,
    #         detail=f"Cannot cancel run with status {run_status['status']}"
    #     )
    #
    # # Cancel the run
    # result = dagster_client.cancel_run(run_id)
    #
    # return {
    #     "run_id": run_id,
    #     "status": "CANCELLED",
    #     "message": "Run cancelled successfully"
    # }

    # Placeholder response
    return {
        "run_id": run_id,
        "status": "CANCELLED",
        "message": "Dagster client not implemented yet. Run not actually cancelled."
    }


@router.get("/runs/{run_id}/logs")
async def get_run_logs(
    run_id: str,
    limit: int = 100
):
    """
    Get logs for a Dagster run.

    - **run_id**: Dagster run ID
    - **limit**: Maximum number of log entries to return

    Returns:
    - List of log entries with timestamps and messages
    """
    # TODO: Implement Dagster GraphQL client
    # from backend.utils.dagster_client import get_dagster_client
    #
    # dagster_client = get_dagster_client()
    # logs = dagster_client.get_run_logs(run_id, limit=limit)
    #
    # return {
    #     "run_id": run_id,
    #     "logs": [
    #         {
    #             "timestamp": log['timestamp'],
    #             "level": log['level'],
    #             "message": log['message'],
    #             "step_key": log.get('step_key')
    #         }
    #         for log in logs
    #     ]
    # }

    # Placeholder response
    return {
        "run_id": run_id,
        "logs": [],
        "message": "Dagster client not implemented yet"
    }


@router.get("/assets")
async def list_assets():
    """
    List all Dagster assets.

    Returns:
    - List of asset keys and their materialization status
    """
    # TODO: Implement Dagster GraphQL client
    # from backend.utils.dagster_client import get_dagster_client
    #
    # dagster_client = get_dagster_client()
    # assets = dagster_client.list_assets()
    #
    # return {
    #     "assets": [
    #         {
    #             "asset_key": asset['asset_key'],
    #             "last_materialization": asset.get('last_materialization'),
    #             "is_materialized": asset.get('is_materialized', False)
    #         }
    #         for asset in assets
    #     ]
    # }

    # Placeholder response
    return {
        "assets": [],
        "message": "Dagster client not implemented yet"
    }


@router.get("/health")
async def dagster_health_check():
    """
    Check if Dagster webserver is reachable.

    Returns:
    - Status of Dagster webserver connection
    """
    # TODO: Implement Dagster GraphQL client
    # from backend.utils.dagster_client import get_dagster_client
    #
    # try:
    #     dagster_client = get_dagster_client()
    #     is_healthy = dagster_client.health_check()
    #
    #     return {
    #         "status": "healthy" if is_healthy else "unhealthy",
    #         "dagster_host": dagster_client.host,
    #         "dagster_port": dagster_client.port
    #     }
    # except Exception as e:
    #     return {
    #         "status": "unhealthy",
    #         "error": str(e)
    #     }

    # Placeholder response
    import os
    return {
        "status": "not_configured",
        "message": "Dagster client not implemented yet",
        "dagster_host": os.getenv("DAGSTER_HOST", "dagster-webserver"),
        "dagster_port": os.getenv("DAGSTER_PORT", "3000")
    }
