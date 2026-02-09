import httpx
from typing import Dict, Any, Optional
import os

class DagsterClient:
    """GraphQL client for interacting with Dagster."""

    def __init__(self, host: str = None, port: int = None):
        self.host = host or os.getenv("DAGSTER_HOST", "localhost")
        self.port = port or int(os.getenv("DAGSTER_PORT", "3000"))
        self.base_url = f"http://{self.host}:{self.port}/graphql"

    def submit_job_execution(
        self,
        job_name: str,
        run_config: Dict[str, Any],
        repository_name: str = "__repository__",
        location_name: str = "backend.dagster_app.definitions:defs"
    ) -> Dict[str, Any]:
        """Submit a job execution to Dagster."""
        mutation = """
        mutation LaunchRun($executionParams: ExecutionParams!) {
          launchRun(executionParams: $executionParams) {
            __typename
            ... on LaunchRunSuccess {
              run {
                runId
                status
              }
            }
            ... on PythonError {
              message
              stack
            }
          }
        }
        """

        variables = {
            "executionParams": {
                "selector": {
                    "repositoryLocationName": location_name,
                    "repositoryName": repository_name,
                    "jobName": job_name,
                },
                "runConfigData": run_config,
                "mode": "default"
            }
        }

        response = httpx.post(
            self.base_url,
            json={"query": mutation, "variables": variables},
            timeout=30.0
        )

        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            raise Exception(f"GraphQL error: {data['errors']}")

        launch_result = data["data"]["launchRun"]

        if launch_result["__typename"] == "LaunchRunSuccess":
            return {
                "run_id": launch_result["run"]["runId"],
                "status": launch_result["run"]["status"]
            }
        else:
            raise Exception(f"Failed to launch run: {launch_result}")

    def get_run_status(self, run_id: str) -> Dict[str, Any]:
        """Get the status of a Dagster run."""
        query = """
        query GetRunStatus($runId: ID!) {
          runOrError(runId: $runId) {
            __typename
            ... on Run {
              runId
              status
              stats {
                __typename
                ... on RunStatsSnapshot {
                  startTime
                  endTime
                }
              }
            }
            ... on RunNotFoundError {
              message
            }
          }
        }
        """

        variables = {"runId": run_id}

        response = httpx.post(
            self.base_url,
            json={"query": query, "variables": variables},
            timeout=30.0
        )

        response.raise_for_status()
        data = response.json()

        if "errors" in data:
            raise Exception(f"GraphQL error: {data['errors']}")

        run_data = data["data"]["runOrError"]

        if run_data["__typename"] == "Run":
            return {
                "run_id": run_data["runId"],
                "status": run_data["status"],
                "start_time": run_data["stats"]["startTime"] if run_data["stats"] else None,
                "end_time": run_data["stats"]["endTime"] if run_data["stats"] else None,
            }
        else:
            raise Exception(f"Run not found: {run_data}")


# Singleton instance
_dagster_client = None


def get_dagster_client() -> DagsterClient:
    """Get or create Dagster client instance."""
    global _dagster_client
    if _dagster_client is None:
        _dagster_client = DagsterClient()
    return _dagster_client
