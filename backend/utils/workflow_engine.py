"""Config-driven workflow engine — orchestrates multi-step Dagster workflows."""
import json
import logging
import re
from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _render_template(template_obj: Any, params: dict) -> Any:
    """Recursively replace {{placeholder}} strings in a nested dict/list with param values."""
    if isinstance(template_obj, str):
        # Replace all {{key}} patterns
        def replacer(match):
            key = match.group(1).strip()
            val = params.get(key, match.group(0))
            return str(val)
        rendered = re.sub(r"\{\{(.+?)\}\}", replacer, template_obj)
        # Try to coerce numeric strings back to int/float
        try:
            return int(rendered)
        except (ValueError, TypeError):
            pass
        try:
            return float(rendered)
        except (ValueError, TypeError):
            pass
        return rendered
    elif isinstance(template_obj, dict):
        return {k: _render_template(v, params) for k, v in template_obj.items()}
    elif isinstance(template_obj, list):
        return [_render_template(item, params) for item in template_obj]
    return template_obj


class WorkflowEngine:
    """Manages lifecycle of workflow runs."""

    def __init__(self, session: Session):
        self.session = session

    def start_run(self, template_id: int, params: Optional[dict] = None) -> dict:
        """Create a WorkflowRun and submit root steps."""
        from backend.utils.dagster_client import get_dagster_client

        # Load template
        row = self.session.execute(
            text("SELECT * FROM workflow_templates WHERE template_id = :id AND is_active = true"),
            {"id": template_id},
        ).fetchone()

        if not row:
            raise ValueError(f"Active workflow template {template_id} not found")

        steps_def = row.steps if isinstance(row.steps, dict) else json.loads(row.steps)
        steps = steps_def.get("steps", [])
        params = params or {}

        # Initialize step_statuses
        step_statuses = {}
        for step in steps:
            step_statuses[step["key"]] = {"status": "PENDING", "dagster_run_id": None}

        # Create run record
        result = self.session.execute(
            text("""
                INSERT INTO workflow_runs (template_id, service, params, step_statuses, status, created_at)
                VALUES (:template_id, :service, CAST(:params AS jsonb), CAST(:step_statuses AS jsonb), 'RUNNING', :now)
                RETURNING run_id
            """),
            {
                "template_id": template_id,
                "service": row.service,
                "params": json.dumps(params),
                "step_statuses": json.dumps(step_statuses),
                "now": datetime.utcnow(),
            },
        )
        run_id = result.fetchone().run_id
        self.session.commit()

        # Submit root steps (no depends_on)
        dagster_client = get_dagster_client()
        for step in steps:
            if not step.get("depends_on"):
                self._submit_step(run_id, step, params, step_statuses, dagster_client)

        # Save updated statuses
        self.session.execute(
            text("UPDATE workflow_runs SET step_statuses = CAST(:ss AS jsonb) WHERE run_id = :id"),
            {"ss": json.dumps(step_statuses), "id": run_id},
        )
        self.session.commit()

        return {"run_id": run_id, "status": "RUNNING", "step_statuses": step_statuses}

    def advance_run(self, run_id: int) -> dict:
        """Check completed steps and submit newly-unblocked steps."""
        from backend.utils.dagster_client import get_dagster_client

        run_row = self.session.execute(
            text("SELECT * FROM workflow_runs WHERE run_id = :id"),
            {"id": run_id},
        ).fetchone()

        if not run_row or run_row.status not in ("RUNNING",):
            return {"run_id": run_id, "status": run_row.status if run_row else "NOT_FOUND"}

        template_row = self.session.execute(
            text("SELECT * FROM workflow_templates WHERE template_id = :id"),
            {"id": run_row.template_id},
        ).fetchone()

        steps_def = template_row.steps if isinstance(template_row.steps, dict) else json.loads(template_row.steps)
        steps = steps_def.get("steps", [])
        step_statuses = run_row.step_statuses if isinstance(run_row.step_statuses, dict) else json.loads(run_row.step_statuses)
        params = run_row.params if isinstance(run_row.params, dict) else json.loads(run_row.params or "{}")

        dagster_client = get_dagster_client()

        # Poll Dagster for RUNNING steps
        for step in steps:
            key = step["key"]
            ss = step_statuses.get(key, {})
            if ss.get("status") == "RUNNING" and ss.get("dagster_run_id"):
                try:
                    dagster_status = dagster_client.get_run_status(ss["dagster_run_id"])
                    d_status = dagster_status.get("status", "")
                    if d_status == "SUCCESS":
                        ss["status"] = "COMPLETED"
                    elif d_status in ("FAILURE", "CANCELED"):
                        ss["status"] = "FAILED"
                        ss["error"] = f"Dagster run {d_status}"
                except Exception as e:
                    logger.warning(f"Failed to poll Dagster for step {key}: {e}")

        # Check for newly-unblocked steps
        for step in steps:
            key = step["key"]
            ss = step_statuses.get(key, {})
            if ss.get("status") != "PENDING":
                continue

            deps = step.get("depends_on", [])
            all_deps_done = all(
                step_statuses.get(d, {}).get("status") == "COMPLETED" for d in deps
            )
            if all_deps_done:
                self._submit_step(run_id, step, params, step_statuses, dagster_client)

        # Determine overall status
        all_statuses = [step_statuses[s["key"]]["status"] for s in steps]
        if any(s == "FAILED" for s in all_statuses):
            overall = "FAILED"
            error_msgs = [step_statuses[s["key"]].get("error", "") for s in steps if step_statuses[s["key"]]["status"] == "FAILED"]
            error_message = "; ".join(filter(None, error_msgs)) or None
        elif all(s == "COMPLETED" for s in all_statuses):
            overall = "COMPLETED"
            error_message = None
        else:
            overall = "RUNNING"
            error_message = None

        now = datetime.utcnow()
        self.session.execute(
            text("""
                UPDATE workflow_runs
                SET step_statuses = CAST(:ss AS jsonb),
                    status = :status,
                    error_message = :error,
                    completed_at = CASE WHEN :status IN ('COMPLETED', 'FAILED') THEN :now ELSE completed_at END
                WHERE run_id = :id
            """),
            {
                "ss": json.dumps(step_statuses),
                "status": overall,
                "error": error_message,
                "now": now,
                "id": run_id,
            },
        )
        self.session.commit()

        return {"run_id": run_id, "status": overall, "step_statuses": step_statuses}

    def _submit_step(self, run_id: int, step: dict, params: dict, step_statuses: dict, dagster_client):
        """Render config and submit a single step to Dagster."""
        key = step["key"]
        dagster_job = step["dagster_job"]
        config_template = step.get("config_template", {})
        rendered_config = _render_template(config_template, params)

        try:
            result = dagster_client.submit_job_execution(
                job_name=dagster_job,
                run_config=rendered_config,
            )
            step_statuses[key] = {
                "status": "RUNNING",
                "dagster_run_id": result["run_id"],
            }
            logger.info(f"Workflow run {run_id}: submitted step '{key}' → Dagster run {result['run_id']}")
        except Exception as e:
            step_statuses[key] = {
                "status": "FAILED",
                "dagster_run_id": None,
                "error": str(e),
            }
            logger.error(f"Workflow run {run_id}: step '{key}' submission failed: {e}")
