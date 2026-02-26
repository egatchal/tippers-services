export interface WorkflowStep {
  key: string;
  dagster_job: string;
  config_template: Record<string, unknown>;
  depends_on: string[];
}

export interface WorkflowTemplate {
  template_id: number;
  name: string;
  service: string;
  description: string | null;
  steps: { steps: WorkflowStep[] };
  is_active: boolean;
  created_at: string;
  updated_at: string | null;
}

export interface WorkflowTemplateCreate {
  name: string;
  service: string;
  description?: string;
  steps: Record<string, unknown>;
}

export interface StepStatus {
  status: string;
  dagster_run_id?: string;
  error?: string;
}

export interface WorkflowRun {
  run_id: number;
  template_id: number;
  service: string;
  params: Record<string, unknown> | null;
  step_statuses: Record<string, StepStatus>;
  status: string;
  error_message: string | null;
  created_at: string;
  completed_at: string | null;
}

export interface WorkflowRunRequest {
  template_id: number;
  params: Record<string, unknown>;
}
