export interface Job {
  job_id: number;
  service: string;
  job_type: string;
  service_job_ref: Record<string, unknown> | null;
  dagster_run_id: string | null;
  dagster_job_name: string | null;
  config: Record<string, unknown> | null;
  status: string;
  error_message: string | null;
  input_dataset_ids: number[] | null;
  output_dataset_ids: number[] | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
}

export interface JobLogs {
  logs: string[];
}
