import client from './client';

export interface DagsterRunStatus {
  run_id: string;
  status: string;
  start_time?: string;
  end_time?: string;
  error_message?: string;
}

export async function getDagsterRunStatus(runId: string): Promise<DagsterRunStatus> {
  const { data } = await client.get(`/dagster/runs/${runId}`);
  return data;
}
