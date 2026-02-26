import client from '../../../shared/api/client';
import type { Job } from '../types/job';

export async function listJobs(params?: { service?: string; status?: string; job_type?: string }): Promise<Job[]> {
  const { data } = await client.get('/jobs', { params });
  return data;
}

export async function getJob(jobId: number): Promise<Job> {
  const { data } = await client.get(`/jobs/${jobId}`);
  return data;
}

export async function getJobLogs(jobId: number): Promise<{ logs: string[] }> {
  const { data } = await client.get(`/jobs/${jobId}/logs`);
  return data;
}

export async function cancelJob(jobId: number): Promise<void> {
  await client.post(`/jobs/${jobId}/cancel`);
}
