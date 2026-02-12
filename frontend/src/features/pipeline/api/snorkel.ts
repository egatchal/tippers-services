import client from '../../../shared/api/client';
import type { SnorkelJob, SnorkelResults } from '../types/entities';

export async function runSnorkel(cId: number, body: {
  selectedIndex: number;
  selectedRules?: number[];
  selectedLFs: number[];
  snorkel: { epochs?: number; lr?: number; sample_size?: number; output_type?: string };
}): Promise<SnorkelJob> {
  const { data } = await client.post(`/concepts/${cId}/snorkel/run`, body);
  return data;
}

export async function listSnorkelJobs(cId: number): Promise<SnorkelJob[]> {
  const { data } = await client.get(`/concepts/${cId}/snorkel/jobs`);
  return data;
}

export async function getSnorkelJob(cId: number, jobId: number): Promise<SnorkelJob> {
  const { data } = await client.get(`/concepts/${cId}/snorkel/jobs/${jobId}`);
  return data;
}

export async function getSnorkelResults(cId: number, jobId: number): Promise<SnorkelResults> {
  const { data } = await client.get(`/concepts/${cId}/snorkel/jobs/${jobId}/results`);
  return data;
}

export async function deleteSnorkelJob(cId: number, jobId: number): Promise<void> {
  await client.delete(`/concepts/${cId}/snorkel/jobs/${jobId}`);
}

export async function cancelSnorkelJob(cId: number, jobId: number): Promise<SnorkelJob> {
  const { data } = await client.post(`/concepts/${cId}/snorkel/jobs/${jobId}/cancel`);
  return data;
}
