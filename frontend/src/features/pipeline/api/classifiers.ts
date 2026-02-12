import client from '../../../shared/api/client';
import type { ClassifierJob, ClassifierResults } from '../types/entities';

export async function runClassifier(cId: number, body: {
  snorkel_job_id: number;
  feature_ids: number[];
  config: {
    threshold_method?: string;
    threshold_value?: number;
    min_labels_per_class?: number;
    imbalance_factor?: number;
    test_size?: number;
    random_state?: number;
    n_estimators?: number;
    max_depth?: number | null;
  };
}): Promise<ClassifierJob> {
  const { data } = await client.post(`/concepts/${cId}/classifiers/run`, body);
  return data;
}

export async function listClassifierJobs(cId: number): Promise<ClassifierJob[]> {
  const { data } = await client.get(`/concepts/${cId}/classifiers/jobs`);
  return data;
}

export async function getClassifierJob(cId: number, jobId: number): Promise<ClassifierJob> {
  const { data } = await client.get(`/concepts/${cId}/classifiers/jobs/${jobId}`);
  return data;
}

export async function getClassifierResults(cId: number, jobId: number): Promise<ClassifierResults> {
  const { data } = await client.get(`/concepts/${cId}/classifiers/jobs/${jobId}/results`);
  return data;
}

export async function deleteClassifierJob(cId: number, jobId: number): Promise<void> {
  await client.delete(`/concepts/${cId}/classifiers/jobs/${jobId}`);
}

export async function cancelClassifierJob(cId: number, jobId: number): Promise<ClassifierJob> {
  const { data } = await client.post(`/concepts/${cId}/classifiers/jobs/${jobId}/cancel`);
  return data;
}
