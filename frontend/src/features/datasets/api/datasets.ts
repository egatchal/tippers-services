import client from '../../../shared/api/client';
import type { Dataset, DatasetCreate, DatasetPreview } from '../types/dataset';

export async function listDatasets(params?: { service?: string; dataset_type?: string }): Promise<Dataset[]> {
  const { data } = await client.get('/datasets', { params });
  return data;
}

export async function getDataset(datasetId: number): Promise<Dataset> {
  const { data } = await client.get(`/datasets/${datasetId}`);
  return data;
}

export async function previewDataset(datasetId: number, limit = 50): Promise<DatasetPreview> {
  const { data } = await client.get(`/datasets/${datasetId}/preview`, { params: { limit } });
  return data;
}

export async function registerDataset(body: DatasetCreate): Promise<Dataset> {
  const { data } = await client.post('/datasets', body);
  return data;
}

export async function deleteDataset(datasetId: number): Promise<void> {
  await client.delete(`/datasets/${datasetId}`);
}

export async function runModelOnDataset(datasetId: number, body: { deployment_id: number }): Promise<Dataset> {
  const { data } = await client.post(`/datasets/${datasetId}/run-model`, body);
  return data;
}
