import client from '../../../shared/api/client';
import type { Feature } from '../types/entities';

export async function listFeatures(cId: number): Promise<Feature[]> {
  const { data } = await client.get(`/concepts/${cId}/features`);
  return data;
}

export async function getFeature(cId: number, featureId: number): Promise<Feature> {
  const { data } = await client.get(`/concepts/${cId}/features/${featureId}`);
  return data;
}

export async function createFeature(cId: number, body: {
  name: string;
  index_id: number;
  sql_query: string;
  index_column?: string;
  columns?: string[];
  description?: string;
  query_template_params?: Record<string, unknown>;
}): Promise<Feature> {
  const { data } = await client.post(`/concepts/${cId}/features`, body);
  return data;
}

export async function updateFeature(cId: number, featureId: number, body: Partial<{
  name: string;
  index_id: number;
  sql_query: string;
  index_column: string;
  columns: string[];
  description: string;
  query_template_params: Record<string, unknown>;
}>): Promise<Feature> {
  const { data } = await client.patch(`/concepts/${cId}/features/${featureId}`, body);
  return data;
}

export async function deleteFeature(cId: number, featureId: number): Promise<void> {
  await client.delete(`/concepts/${cId}/features/${featureId}`);
}

export async function materializeFeature(cId: number, featureId: number): Promise<{ feature_id: number; dagster_run_id: string; status: string }> {
  const { data } = await client.post(`/concepts/${cId}/features/${featureId}/materialize`);
  return data;
}
