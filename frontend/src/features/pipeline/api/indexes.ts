import client from '../../../shared/api/client';
import type { Index, EntityPreviewResponse } from '../types/entities';

export async function listIndexes(cId: number): Promise<Index[]> {
  const { data } = await client.get(`/concepts/${cId}/indexes`);
  return data;
}

export async function getIndex(cId: number, indexId: number): Promise<Index> {
  const { data } = await client.get(`/concepts/${cId}/indexes/${indexId}`);
  return data;
}

export async function createIndex(cId: number, body: {
  name: string;
  conn_id: number;
  key_column: string;
  sql_query: string;
  query_template_params?: Record<string, unknown>;
  cv_id?: number;
}): Promise<Index> {
  const { data } = await client.post(`/concepts/${cId}/indexes`, body);
  return data;
}

export async function updateIndex(cId: number, indexId: number, body: Partial<{
  name: string;
  conn_id: number;
  key_column: string;
  sql_query: string;
  query_template_params: Record<string, unknown>;
  parent_snorkel_job_id: number;
}>): Promise<Index> {
  const { data } = await client.patch(`/concepts/${cId}/indexes/${indexId}`, body);
  return data;
}

export async function deleteIndex(cId: number, indexId: number): Promise<void> {
  await client.delete(`/concepts/${cId}/indexes/${indexId}`);
}

export async function materializeIndex(cId: number, indexId: number): Promise<{ index_id: number; dagster_run_id: string; status: string }> {
  const { data } = await client.post(`/concepts/${cId}/indexes/${indexId}/materialize`);
  return data;
}

export async function createDerivedIndex(cId: number, body: {
  name: string;
  cv_id: number;
  parent_index_id?: number;
  parent_snorkel_job_id?: number;
  label_filter?: Record<string, unknown>;
  output_type?: string;
}): Promise<Index> {
  const { data } = await client.post(`/concepts/${cId}/indexes/derived`, body);
  return data;
}

export async function updateLabelFilter(cId: number, indexId: number, body: {
  label_filter: Record<string, unknown>;
}): Promise<Index> {
  const { data } = await client.patch(`/concepts/${cId}/indexes/${indexId}/filter`, body);
  return data;
}

export async function getEntityPreview(cId: number, indexId: number, limit = 50, offset = 0): Promise<EntityPreviewResponse> {
  const { data } = await client.get(`/concepts/${cId}/indexes/${indexId}/entities`, { params: { limit, offset } });
  return data;
}
