import client from '../../../shared/api/client';
import type { Index } from '../types/entities';

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
  sql_query: string;
  query_template_params?: Record<string, unknown>;
}): Promise<Index> {
  const { data } = await client.post(`/concepts/${cId}/indexes`, body);
  return data;
}

export async function updateIndex(cId: number, indexId: number, body: Partial<{
  name: string;
  conn_id: number;
  sql_query: string;
  query_template_params: Record<string, unknown>;
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
