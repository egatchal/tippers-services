import client from '../../../shared/api/client';

export async function getStaleness(cId: number): Promise<Record<string, unknown>> {
  const { data } = await client.get(`/concepts/${cId}/pipeline/staleness`);
  return data;
}

export async function invalidateIndex(cId: number, indexId: number): Promise<void> {
  await client.post(`/concepts/${cId}/pipeline/invalidate/${indexId}`);
}

export async function executePipeline(cId: number, indexId?: number): Promise<Record<string, unknown>> {
  const { data } = await client.post(`/concepts/${cId}/pipeline/execute`, { index_id: indexId });
  return data;
}
