import client from '../../../shared/api/client';

export async function getAssetCatalog(cId: number): Promise<Record<string, unknown>> {
  const { data } = await client.get(`/concepts/${cId}/catalog`);
  return data;
}

export async function getMaterializedAssets(cId: number): Promise<Record<string, unknown>> {
  const { data } = await client.get(`/concepts/${cId}/catalog/materialized`);
  return data;
}

export async function getCatalogStats(cId: number): Promise<Record<string, unknown>> {
  const { data } = await client.get(`/concepts/${cId}/catalog/stats`);
  return data;
}
