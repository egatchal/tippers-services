import client from '../../../shared/api/client';
import type { OccupancyDataset, OccupancyDatasetCreate, OccupancyResults } from '../types/occupancy';

export async function createOccupancyDataset(body: OccupancyDatasetCreate): Promise<OccupancyDataset> {
  const { data } = await client.post('/occupancy/datasets', body);
  return data;
}

export async function listOccupancyDatasets(spaceId?: number): Promise<OccupancyDataset[]> {
  const params = spaceId != null ? { space_id: spaceId } : {};
  const { data } = await client.get('/occupancy/datasets', { params });
  return data;
}

export async function getOccupancyResults(datasetId: number, spaceId?: number): Promise<OccupancyResults> {
  const params = spaceId != null ? { space_id: spaceId } : {};
  const { data } = await client.get(`/occupancy/datasets/${datasetId}/results`, { params });
  return data;
}
