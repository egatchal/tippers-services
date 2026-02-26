import client from '../../../shared/api/client';
import type { Space } from '../types/occupancy';

export async function listRootSpaces(): Promise<Space[]> {
  const { data } = await client.get('/spaces/roots');
  return data;
}

export async function getSpaceChildren(spaceId: number): Promise<Space[]> {
  const { data } = await client.get(`/spaces/${spaceId}/children`);
  return data;
}

export async function getSpace(spaceId: number): Promise<Space> {
  const { data } = await client.get(`/spaces/${spaceId}`);
  return data;
}
