import client from '../../../shared/api/client';
import type { Concept, ConceptValue } from '../types/entities';

export async function listConcepts(): Promise<Concept[]> {
  const { data } = await client.get('/concepts/');
  return data;
}

export async function getConcept(cId: number): Promise<Concept> {
  const { data } = await client.get(`/concepts/${cId}`);
  return data;
}

export async function createConcept(body: { name: string; description?: string }): Promise<Concept> {
  const { data } = await client.post('/concepts/', body);
  return data;
}

export async function updateConcept(cId: number, body: { name?: string; description?: string }): Promise<Concept> {
  const { data } = await client.patch(`/concepts/${cId}`, body);
  return data;
}

export async function deleteConcept(cId: number): Promise<void> {
  await client.delete(`/concepts/${cId}`);
}

export async function listConceptValues(cId: number): Promise<ConceptValue[]> {
  const { data } = await client.get(`/concepts/${cId}/values`);
  return data;
}

export async function createConceptValue(cId: number, body: { name: string; description?: string; display_order?: number; level?: number }): Promise<ConceptValue> {
  const { data } = await client.post(`/concepts/${cId}/values`, body);
  return data;
}

export async function updateConceptValue(cId: number, cvId: number, body: { name?: string; description?: string; display_order?: number; level?: number }): Promise<ConceptValue> {
  const { data } = await client.patch(`/concepts/${cId}/values/${cvId}`, body);
  return data;
}

export async function deleteConceptValue(cId: number, cvId: number): Promise<void> {
  await client.delete(`/concepts/${cId}/values/${cvId}`);
}
