import client from '../../../shared/api/client';
import type { LabelingFunction } from '../types/entities';

export async function listLFs(cId: number): Promise<LabelingFunction[]> {
  const { data } = await client.get(`/concepts/${cId}/labeling-functions`);
  return data;
}

export async function getLF(cId: number, lfId: number): Promise<LabelingFunction> {
  const { data } = await client.get(`/concepts/${cId}/labeling-functions/${lfId}`);
  return data;
}

export async function createLF(cId: number, body: {
  name: string;
  rule_id: number;
  applicable_cv_ids: number[];
  code?: string;
  allowed_imports?: string[];
}): Promise<LabelingFunction> {
  const { data } = await client.post(`/concepts/${cId}/labeling-functions`, body);
  return data;
}

export async function updateLF(cId: number, lfId: number, body: Partial<{
  name: string;
  is_active: boolean;
  lf_config: Record<string, unknown>;
  applicable_cv_ids: number[];
}>): Promise<LabelingFunction> {
  const { data } = await client.patch(`/concepts/${cId}/labeling-functions/${lfId}`, body);
  return data;
}

export async function deleteLF(cId: number, lfId: number): Promise<void> {
  await client.delete(`/concepts/${cId}/labeling-functions/${lfId}`);
}

export async function approveLF(cId: number, lfId: number): Promise<LabelingFunction> {
  const { data } = await client.post(`/concepts/${cId}/labeling-functions/${lfId}/approve`);
  return data;
}

export async function toggleLF(cId: number, lfId: number): Promise<LabelingFunction> {
  const { data } = await client.post(`/concepts/${cId}/labeling-functions/${lfId}/toggle`);
  return data;
}

export async function getLFMetrics(cId: number, lfId: number): Promise<{
  lf_id: number;
  name: string;
  version: number;
  estimated_accuracy: number;
  coverage: number;
  conflicts: number;
  metrics_available: boolean;
}> {
  const { data } = await client.get(`/concepts/${cId}/labeling-functions/${lfId}/metrics`);
  return data;
}
