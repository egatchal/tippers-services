import client from '../../../shared/api/client';
import type { Rule } from '../types/entities';

export async function listRules(cId: number): Promise<Rule[]> {
  const { data } = await client.get(`/concepts/${cId}/rules`);
  return data;
}

export async function getRule(cId: number, rId: number): Promise<Rule> {
  const { data } = await client.get(`/concepts/${cId}/rules/${rId}`);
  return data;
}

export async function createRule(cId: number, body: {
  name: string;
  index_id: number;
  sql_query: string;
  index_column?: string;
  query_template_params?: Record<string, unknown>;
}): Promise<Rule> {
  const { data } = await client.post(`/concepts/${cId}/rules`, body);
  return data;
}

export async function updateRule(cId: number, rId: number, body: Partial<{
  name: string;
  index_id: number;
  sql_query: string;
  index_column: string;
  query_template_params: Record<string, unknown>;
}>): Promise<Rule> {
  const { data } = await client.patch(`/concepts/${cId}/rules/${rId}`, body);
  return data;
}

export async function deleteRule(cId: number, rId: number): Promise<void> {
  await client.delete(`/concepts/${cId}/rules/${rId}`);
}

export async function materializeRule(cId: number, rId: number): Promise<{ r_id: number; dagster_run_id: string; status: string }> {
  const { data } = await client.post(`/concepts/${cId}/rules/${rId}/materialize`);
  return data;
}
