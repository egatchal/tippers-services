import client from '../../../shared/api/client';
import type { WorkflowTemplate, WorkflowTemplateCreate, WorkflowRun, WorkflowRunRequest } from '../types/workflow';

export async function listTemplates(params?: { service?: string }): Promise<WorkflowTemplate[]> {
  const { data } = await client.get('/workflows/templates', { params });
  return data;
}

export async function getTemplate(templateId: number): Promise<WorkflowTemplate> {
  const { data } = await client.get(`/workflows/templates/${templateId}`);
  return data;
}

export async function createTemplate(body: WorkflowTemplateCreate): Promise<WorkflowTemplate> {
  const { data } = await client.post('/workflows/templates', body);
  return data;
}

export async function runWorkflow(body: WorkflowRunRequest): Promise<WorkflowRun> {
  const { data } = await client.post('/workflows/run', body);
  return data;
}

export async function getWorkflowRun(runId: number): Promise<WorkflowRun> {
  const { data } = await client.get(`/workflows/runs/${runId}`);
  return data;
}

export async function cancelWorkflowRun(runId: number): Promise<void> {
  await client.post(`/workflows/runs/${runId}/cancel`);
}
