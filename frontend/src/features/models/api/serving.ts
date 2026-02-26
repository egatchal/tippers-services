import client from '../../../shared/api/client';
import type { Deployment, DeploymentCreate, PredictRequest, PredictResponse } from '../types/deployment';

export async function listDeployments(params?: { service?: string }): Promise<Deployment[]> {
  const { data } = await client.get('/serving/deployments', { params });
  return data;
}

export async function getDeployment(deploymentId: number): Promise<Deployment> {
  const { data } = await client.get(`/serving/deployments/${deploymentId}`);
  return data;
}

export async function createDeployment(body: DeploymentCreate): Promise<Deployment> {
  const { data } = await client.post('/serving/deployments', body);
  return data;
}

export async function deactivateDeployment(deploymentId: number): Promise<void> {
  await client.delete(`/serving/deployments/${deploymentId}`);
}

export async function predict(deploymentId: number, body: PredictRequest): Promise<PredictResponse> {
  const { data } = await client.post(`/serving/predict/${deploymentId}`, body);
  return data;
}

export async function predictByService(service: string, body: PredictRequest): Promise<PredictResponse> {
  const { data } = await client.post(`/serving/predict-by-service/${service}`, body);
  return data;
}
