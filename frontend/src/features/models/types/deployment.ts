export interface Deployment {
  deployment_id: number;
  model_id: number;
  model_version_id: number;
  service: string;
  status: string;
  deploy_config: Record<string, unknown> | null;
  mlflow_model_uri: string | null;
  created_at: string;
  updated_at: string | null;
}

export interface DeploymentCreate {
  model_id: number;
  model_version_id: number;
  service: string;
  deploy_config?: Record<string, unknown>;
}

export interface PredictRequest {
  instances: Record<string, unknown>[];
}

export interface PredictResponse {
  deployment_id: number;
  model_name: string;
  model_version: number;
  predictions: unknown[];
  prediction_probs: number[][] | null;
}
