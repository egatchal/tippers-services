export interface Dataset {
  dataset_id: number;
  name: string;
  description: string | null;
  service: string;
  dataset_type: string;
  format: string;
  storage_path: string | null;
  source_ref: Record<string, unknown> | null;
  row_count: number | null;
  column_stats: Record<string, unknown> | null;
  schema_info: Record<string, unknown> | null;
  tags: Record<string, unknown> | null;
  status: string;
  created_at: string;
  updated_at: string | null;
}

export interface DatasetCreate {
  name: string;
  description?: string;
  service: string;
  dataset_type: string;
  format?: string;
  storage_path?: string;
  row_count?: number;
  tags?: Record<string, unknown>;
}

export interface DatasetPreview {
  dataset_id: number;
  name: string;
  columns: string[];
  rows: Record<string, unknown>[];
  total_rows: number;
}
