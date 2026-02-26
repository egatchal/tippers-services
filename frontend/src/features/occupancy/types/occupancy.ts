export interface Space {
  space_id: number;
  space_name: string | null;
  parent_space_id: number | null;
  building_room: string | null;
}

export interface OccupancyDatasetCreate {
  name: string;
  description?: string;
  root_space_id: number;
  start_time?: string;
  end_time?: string;
  interval_seconds: number;
  force_overwrite?: boolean;
}

export interface OccupancyDataset {
  dataset_id: number;
  name: string;
  description: string | null;
  root_space_id: number;
  start_time: string;
  end_time: string;
  interval_seconds: number;
  chunk_days: number | null;
  status: string;
  dagster_run_id: string | null;
  storage_path: string | null;
  row_count: number | null;
  column_stats: Record<string, unknown> | null;
  error_message: string | null;
  created_at: string;
  completed_at: string | null;
}

export interface OccupancyResultRow {
  interval_begin_time: string;
  number_connections: number;
  [key: string]: unknown;
}

export interface OccupancyResults {
  dataset_id: number;
  space_id: number;
  status: string;
  completed_chunks: number;
  total_chunks: number;
  message?: string;
  error?: string;
  row_count?: number;
  rows?: OccupancyResultRow[];
}
