export interface Concept {
  c_id: number;
  name: string;
  description?: string;
  created_at: string;
  updated_at: string;
}

export interface ConceptValue {
  cv_id: number;
  c_id: number;
  name: string;
  description?: string;
  display_order?: number;
  level: number;
  parent_cv_id?: number;
  created_at: string;
}

export interface ConceptValueTreeNode extends ConceptValue {
  children: ConceptValueTreeNode[];
}

export interface EntityPreviewResponse {
  index_id: number;
  source_type: string;
  total_count: number;
  entities: Record<string, unknown>[];
}

export interface ColumnStat {
  dtype: string;
  null_count: number;
  min?: number;
  max?: number;
  mean?: number;
  std?: number;
  unique_count?: number;
  top_values?: Record<string, number>;
  [key: string]: unknown;
}

export interface Index {
  index_id: number;
  c_id: number;
  conn_id: number;
  name: string;
  key_column?: string;
  sql_query: string;
  query_template_params?: Record<string, unknown>;
  partition_type?: string;
  partition_config?: Record<string, unknown>;
  storage_path?: string;
  is_materialized: boolean;
  materialized_at?: string;
  row_count?: number;
  column_stats?: Record<string, ColumnStat>;
  source_type?: string;
  cv_id?: number;
  parent_index_id?: number;
  parent_snorkel_job_id?: number;
  label_filter?: Record<string, unknown>;
  filtered_count?: number;
  output_type?: string;
  created_at: string;
  updated_at: string;
}

export interface Rule {
  r_id: number;
  c_id: number;
  index_id: number;
  name: string;
  sql_query: string;
  query_template_params?: Record<string, unknown>;
  partition_type?: string;
  partition_config?: Record<string, unknown>;
  storage_path?: string;
  is_materialized: boolean;
  materialized_at?: string;
  row_count?: number;
  column_stats?: Record<string, ColumnStat>;
  created_at: string;
  updated_at: string;
}


export interface LabelingFunction {
  lf_id: number;
  c_id: number;
  applicable_cv_ids: number[];
  rule_id: number;
  name: string;
  version: number;
  parent_lf_id?: number;
  lf_type: string;
  lf_config: { code?: string; allowed_imports?: string[] };
  is_active: boolean;
  requires_approval: boolean;
  deprecated_at?: string;
  deprecated_by_lf_id?: number;
  estimated_accuracy?: number;
  coverage?: number;
  conflicts?: number;
  created_at: string;
  updated_at: string;
}

export interface SnorkelJob {
  job_id: number;
  c_id: number;
  index_id: number | null;
  rule_ids: number[];
  lf_ids: number[];
  config: { epochs?: number; lr?: number; sample_size?: number };
  output_type: string;
  dagster_run_id?: string;
  status: string;
  result_path?: string;
  error_message?: string;
  created_at: string;
  completed_at?: string;
}


export interface SnorkelResults {
  job_id: number;
  status: string;
  output_type: string;
  lf_summary: Array<Record<string, unknown>>;
  label_matrix_class_distribution: Record<string, number>;
  model_class_distribution: Record<string, number>;
  overall_stats: Record<string, unknown>;
  cv_id_to_name: Record<string, string>;
  cv_id_to_index: Record<string, number>;
  predictions?: { probabilities: number[][]; labels: number[]; sample_ids: unknown[] };
}

