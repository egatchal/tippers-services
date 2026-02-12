import type { PipelineNode } from '../types/nodes';
import type { Index, Rule, Feature, LabelingFunction, SnorkelJob, ClassifierJob, ConceptValue } from '../types/entities';
import { getStatusFromEntity } from '../../../shared/utils/statusColors';

export function buildNodes({
  indexes,
  rules,
  features,
  lfs,
  snorkelJobs,
  classifierJobs,
  conceptValues,
}: {
  indexes: Index[];
  rules: Rule[];
  features: Feature[];
  lfs: LabelingFunction[];
  snorkelJobs: SnorkelJob[];
  classifierJobs: ClassifierJob[];
  conceptValues: ConceptValue[];
}): PipelineNode[] {
  const nodes: PipelineNode[] = [];

  for (const idx of indexes) {
    const colCount = idx.column_stats ? Object.keys(idx.column_stats).length : undefined;
    nodes.push({
      id: `index-${idx.index_id}`,
      type: 'indexNode',
      position: { x: 0, y: 0 },
      data: {
        entityType: 'index',
        label: idx.name,
        status: getStatusFromEntity(idx),
        summary: idx.is_materialized ? `${idx.row_count ?? 0} rows` : undefined,
        metrics: idx.is_materialized
          ? [
              { label: 'Rows', value: idx.row_count ?? 0 },
              ...(colCount != null ? [{ label: 'Cols', value: colCount }] : []),
            ]
          : undefined,
        entity: idx,
      },
    });
  }

  for (const rule of rules) {
    const colCount = rule.column_stats ? Object.keys(rule.column_stats).length : undefined;
    nodes.push({
      id: `rule-${rule.r_id}`,
      type: 'ruleNode',
      position: { x: 0, y: 0 },
      data: {
        entityType: 'rule',
        label: rule.name,
        status: getStatusFromEntity(rule),
        summary: rule.is_materialized ? `${rule.row_count ?? 0} rows` : undefined,
        metrics: rule.is_materialized
          ? [
              { label: 'Rows', value: rule.row_count ?? 0 },
              ...(colCount != null ? [{ label: 'Cols', value: colCount }] : []),
            ]
          : undefined,
        entity: rule,
      },
    });
  }

  for (const feat of features) {
    const colCount = feat.column_stats ? Object.keys(feat.column_stats).length : undefined;
    nodes.push({
      id: `feature-${feat.feature_id}`,
      type: 'featureNode',
      position: { x: 0, y: 0 },
      data: {
        entityType: 'feature',
        label: feat.name,
        status: getStatusFromEntity(feat),
        summary: feat.is_materialized ? `${feat.row_count ?? 0} rows` : undefined,
        metrics: feat.is_materialized
          ? [
              { label: 'Rows', value: feat.row_count ?? 0 },
              ...(colCount != null ? [{ label: 'Cols', value: colCount }] : []),
            ]
          : undefined,
        entity: feat,
      },
    });
  }

  for (const cv of conceptValues) {
    const lfCount = lfs.filter((lf) => lf.applicable_cv_ids.includes(cv.cv_id)).length;
    nodes.push({
      id: `cv-${cv.cv_id}`,
      type: 'cvNode',
      position: { x: 0, y: 0 },
      data: {
        entityType: 'cv',
        label: cv.name,
        status: 'materialized' as const,
        metrics: [
          { label: 'Level', value: cv.level },
          { label: 'LFs', value: lfCount },
        ],
        entity: cv,
      },
    });
  }

  for (const lf of lfs) {
    nodes.push({
      id: `lf-${lf.lf_id}`,
      type: 'lfNode',
      position: { x: 0, y: 0 },
      data: {
        entityType: 'lf',
        label: lf.name,
        status: lf.is_active ? 'materialized' : 'default',
        summary: lf.coverage != null ? `coverage: ${(lf.coverage * 100).toFixed(0)}%` : undefined,
        metrics: [
          ...(lf.coverage != null ? [{ label: 'Cov', value: `${(lf.coverage * 100).toFixed(0)}%` }] : []),
          { label: 'Status', value: lf.is_active ? 'Active' : 'Off' },
        ],
        entity: lf,
      },
    });
  }

  for (const job of snorkelJobs) {
    nodes.push({
      id: `snorkel-${job.job_id}`,
      type: 'snorkelNode',
      position: { x: 0, y: 0 },
      data: {
        entityType: 'snorkel',
        label: `Snorkel #${job.job_id}`,
        status: getStatusFromEntity(job),
        summary: job.status,
        metrics: [
          { label: 'LFs', value: job.lf_ids.length },
          { label: 'Status', value: job.status },
        ],
        entity: job,
      },
    });
  }

  for (const job of classifierJobs) {
    nodes.push({
      id: `classifier-${job.job_id}`,
      type: 'classifierNode',
      position: { x: 0, y: 0 },
      data: {
        entityType: 'classifier',
        label: `Classifier #${job.job_id}`,
        status: getStatusFromEntity(job),
        summary: job.status,
        metrics: [
          { label: 'Features', value: job.feature_ids.length },
          { label: 'Status', value: job.status },
        ],
        entity: job,
      },
    });
  }

  return nodes;
}
