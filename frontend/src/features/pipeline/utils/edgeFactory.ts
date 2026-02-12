import type { PipelineEdge } from '../types/edges';
import type { Index, Rule, Feature, LabelingFunction, SnorkelJob, ClassifierJob, ConceptValue } from '../types/entities';

export function buildEdges({
  rules,
  features,
  lfs,
  snorkelJobs,
  classifierJobs,
  conceptValues,
}: {
  indexes?: Index[];
  rules: Rule[];
  features: Feature[];
  lfs: LabelingFunction[];
  snorkelJobs: SnorkelJob[];
  classifierJobs: ClassifierJob[];
  conceptValues: ConceptValue[];
}): PipelineEdge[] {
  const edges: PipelineEdge[] = [];

  // Index → Rule (via rule.index_id)
  for (const rule of rules) {
    edges.push({
      id: `e-index-${rule.index_id}-rule-${rule.r_id}`,
      source: `index-${rule.index_id}`,
      target: `rule-${rule.r_id}`,
      type: 'pipelineEdge',
    });
  }

  // Index → Feature (via feature.index_id)
  for (const feat of features) {
    edges.push({
      id: `e-index-${feat.index_id}-feature-${feat.feature_id}`,
      source: `index-${feat.index_id}`,
      target: `feature-${feat.feature_id}`,
      type: 'pipelineEdge',
    });
  }

  // Rule → LF (via lf.rule_id)
  for (const lf of lfs) {
    edges.push({
      id: `e-rule-${lf.rule_id}-lf-${lf.lf_id}`,
      source: `rule-${lf.rule_id}`,
      target: `lf-${lf.lf_id}`,
      type: 'pipelineEdge',
    });
  }

  // CV → LF (via lf.applicable_cv_ids)
  const cvIdSet = new Set(conceptValues.map((cv) => cv.cv_id));
  for (const lf of lfs) {
    for (const cvId of lf.applicable_cv_ids) {
      if (cvIdSet.has(cvId)) {
        edges.push({
          id: `e-cv-${cvId}-lf-${lf.lf_id}`,
          source: `cv-${cvId}`,
          target: `lf-${lf.lf_id}`,
          type: 'pipelineEdge',
        });
      }
    }
  }

  // LF → Snorkel (via snorkelJob.lf_ids)
  for (const job of snorkelJobs) {
    for (const lfId of job.lf_ids) {
      edges.push({
        id: `e-lf-${lfId}-snorkel-${job.job_id}`,
        source: `lf-${lfId}`,
        target: `snorkel-${job.job_id}`,
        type: 'pipelineEdge',
        data: { animated: job.status === 'RUNNING' || job.status === 'PENDING' },
      });
    }
  }

  // Feature → Classifier (via classifierJob.feature_ids)
  for (const job of classifierJobs) {
    for (const fId of job.feature_ids) {
      edges.push({
        id: `e-feature-${fId}-classifier-${job.job_id}`,
        source: `feature-${fId}`,
        target: `classifier-${job.job_id}`,
        type: 'pipelineEdge',
        data: { animated: job.status === 'RUNNING' || job.status === 'PENDING' },
      });
    }

    // Snorkel → Classifier (via classifierJob.snorkel_job_id)
    edges.push({
      id: `e-snorkel-${job.snorkel_job_id}-classifier-${job.job_id}`,
      source: `snorkel-${job.snorkel_job_id}`,
      target: `classifier-${job.job_id}`,
      type: 'pipelineEdge',
      data: { animated: job.status === 'RUNNING' || job.status === 'PENDING' },
    });
  }

  return edges;
}
