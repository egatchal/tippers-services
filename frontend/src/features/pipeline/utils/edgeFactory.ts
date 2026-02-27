import type { PipelineEdge } from '../types/edges';
import type { Index, Rule, LabelingFunction, SnorkelJob, ConceptValue } from '../types/entities';

export function buildEdges({
  indexes,
  rules,
  lfs,
  snorkelJobs,
  conceptValues,
  selectedCV,
}: {
  indexes?: Index[];
  rules: Rule[];
  lfs: LabelingFunction[];
  snorkelJobs: SnorkelJob[];
  conceptValues: ConceptValue[];
  selectedCV?: ConceptValue | null;
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

  // Derived index edges: Snorkel → Derived Index, SQL Index → Derived Index
  if (indexes) {
    for (const idx of indexes) {
      if (idx.source_type !== 'derived') continue;
      if (idx.parent_snorkel_job_id != null) {
        edges.push({
          id: `e-snorkel-${idx.parent_snorkel_job_id}-index-${idx.index_id}`,
          source: `snorkel-${idx.parent_snorkel_job_id}`,
          target: `index-${idx.index_id}`,
          type: 'pipelineEdge',
        });
      }
      if (idx.parent_index_id != null) {
        edges.push({
          id: `e-index-${idx.parent_index_id}-index-${idx.index_id}`,
          source: `index-${idx.parent_index_id}`,
          target: `index-${idx.index_id}`,
          type: 'pipelineEdge',
        });
      }
    }
  }

  // Helper: create a non-interactive placeholder edge
  const phEdge = (source: string, target: string): PipelineEdge => ({
    id: `e-${source}-${target}`,
    source,
    target,
    type: 'pipelineEdge',
    selectable: false,
    deletable: false,
    data: { isPlaceholder: true },
  });

  // Placeholder edges — static reference chain, never connects to real nodes.
  // Always shows full flow: Index → Rule → LF → Snorkel
  edges.push(phEdge('placeholder-index', 'placeholder-rule'));
  edges.push(phEdge('placeholder-rule', 'placeholder-lf'));
  edges.push(phEdge('placeholder-lf', 'placeholder-snorkel'));

  return edges;
}
