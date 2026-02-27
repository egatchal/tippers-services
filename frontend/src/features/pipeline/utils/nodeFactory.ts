import type { PipelineNode, CVTreeNodeData, PipelineNodeType } from '../types/nodes';
import type { PipelineEdge } from '../types/edges';
import type { Index, Rule, LabelingFunction, SnorkelJob, ConceptValue } from '../types/entities';
import type { AllEntities } from '../stores/conceptStore';
import { getStatusFromEntity, type NodeStatus } from '../../../shared/utils/statusColors';
import { applyDagreLayout } from './layoutEngine';
import { computeCVTreeStaleness } from './staleness';

/** Create a single PipelineNode from a node ID (e.g. "rule-3") by looking up the entity. */
export function createSingleNode(
  nodeId: string,
  allEntities: AllEntities,
  position: { x: number; y: number },
): PipelineNode | null {
  const dashIdx = nodeId.indexOf('-');
  if (dashIdx === -1) return null;
  const type = nodeId.slice(0, dashIdx);
  const id = Number(nodeId.slice(dashIdx + 1));
  if (isNaN(id)) return null;

  switch (type) {
    case 'index': {
      const idx = allEntities.indexes.find((i) => i.index_id === id);
      if (!idx) return null;
      const isDerived = idx.source_type === 'derived';
      const colCount = idx.column_stats ? Object.keys(idx.column_stats).length : undefined;
      return {
        id: nodeId,
        type: 'indexNode',
        position,
        data: {
          entityType: 'index',
          label: idx.name,
          status: getStatusFromEntity(idx),
          summary: isDerived
            ? `Derived | ${idx.filtered_count ?? 0} entities`
            : idx.is_materialized ? `${idx.row_count ?? 0} rows` : undefined,
          metrics: isDerived
            ? [{ label: 'Entities', value: idx.filtered_count ?? 0 }, { label: 'Type', value: 'Derived' }]
            : idx.is_materialized
              ? [{ label: 'Rows', value: idx.row_count ?? 0 }, ...(colCount != null ? [{ label: 'Cols', value: colCount }] : [])]
              : undefined,
          entity: idx,
        },
      } as PipelineNode;
    }
    case 'rule': {
      const rule = allEntities.rules.find((r) => r.r_id === id);
      if (!rule) return null;
      const colCount = rule.column_stats ? Object.keys(rule.column_stats).length : undefined;
      return {
        id: nodeId,
        type: 'ruleNode',
        position,
        data: {
          entityType: 'rule',
          label: rule.name,
          status: getStatusFromEntity(rule),
          summary: rule.is_materialized ? `${rule.row_count ?? 0} rows` : undefined,
          metrics: rule.is_materialized
            ? [{ label: 'Rows', value: rule.row_count ?? 0 }, ...(colCount != null ? [{ label: 'Cols', value: colCount }] : [])]
            : undefined,
          entity: rule,
        },
      } as PipelineNode;
    }
    case 'cv': {
      const cv = allEntities.conceptValues.find((c) => c.cv_id === id);
      if (!cv) return null;
      const lfCount = allEntities.lfs.filter((lf) => lf.applicable_cv_ids.includes(cv.cv_id)).length;
      return {
        id: nodeId,
        type: 'cvNode',
        position,
        data: {
          entityType: 'cv',
          label: cv.name,
          status: 'materialized' as const,
          summary: `#${cv.cv_id}`,
          metrics: [{ label: 'ID', value: cv.cv_id }, { label: 'Level', value: cv.level }, { label: 'LFs', value: lfCount }],
          entity: cv,
        },
      } as PipelineNode;
    }
    case 'lf': {
      const lf = allEntities.lfs.find((l) => l.lf_id === id);
      if (!lf) return null;
      return {
        id: nodeId,
        type: 'lfNode',
        position,
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
      } as PipelineNode;
    }
    case 'snorkel': {
      const job = allEntities.snorkelJobs.find((sj) => sj.job_id === id);
      if (!job) return null;
      return {
        id: nodeId,
        type: 'snorkelNode',
        position,
        data: {
          entityType: 'snorkel',
          label: `Snorkel #${job.job_id}`,
          status: getStatusFromEntity(job),
          summary: job.status,
          metrics: [{ label: 'LFs', value: job.lf_ids.length }, { label: 'Status', value: job.status }],
          entity: job,
        },
      } as PipelineNode;
    }
    default:
      return null;
  }
}

function makePlaceholder(
  id: string,
  targetType: PipelineNodeType,
  label: string,
  opts?: { hideTopHandle?: boolean; hideBottomHandle?: boolean },
): PipelineNode {
  return {
    id,
    type: 'placeholderNode',
    position: { x: 0, y: 0 },
    zIndex: -1,
    draggable: false,
    selectable: false,
    connectable: false,
    deletable: false,
    data: {
      entityType: 'placeholder',
      targetType,
      label,
      status: 'default',
      hideTopHandle: opts?.hideTopHandle,
      hideBottomHandle: opts?.hideBottomHandle,
    },
  } as PipelineNode;
}

export function buildNodes({
  indexes,
  rules,
  lfs,
  snorkelJobs,
  conceptValues,
  selectedCV,
}: {
  indexes: Index[];
  rules: Rule[];
  lfs: LabelingFunction[];
  snorkelJobs: SnorkelJob[];
  conceptValues: ConceptValue[];
  selectedCV?: ConceptValue | null;
}): PipelineNode[] {
  const nodes: PipelineNode[] = [];

  for (const idx of indexes) {
    const isDerived = idx.source_type === 'derived';
    const colCount = idx.column_stats ? Object.keys(idx.column_stats).length : undefined;
    nodes.push({
      id: `index-${idx.index_id}`,
      type: 'indexNode',
      position: { x: 0, y: 0 },
      data: {
        entityType: 'index',
        label: idx.name,
        status: getStatusFromEntity(idx),
        summary: isDerived
          ? `Derived | ${idx.filtered_count ?? 0} entities`
          : idx.is_materialized ? `${idx.row_count ?? 0} rows` : undefined,
        metrics: isDerived
          ? [
              { label: 'Entities', value: idx.filtered_count ?? 0 },
              { label: 'Type', value: 'Derived' },
            ]
          : idx.is_materialized
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
        summary: `#${cv.cv_id}`,
        metrics: [
          { label: 'ID', value: cv.cv_id },
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

  // Always show all 4 placeholder stages as a static reference (never removed)
  const indexLabel = selectedCV?.parent_cv_id == null ? 'Index' : 'Derived Index';
  nodes.push(makePlaceholder('placeholder-index', 'index', indexLabel, { hideTopHandle: true }));
  nodes.push(makePlaceholder('placeholder-rule', 'rule', 'Rule'));
  nodes.push(makePlaceholder('placeholder-lf', 'lf', 'Labeling Function'));
  nodes.push(makePlaceholder('placeholder-snorkel', 'snorkel', 'Snorkel Run', { hideBottomHandle: true }));

  return nodes;
}

/**
 * Build CV tree nodes and edges from flat concept values list.
 * Used when canvasView === 'tree' to render the hierarchy.
 */
export function buildCVTreeNodes(
  conceptValues: ConceptValue[],
  entities: AllEntities,
): { nodes: PipelineNode[]; edges: PipelineEdge[] } {
  const { indexes, rules, lfs, snorkelJobs } = entities;

  // Compute comprehensive staleness for all CVs (with cascade)
  const cvStaleness = computeCVTreeStaleness(conceptValues, entities);

  const treeNodes: PipelineNode[] = [];
  const treeEdges: PipelineEdge[] = [];

  for (const cv of conceptValues) {
    // Count rules/LFs for this CV's children (the labels it votes on)
    const childCVIds = new Set(conceptValues.filter((c) => c.parent_cv_id === cv.cv_id).map((c) => c.cv_id));
    const relevantLFs = lfs.filter((lf) => lf.applicable_cv_ids?.some((id) => childCVIds.has(id)));
    const relevantRuleIds = new Set(relevantLFs.map((lf) => lf.rule_id));
    const ruleCount = rules.filter((r) => relevantRuleIds.has(r.r_id)).length;

    // Determine snorkel status for this CV (includes both SQL and derived indexes)
    const cvIndexes = indexes.filter((idx) => idx.cv_id === cv.cv_id);
    const cvIndexIds = new Set(cvIndexes.map((idx) => idx.index_id));
    const relatedSnorkel = snorkelJobs.filter((sj) => cvIndexIds.has(sj.index_id));

    let snorkelStatus: 'completed' | 'running' | 'none' = 'none';
    if (relatedSnorkel.some((sj) => sj.status === 'COMPLETED')) {
      snorkelStatus = 'completed';
    } else if (relatedSnorkel.some((sj) => ['RUNNING', 'PENDING'].includes(sj.status))) {
      snorkelStatus = 'running';
    }

    // Use comprehensive staleness from computeCVTreeStaleness
    const cvStale = cvStaleness.get(cv.cv_id);
    const isStale = cvStale?.status === 'stale';

    let nodeStatus: NodeStatus;
    if (snorkelStatus === 'running') nodeStatus = 'running';
    else if (isStale) nodeStatus = 'stale';
    else if (snorkelStatus === 'completed') nodeStatus = 'materialized';
    else nodeStatus = 'default';

    const nodeData: CVTreeNodeData = {
      entityType: 'cvTree',
      label: cv.name,
      status: nodeStatus,
      entity: cv,
      hasCompletedSnorkel: snorkelStatus === 'completed',
      ruleCount,
      lfCount: relevantLFs.length,
      snorkelStatus,
    };

    treeNodes.push({
      id: `cvTree-${cv.cv_id}`,
      type: 'cvTreeNode',
      position: { x: 0, y: 0 },
      data: nodeData,
    } as PipelineNode);

    // Create edge from parent
    if (cv.parent_cv_id != null) {
      treeEdges.push({
        id: `e-cvTree-${cv.parent_cv_id}-cvTree-${cv.cv_id}`,
        source: `cvTree-${cv.parent_cv_id}`,
        target: `cvTree-${cv.cv_id}`,
        type: 'pipelineEdge',
      });
    }
  }

  // Always show example placeholder tree: root + two children
  treeNodes.push(makePlaceholder('placeholder-cv-root', 'cv', 'Root Concept Value', { hideTopHandle: true }));
  treeNodes.push(makePlaceholder('placeholder-cv-child1', 'cv', 'Child Value A', { hideBottomHandle: true }));
  treeNodes.push(makePlaceholder('placeholder-cv-child2', 'cv', 'Child Value B', { hideBottomHandle: true }));
  treeEdges.push({ id: 'e-ph-root-child1', source: 'placeholder-cv-root', target: 'placeholder-cv-child1', type: 'pipelineEdge' });
  treeEdges.push({ id: 'e-ph-root-child2', source: 'placeholder-cv-root', target: 'placeholder-cv-child2', type: 'pipelineEdge' });

  const laidOut = applyDagreLayout(treeNodes, treeEdges);
  return { nodes: laidOut, edges: treeEdges };
}
