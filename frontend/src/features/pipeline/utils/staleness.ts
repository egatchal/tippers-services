import type { PipelineNode, PipelineNodeType } from '../types/nodes';
import type { Index, Rule, Feature, LabelingFunction, SnorkelJob, ClassifierJob } from '../types/entities';
import type { NodeStatus } from '../../../shared/utils/statusColors';

export type StaleStatus = 'fresh' | 'stale' | 'running' | 'n/a';

export interface ExecutionStep {
  tier: number; // 0=index, 1=rule/feature, 2=snorkel, 3=classifier
  action: 'materialize' | 'run';
  entityType: PipelineNodeType;
  entityId: number;
  label: string;
}

/**
 * Compute transitive staleness for every node in the pipeline.
 * Returns a Map from node ID (e.g. "index-1") to StaleStatus.
 */
export function computeStaleness(nodes: PipelineNode[]): Map<string, StaleStatus> {
  const result = new Map<string, StaleStatus>();

  // Build lookup tables by entity type + ID
  const indexMap = new Map<number, { node: PipelineNode; entity: Index }>();
  const ruleMap = new Map<number, { node: PipelineNode; entity: Rule }>();
  const featureMap = new Map<number, { node: PipelineNode; entity: Feature }>();
  const lfMap = new Map<number, { node: PipelineNode; entity: LabelingFunction }>();
  const snorkelMap = new Map<number, { node: PipelineNode; entity: SnorkelJob }>();
  const classifierMap = new Map<number, { node: PipelineNode; entity: ClassifierJob }>();

  for (const n of nodes) {
    const d = n.data;
    switch (d.entityType) {
      case 'index':
        indexMap.set((d.entity as Index).index_id, { node: n, entity: d.entity as Index });
        break;
      case 'rule':
        ruleMap.set((d.entity as Rule).r_id, { node: n, entity: d.entity as Rule });
        break;
      case 'feature':
        featureMap.set((d.entity as Feature).feature_id, { node: n, entity: d.entity as Feature });
        break;
      case 'lf':
        lfMap.set((d.entity as LabelingFunction).lf_id, { node: n, entity: d.entity as LabelingFunction });
        break;
      case 'snorkel':
        snorkelMap.set((d.entity as SnorkelJob).job_id, { node: n, entity: d.entity as SnorkelJob });
        break;
      case 'classifier':
        classifierMap.set((d.entity as ClassifierJob).job_id, { node: n, entity: d.entity as ClassifierJob });
        break;
      case 'cv':
        result.set(n.id, 'n/a');
        break;
    }
  }

  function isRunning(status: string | undefined): boolean {
    if (!status) return false;
    const s = status.toUpperCase();
    return s === 'RUNNING' || s === 'PENDING';
  }

  // Tier 0: Indexes (root nodes)
  for (const [, { node, entity }] of indexMap) {
    if (node.data.status === 'running' || node.data.status === 'pending') {
      result.set(node.id, 'running');
    } else if (!entity.is_materialized) {
      result.set(node.id, 'stale');
    } else {
      result.set(node.id, 'fresh');
    }
  }

  // Tier 1a: Rules (depend on parent index)
  for (const [, { node, entity }] of ruleMap) {
    if (node.data.status === 'running' || node.data.status === 'pending') {
      result.set(node.id, 'running');
    } else if (!entity.is_materialized) {
      result.set(node.id, 'stale');
    } else {
      const parentIdx = indexMap.get(entity.index_id);
      const parentStatus = parentIdx ? result.get(parentIdx.node.id) : undefined;
      if (parentStatus === 'stale' || parentStatus === 'running') {
        result.set(node.id, 'stale');
      } else if (parentIdx && parentIdx.entity.materialized_at && entity.materialized_at) {
        if (new Date(parentIdx.entity.materialized_at) > new Date(entity.materialized_at)) {
          result.set(node.id, 'stale');
        } else {
          result.set(node.id, 'fresh');
        }
      } else {
        result.set(node.id, 'fresh');
      }
    }
  }

  // Tier 1b: Features (depend on parent index)
  for (const [, { node, entity }] of featureMap) {
    if (node.data.status === 'running' || node.data.status === 'pending') {
      result.set(node.id, 'running');
    } else if (!entity.is_materialized) {
      result.set(node.id, 'stale');
    } else {
      const parentIdx = indexMap.get(entity.index_id);
      const parentStatus = parentIdx ? result.get(parentIdx.node.id) : undefined;
      if (parentStatus === 'stale' || parentStatus === 'running') {
        result.set(node.id, 'stale');
      } else if (parentIdx && parentIdx.entity.materialized_at && entity.materialized_at) {
        if (new Date(parentIdx.entity.materialized_at) > new Date(entity.materialized_at)) {
          result.set(node.id, 'stale');
        } else {
          result.set(node.id, 'fresh');
        }
      } else {
        result.set(node.id, 'fresh');
      }
    }
  }

  // LFs: stale if parent rule is stale
  for (const [, { node, entity }] of lfMap) {
    const parentRule = ruleMap.get(entity.rule_id);
    const parentStatus = parentRule ? result.get(parentRule.node.id) : undefined;
    if (parentStatus === 'stale' || parentStatus === 'running') {
      result.set(node.id, 'stale');
    } else {
      result.set(node.id, 'fresh');
    }
  }

  // Snorkel jobs: stale if any upstream is stale or upstream was re-materialized after completion
  for (const [, { node, entity }] of snorkelMap) {
    if (isRunning(entity.status)) {
      result.set(node.id, 'running');
      continue;
    }
    if (entity.status === 'FAILED') {
      result.set(node.id, 'stale');
      continue;
    }

    let entityIsStale = false;

    // Check input index
    const parentIdx = indexMap.get(entity.index_id);
    if (parentIdx) {
      const idxStatus = result.get(parentIdx.node.id);
      if (idxStatus === 'stale') entityIsStale = true;
      if (entity.completed_at && parentIdx.entity.materialized_at) {
        if (new Date(parentIdx.entity.materialized_at) > new Date(entity.completed_at)) {
          entityIsStale = true;
        }
      }
    }

    // Check input rules (derived from LF rule_ids)
    const ruleIdsUsed = new Set<number>();
    for (const lfId of entity.lf_ids) {
      const lf = lfMap.get(lfId);
      if (lf) ruleIdsUsed.add(lf.entity.rule_id);
    }
    for (const ruleId of ruleIdsUsed) {
      const rule = ruleMap.get(ruleId);
      if (rule) {
        const ruleStatus = result.get(rule.node.id);
        if (ruleStatus === 'stale') entityIsStale = true;
        if (entity.completed_at && rule.entity.materialized_at) {
          if (new Date(rule.entity.materialized_at) > new Date(entity.completed_at)) {
            entityIsStale = true;
          }
        }
      }
    }

    // Check if any LF's parent rule is stale
    for (const lfId of entity.lf_ids) {
      const lf = lfMap.get(lfId);
      if (lf) {
        const lfStatus = result.get(lf.node.id);
        if (lfStatus === 'stale') entityIsStale = true;
      }
    }

    result.set(node.id, entityIsStale ? 'stale' : 'fresh');
  }

  // Classifier jobs: stale if source snorkel is stale or any input feature is stale
  for (const [, { node, entity }] of classifierMap) {
    if (isRunning(entity.status)) {
      result.set(node.id, 'running');
      continue;
    }
    if (entity.status === 'FAILED') {
      result.set(node.id, 'stale');
      continue;
    }

    let entityIsStale = false;

    // Check source snorkel job
    const srcSnorkel = snorkelMap.get(entity.snorkel_job_id);
    if (srcSnorkel) {
      const snorkelStatus = result.get(srcSnorkel.node.id);
      if (snorkelStatus === 'stale') entityIsStale = true;
      if (entity.completed_at && srcSnorkel.entity.completed_at) {
        if (new Date(srcSnorkel.entity.completed_at) > new Date(entity.completed_at)) {
          entityIsStale = true;
        }
      }
    }

    // Check input features
    for (const fId of entity.feature_ids) {
      const feat = featureMap.get(fId);
      if (feat) {
        const featStatus = result.get(feat.node.id);
        if (featStatus === 'stale') entityIsStale = true;
        if (entity.completed_at && feat.entity.materialized_at) {
          if (new Date(feat.entity.materialized_at) > new Date(entity.completed_at)) {
            entityIsStale = true;
          }
        }
      }
    }

    result.set(node.id, entityIsStale ? 'stale' : 'fresh');
  }

  return result;
}

/**
 * Build an ordered execution plan to re-run a target (or the full pipeline).
 * Only includes steps that are currently stale or unmaterialized.
 */
export function buildExecutionPlan(
  target:
    | { type: 'snorkel'; entity: SnorkelJob }
    | { type: 'classifier'; entity: ClassifierJob }
    | { type: 'all' },
  nodes: PipelineNode[],
  staleness: Map<string, StaleStatus>,
): ExecutionStep[] {
  const steps: ExecutionStep[] = [];

  // Build entity lookup maps
  const indexMap = new Map<number, { node: PipelineNode; entity: Index }>();
  const ruleMap = new Map<number, { node: PipelineNode; entity: Rule }>();
  const featureMap = new Map<number, { node: PipelineNode; entity: Feature }>();
  const lfMap = new Map<number, { node: PipelineNode; entity: LabelingFunction }>();
  const snorkelMap = new Map<number, { node: PipelineNode; entity: SnorkelJob }>();
  const classifierMap = new Map<number, { node: PipelineNode; entity: ClassifierJob }>();

  for (const n of nodes) {
    const d = n.data;
    switch (d.entityType) {
      case 'index': indexMap.set((d.entity as Index).index_id, { node: n, entity: d.entity as Index }); break;
      case 'rule': ruleMap.set((d.entity as Rule).r_id, { node: n, entity: d.entity as Rule }); break;
      case 'feature': featureMap.set((d.entity as Feature).feature_id, { node: n, entity: d.entity as Feature }); break;
      case 'lf': lfMap.set((d.entity as LabelingFunction).lf_id, { node: n, entity: d.entity as LabelingFunction }); break;
      case 'snorkel': snorkelMap.set((d.entity as SnorkelJob).job_id, { node: n, entity: d.entity as SnorkelJob }); break;
      case 'classifier': classifierMap.set((d.entity as ClassifierJob).job_id, { node: n, entity: d.entity as ClassifierJob }); break;
    }
  }

  // Collect which indexes, rules, features, snorkel, classifier need re-running
  const staleIndexIds = new Set<number>();
  const staleRuleIds = new Set<number>();
  const staleFeatureIds = new Set<number>();
  let includeSnorkel: SnorkelJob | null = null;
  let includeClassifier: ClassifierJob | null = null;

  if (target.type === 'snorkel') {
    const job = target.entity;
    includeSnorkel = job;

    // Collect upstream: index, rules (via LFs)
    const idx = indexMap.get(job.index_id);
    if (idx && staleness.get(idx.node.id) === 'stale') {
      staleIndexIds.add(job.index_id);
    }

    for (const lfId of job.lf_ids) {
      const lf = lfMap.get(lfId);
      if (lf) {
        const rule = ruleMap.get(lf.entity.rule_id);
        if (rule && staleness.get(rule.node.id) === 'stale') {
          staleRuleIds.add(lf.entity.rule_id);
          const ruleIdx = indexMap.get(rule.entity.index_id);
          if (ruleIdx && staleness.get(ruleIdx.node.id) === 'stale') {
            staleIndexIds.add(rule.entity.index_id);
          }
        }
      }
    }
  } else if (target.type === 'classifier') {
    const job = target.entity;
    includeClassifier = job;

    // Check if source snorkel is stale
    const srcSnorkel = snorkelMap.get(job.snorkel_job_id);
    if (srcSnorkel && staleness.get(srcSnorkel.node.id) === 'stale') {
      includeSnorkel = srcSnorkel.entity;

      // Collect snorkel's upstream
      const idx = indexMap.get(srcSnorkel.entity.index_id);
      if (idx && staleness.get(idx.node.id) === 'stale') {
        staleIndexIds.add(srcSnorkel.entity.index_id);
      }
      for (const lfId of srcSnorkel.entity.lf_ids) {
        const lf = lfMap.get(lfId);
        if (lf) {
          const rule = ruleMap.get(lf.entity.rule_id);
          if (rule && staleness.get(rule.node.id) === 'stale') {
            staleRuleIds.add(lf.entity.rule_id);
            const ruleIdx = indexMap.get(rule.entity.index_id);
            if (ruleIdx && staleness.get(ruleIdx.node.id) === 'stale') {
              staleIndexIds.add(rule.entity.index_id);
            }
          }
        }
      }
    }

    // Check stale features
    for (const fId of job.feature_ids) {
      const feat = featureMap.get(fId);
      if (feat && staleness.get(feat.node.id) === 'stale') {
        staleFeatureIds.add(fId);
        const featIdx = indexMap.get(feat.entity.index_id);
        if (featIdx && staleness.get(featIdx.node.id) === 'stale') {
          staleIndexIds.add(feat.entity.index_id);
        }
      }
    }
  } else {
    // 'all' — collect everything stale
    for (const [entId, { node }] of indexMap) {
      if (staleness.get(node.id) === 'stale') staleIndexIds.add(entId);
    }
    for (const [entId, { node }] of ruleMap) {
      if (staleness.get(node.id) === 'stale') staleRuleIds.add(entId);
    }
    for (const [entId, { node }] of featureMap) {
      if (staleness.get(node.id) === 'stale') staleFeatureIds.add(entId);
    }
    for (const [, { node, entity: snorkelEntity }] of snorkelMap) {
      if (staleness.get(node.id) === 'stale' && !includeSnorkel) {
        includeSnorkel = snorkelEntity;
      }
    }
    for (const [, { node, entity: classifierEntity }] of classifierMap) {
      if (staleness.get(node.id) === 'stale' && !includeClassifier) {
        includeClassifier = classifierEntity;
      }
    }
  }

  // Build steps by tier
  // Tier 0: Indexes
  for (const indexId of staleIndexIds) {
    const idx = indexMap.get(indexId);
    if (idx) {
      steps.push({
        tier: 0,
        action: 'materialize',
        entityType: 'index',
        entityId: indexId,
        label: `Materialize Index: ${idx.entity.name}`,
      });
    }
  }

  // Tier 1: Rules + Features
  for (const ruleId of staleRuleIds) {
    const rule = ruleMap.get(ruleId);
    if (rule) {
      steps.push({
        tier: 1,
        action: 'materialize',
        entityType: 'rule',
        entityId: ruleId,
        label: `Materialize Rule: ${rule.entity.name}`,
      });
    }
  }
  for (const featureId of staleFeatureIds) {
    const feat = featureMap.get(featureId);
    if (feat) {
      steps.push({
        tier: 1,
        action: 'materialize',
        entityType: 'feature',
        entityId: featureId,
        label: `Materialize Feature: ${feat.entity.name}`,
      });
    }
  }

  // Tier 2: Snorkel
  if (includeSnorkel) {
    steps.push({
      tier: 2,
      action: 'run',
      entityType: 'snorkel',
      entityId: includeSnorkel.job_id,
      label: `Run Snorkel #${includeSnorkel.job_id}`,
    });
  }

  // Tier 3: Classifier
  if (includeClassifier) {
    steps.push({
      tier: 3,
      action: 'run',
      entityType: 'classifier',
      entityId: includeClassifier.job_id,
      label: `Run Classifier #${includeClassifier.job_id}`,
    });
  }

  return steps.sort((a, b) => a.tier - b.tier);
}

/**
 * Overlay staleness onto node statuses: returns the NodeStatus to display.
 * If a node is "materialized" but stale, returns "stale" instead.
 */
export function overlayStaleStatus(
  baseStatus: NodeStatus,
  staleStatus: StaleStatus | undefined,
): NodeStatus {
  if (staleStatus === 'stale' && (baseStatus === 'materialized' || baseStatus === 'default')) {
    return 'stale';
  }
  return baseStatus;
}
