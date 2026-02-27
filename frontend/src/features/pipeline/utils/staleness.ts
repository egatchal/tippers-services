import type { PipelineNode, PipelineNodeType } from '../types/nodes';
import type { Index, Rule, LabelingFunction, SnorkelJob, ConceptValue } from '../types/entities';
import type { NodeStatus } from '../../../shared/utils/statusColors';
import type { AllEntities } from '../stores/conceptStore';
import { filterEntitiesForCV } from './entityFilter';

export type StaleStatus = 'fresh' | 'stale' | 'running' | 'n/a';

export interface ExecutionStep {
  tier: number; // 0=index, 1=rule, 2=snorkel (or cv.level*3 + entityTier for multi-CV plans)
  action: 'materialize' | 'run';
  entityType: PipelineNodeType;
  entityId: number;
  label: string;
  cvName?: string; // present when step belongs to a specific CV (for multi-CV display grouping)
}

/* ------------------------------------------------------------------ */
/*  CV-level staleness (used by tree view & entity-based exec plans)  */
/* ------------------------------------------------------------------ */

export interface CVStaleInfo {
  status: StaleStatus;
  staleIndexIds: Set<number>;
  staleRuleIds: Set<number>;
  staleSnorkelId: number | null; // the snorkel job to re-run
}

/**
 * Compute staleness for every CV based on ALL inner components.
 * Processes CVs in topological order (roots first) so parent staleness cascades.
 */
export function computeCVTreeStaleness(
  conceptValues: ConceptValue[],
  allEntities: AllEntities,
): Map<number, CVStaleInfo> {
  const result = new Map<number, CVStaleInfo>();

  // Sort topologically: roots (level 0 / no parent) first, then by ascending level
  const sorted = [...conceptValues].sort((a, b) => a.level - b.level);

  for (const cv of sorted) {
    const filtered = filterEntitiesForCV(
      cv,
      conceptValues,
      allEntities.indexes,
      allEntities.rules,
      allEntities.lfs,
      allEntities.snorkelJobs,
    );

    const staleIndexIds = new Set<number>();
    const staleRuleIds = new Set<number>();
    let staleSnorkelId: number | null = null;
    let isStale = false;

    // --- Check indexes ---
    for (const idx of filtered.indexes) {
      if (!idx.is_materialized) {
        staleIndexIds.add(idx.index_id);
        isStale = true;
      }
    }

    // --- Check rules ---
    // A rule is stale if: not materialized, OR its parent index is stale,
    // OR it was materialized before its parent index
    for (const rule of filtered.rules) {
      if (!rule.is_materialized) {
        staleRuleIds.add(rule.r_id);
        isStale = true;
        continue;
      }
      // Check if parent index is stale or was re-materialized after the rule
      const parentIdx = filtered.indexes.find((i) => i.index_id === rule.index_id);
      if (parentIdx && staleIndexIds.has(parentIdx.index_id)) {
        staleRuleIds.add(rule.r_id);
        isStale = true;
      } else if (parentIdx?.materialized_at && rule.materialized_at) {
        if (new Date(parentIdx.materialized_at) > new Date(rule.materialized_at)) {
          staleRuleIds.add(rule.r_id);
          isStale = true;
        }
      }
    }

    // --- Check snorkel jobs ---
    // Find the latest snorkel job for this CV
    const cvSnorkelJobs = filtered.snorkelJobs.filter((sj) => sj.index_id != null);
    const latestSnorkel = cvSnorkelJobs
      .sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())[0];

    if (latestSnorkel) {
      // Non-COMPLETED (DRAFT, FAILED, etc.) → stale
      if (latestSnorkel.status !== 'COMPLETED') {
        staleSnorkelId = latestSnorkel.job_id;
        isStale = true;
      } else {
        // COMPLETED but upstream changed after it finished
        let snorkelIsStale = false;

        // Check if any index was materialized after snorkel completed
        for (const idx of filtered.indexes) {
          if (idx.materialized_at && latestSnorkel.completed_at) {
            if (new Date(idx.materialized_at) > new Date(latestSnorkel.completed_at)) {
              snorkelIsStale = true;
            }
          }
        }

        // Check if any rule was materialized after snorkel completed
        for (const rule of filtered.rules) {
          if (rule.materialized_at && latestSnorkel.completed_at) {
            if (new Date(rule.materialized_at) > new Date(latestSnorkel.completed_at)) {
              snorkelIsStale = true;
            }
          }
        }

        // Check if any upstream entity (index or rule) is stale
        if (staleIndexIds.size > 0 || staleRuleIds.size > 0) {
          snorkelIsStale = true;
        }

        if (snorkelIsStale) {
          staleSnorkelId = latestSnorkel.job_id;
          isStale = true;
        }
      }
    }

    // --- Cascade from parent CV ---
    if (cv.parent_cv_id != null) {
      const parentInfo = result.get(cv.parent_cv_id);
      if (parentInfo?.status === 'stale') {
        isStale = true;
        // If parent is stale, derived indexes need re-materialization
        for (const idx of filtered.indexes) {
          if (idx.source_type === 'derived') {
            staleIndexIds.add(idx.index_id);
          }
        }
      }
    }

    result.set(cv.cv_id, {
      status: isStale ? 'stale' : 'fresh',
      staleIndexIds,
      staleRuleIds,
      staleSnorkelId,
    });
  }

  return result;
}

/**
 * Build an execution plan directly from entities (not canvas nodes).
 * Used by tree-view "Re-run Pipeline" and per-CV "Re-run Stale" buttons.
 */
export function buildExecutionPlanFromEntities(
  allEntities: AllEntities,
  conceptValues: ConceptValue[],
  cvFilter?: number, // optional: scope to a single CV
): ExecutionStep[] {
  const steps: ExecutionStep[] = [];
  const cvStaleness = computeCVTreeStaleness(conceptValues, allEntities);
  const seenEntityIds = new Set<string>(); // "index-5", "rule-3" etc. to deduplicate

  const cvsToProcess = cvFilter != null
    ? conceptValues.filter((cv) => cv.cv_id === cvFilter)
    : conceptValues;

  for (const cv of cvsToProcess) {
    const info = cvStaleness.get(cv.cv_id);
    if (!info || info.status !== 'stale') continue;

    const baseTier = cv.level * 3;

    // Tier 0 (within this CV level): stale indexes
    for (const indexId of info.staleIndexIds) {
      const key = `index-${indexId}`;
      if (seenEntityIds.has(key)) continue;
      seenEntityIds.add(key);

      const idx = allEntities.indexes.find((i) => i.index_id === indexId);
      if (!idx) continue;

      steps.push({
        tier: baseTier + 0,
        action: 'materialize',
        entityType: 'index',
        entityId: indexId,
        label: `Materialize Index: ${idx.name}`,
        cvName: cv.name,
      });
    }

    // Tier 1 (within this CV level): stale rules
    for (const ruleId of info.staleRuleIds) {
      const key = `rule-${ruleId}`;
      if (seenEntityIds.has(key)) continue;
      seenEntityIds.add(key);

      const rule = allEntities.rules.find((r) => r.r_id === ruleId);
      if (!rule) continue;

      steps.push({
        tier: baseTier + 1,
        action: 'materialize',
        entityType: 'rule',
        entityId: ruleId,
        label: `Materialize Rule: ${rule.name}`,
        cvName: cv.name,
      });
    }

    // Tier 2 (within this CV level): snorkel job
    if (info.staleSnorkelId != null) {
      const key = `snorkel-${info.staleSnorkelId}`;
      if (!seenEntityIds.has(key)) {
        seenEntityIds.add(key);
        steps.push({
          tier: baseTier + 2,
          action: 'run',
          entityType: 'snorkel',
          entityId: info.staleSnorkelId,
          label: `Run Snorkel #${info.staleSnorkelId}`,
          cvName: cv.name,
        });
      }
    }
  }

  return steps.sort((a, b) => a.tier - b.tier);
}

/**
 * Compute transitive staleness for every node in the pipeline.
 * Returns a Map from node ID (e.g. "index-1") to StaleStatus.
 *
 * `allIndexes` and `allSnorkelJobs` provide cross-view lookups so derived
 * indexes can resolve their parent SQL index or parent Snorkel job even when
 * those parents aren't on the current canvas (they live in the root/tree view).
 */
export function computeStaleness(
  nodes: PipelineNode[],
  allIndexes?: Index[],
  allSnorkelJobs?: SnorkelJob[],
): Map<string, StaleStatus> {
  const result = new Map<string, StaleStatus>();

  // Build lookup tables by entity type + ID
  const indexMap = new Map<number, { node: PipelineNode; entity: Index }>();
  const ruleMap = new Map<number, { node: PipelineNode; entity: Rule }>();
  const lfMap = new Map<number, { node: PipelineNode; entity: LabelingFunction }>();
  const snorkelMap = new Map<number, { node: PipelineNode; entity: SnorkelJob }>();

  for (const n of nodes) {
    const d = n.data;
    switch (d.entityType) {
      case 'index':
        indexMap.set((d.entity as Index).index_id, { node: n, entity: d.entity as Index });
        break;
      case 'rule':
        ruleMap.set((d.entity as Rule).r_id, { node: n, entity: d.entity as Rule });
        break;
      case 'lf':
        lfMap.set((d.entity as LabelingFunction).lf_id, { node: n, entity: d.entity as LabelingFunction });
        break;
      case 'snorkel':
        snorkelMap.set((d.entity as SnorkelJob).job_id, { node: n, entity: d.entity as SnorkelJob });
        break;
      case 'cv':
      case 'cvTree':
        result.set(n.id, 'n/a');
        break;
    }
  }

  function isRunning(status: string | undefined): boolean {
    if (!status) return false;
    const s = status.toUpperCase();
    return s === 'RUNNING' || s === 'PENDING';
  }

  // Tier 0a: SQL indexes (root nodes)
  for (const [, { node, entity }] of indexMap) {
    if (entity.source_type === 'derived') continue;
    if (node.data.status === 'running' || node.data.status === 'pending') {
      result.set(node.id, 'running');
    } else if (!entity.is_materialized) {
      result.set(node.id, 'stale');
    } else {
      result.set(node.id, 'fresh');
    }
  }

  // Tier 0b: Derived indexes — must be computed BEFORE rules so rules can
  // see their parent derived index's staleness via result.get()
  for (const [, { node, entity }] of indexMap) {
    if (entity.source_type !== 'derived') continue;
    if (node.data.status === 'running' || node.data.status === 'pending') {
      result.set(node.id, 'running');
      continue;
    }

    let entityIsStale = !entity.is_materialized;

    if (entity.parent_index_id != null) {
      const parentIdx = indexMap.get(entity.parent_index_id);
      if (parentIdx) {
        const parentStatus = result.get(parentIdx.node.id);
        if (parentStatus === 'stale') entityIsStale = true;
        if (entity.materialized_at && parentIdx.entity.materialized_at) {
          if (new Date(parentIdx.entity.materialized_at) > new Date(entity.materialized_at)) {
            entityIsStale = true;
          }
        }
      } else {
        // Parent index not on canvas — look up from full entity list
        const parentEntity = allIndexes?.find((i) => i.index_id === entity.parent_index_id);
        if (parentEntity && entity.materialized_at && parentEntity.materialized_at) {
          if (new Date(parentEntity.materialized_at) > new Date(entity.materialized_at)) {
            entityIsStale = true;
          }
        }
      }
    }

    if (entity.parent_snorkel_job_id != null) {
      const parentSnorkel = snorkelMap.get(entity.parent_snorkel_job_id);
      if (parentSnorkel) {
        const parentStatus = result.get(parentSnorkel.node.id);
        if (parentStatus === 'stale') entityIsStale = true;
        if (entity.materialized_at && parentSnorkel.entity.completed_at) {
          if (new Date(parentSnorkel.entity.completed_at) > new Date(entity.materialized_at)) {
            entityIsStale = true;
          }
        }
      } else {
        // Parent snorkel not on canvas — look up from full entity list
        const parentEntity = allSnorkelJobs?.find((j) => j.job_id === entity.parent_snorkel_job_id);
        if (parentEntity && entity.materialized_at && parentEntity.completed_at) {
          if (new Date(parentEntity.completed_at) > new Date(entity.materialized_at)) {
            entityIsStale = true;
          }
        }
      }
    }

    result.set(node.id, entityIsStale ? 'stale' : 'fresh');
  }

  // Tier 1: Rules (depend on parent index)
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
    // Any non-COMPLETED job (DRAFT, FAILED, etc.) is stale — it hasn't produced results
    if (entity.status !== 'COMPLETED') {
      result.set(node.id, 'stale');
      continue;
    }

    let entityIsStale = false;

    // Check input index (index_id can be null for draft jobs)
    const parentIdx = entity.index_id != null ? indexMap.get(entity.index_id) : undefined;
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

  // Derived indexes: inherit staleness from parent SQL index or parent Snorkel job
  for (const [, { node, entity }] of indexMap) {
    if (entity.source_type !== 'derived') continue;
    if (node.data.status === 'running' || node.data.status === 'pending') {
      result.set(node.id, 'running');
      continue;
    }

    let entityIsStale = !entity.is_materialized;

    if (entity.parent_index_id != null) {
      // Try canvas nodes first, fall back to allIndexes for cross-view lookup
      const parentIdx = indexMap.get(entity.parent_index_id);
      if (parentIdx) {
        const parentStatus = result.get(parentIdx.node.id);
        if (parentStatus === 'stale') entityIsStale = true;
        if (entity.materialized_at && parentIdx.entity.materialized_at) {
          if (new Date(parentIdx.entity.materialized_at) > new Date(entity.materialized_at)) {
            entityIsStale = true;
          }
        }
      } else {
        // Parent index not on canvas — look up from full entity list
        const parentEntity = allIndexes?.find((i) => i.index_id === entity.parent_index_id);
        if (parentEntity && entity.materialized_at && parentEntity.materialized_at) {
          if (new Date(parentEntity.materialized_at) > new Date(entity.materialized_at)) {
            entityIsStale = true;
          }
        }
      }
    }

    if (entity.parent_snorkel_job_id != null) {
      // Try canvas nodes first, fall back to allSnorkelJobs for cross-view lookup
      const parentSnorkel = snorkelMap.get(entity.parent_snorkel_job_id);
      if (parentSnorkel) {
        const parentStatus = result.get(parentSnorkel.node.id);
        if (parentStatus === 'stale') entityIsStale = true;
        if (entity.materialized_at && parentSnorkel.entity.completed_at) {
          if (new Date(parentSnorkel.entity.completed_at) > new Date(entity.materialized_at)) {
            entityIsStale = true;
          }
        }
      } else {
        // Parent snorkel not on canvas — look up from full entity list
        const parentEntity = allSnorkelJobs?.find((j) => j.job_id === entity.parent_snorkel_job_id);
        if (parentEntity && entity.materialized_at && parentEntity.completed_at) {
          if (new Date(parentEntity.completed_at) > new Date(entity.materialized_at)) {
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
    | { type: 'all' },
  nodes: PipelineNode[],
  staleness: Map<string, StaleStatus>,
): ExecutionStep[] {
  const steps: ExecutionStep[] = [];

  // Build entity lookup maps
  const indexMap = new Map<number, { node: PipelineNode; entity: Index }>();
  const ruleMap = new Map<number, { node: PipelineNode; entity: Rule }>();
  const lfMap = new Map<number, { node: PipelineNode; entity: LabelingFunction }>();
  const snorkelMap = new Map<number, { node: PipelineNode; entity: SnorkelJob }>();

  for (const n of nodes) {
    const d = n.data;
    switch (d.entityType) {
      case 'index': indexMap.set((d.entity as Index).index_id, { node: n, entity: d.entity as Index }); break;
      case 'rule': ruleMap.set((d.entity as Rule).r_id, { node: n, entity: d.entity as Rule }); break;
      case 'lf': lfMap.set((d.entity as LabelingFunction).lf_id, { node: n, entity: d.entity as LabelingFunction }); break;
      case 'snorkel': snorkelMap.set((d.entity as SnorkelJob).job_id, { node: n, entity: d.entity as SnorkelJob }); break;
    }
  }

  // Collect which indexes, rules need re-running
  const staleIndexIds = new Set<number>();
  const staleRuleIds = new Set<number>();
  let includeSnorkel: SnorkelJob | null = null;

  if (target.type === 'snorkel') {
    const job = target.entity;
    includeSnorkel = job;

    // Collect upstream: index, rules (via LFs)
    const idx = job.index_id != null ? indexMap.get(job.index_id) : undefined;
    if (idx && job.index_id != null && staleness.get(idx.node.id) === 'stale') {
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
  } else {
    // 'all' — collect everything stale
    for (const [entId, { node }] of indexMap) {
      if (staleness.get(node.id) === 'stale') staleIndexIds.add(entId);
    }
    for (const [entId, { node }] of ruleMap) {
      if (staleness.get(node.id) === 'stale') staleRuleIds.add(entId);
    }
    for (const [, { node, entity: snorkelEntity }] of snorkelMap) {
      if (staleness.get(node.id) === 'stale' && !includeSnorkel) {
        includeSnorkel = snorkelEntity;
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

  // Tier 1: Rules
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
