import { useState, useCallback, useRef } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import toast from 'react-hot-toast';
import type { ExecutionStep } from '../utils/staleness';
import type { Index, Rule, Feature, SnorkelJob, ClassifierJob } from '../types/entities';
import { materializeIndex } from '../api/indexes';
import { materializeRule } from '../api/rules';
import { materializeFeature } from '../api/features';
import { runSnorkel } from '../api/snorkel';
import { runClassifier } from '../api/classifiers';
import { POLL_INTERVAL } from '../../../shared/utils/constants';

export interface SnorkelConfig {
  selectedIndex: number;
  selectedLFs: number[];
  snorkel: { epochs?: number; lr?: number; output_type?: string };
}

export interface ClassifierConfig {
  snorkel_job_id: number;
  feature_ids: number[];
  config: {
    threshold_method?: string;
    threshold_value?: number;
    test_size?: number;
    random_state?: number;
    n_estimators?: number;
    max_depth?: number | null;
  };
}

export type ExecutionStatus = 'idle' | 'running' | 'completed' | 'failed';

interface CompletionCheck {
  type: 'index' | 'rule' | 'feature' | 'snorkel' | 'classifier';
  entityId: number;
}

export function usePipelineExecution() {
  const queryClient = useQueryClient();
  const [status, setStatus] = useState<ExecutionStatus>('idle');
  const [currentTier, setCurrentTier] = useState(-1);
  const [completedSteps, setCompletedSteps] = useState<ExecutionStep[]>([]);
  const [error, setError] = useState<string | null>(null);
  const cancelledRef = useRef(false);

  const refetchAll = useCallback(async (cId: number) => {
    await Promise.all([
      queryClient.refetchQueries({ queryKey: ['indexes', cId] }),
      queryClient.refetchQueries({ queryKey: ['rules', cId] }),
      queryClient.refetchQueries({ queryKey: ['features', cId] }),
      queryClient.refetchQueries({ queryKey: ['snorkelJobs', cId] }),
      queryClient.refetchQueries({ queryKey: ['classifierJobs', cId] }),
    ]);
  }, [queryClient]);

  /**
   * Polls with a proper async while-loop (not setInterval) until every check
   * in the list reports done. Checks specific entity IDs, not "any running".
   */
  const waitForTierCompletion = useCallback(async (
    cId: number,
    checks: CompletionCheck[],
  ): Promise<void> => {
    if (checks.length === 0) return;

    let attempts = 0;
    const maxAttempts = 150; // ~10 min at 4 s

    while (attempts < maxAttempts) {
      if (cancelledRef.current) return;

      await new Promise((r) => setTimeout(r, POLL_INTERVAL));
      attempts++;

      // Refetch ALL queries and wait for the network round-trips to finish
      // so getQueryData below reads fresh data.
      await Promise.all([
        queryClient.refetchQueries({ queryKey: ['indexes', cId] }),
        queryClient.refetchQueries({ queryKey: ['rules', cId] }),
        queryClient.refetchQueries({ queryKey: ['features', cId] }),
        queryClient.refetchQueries({ queryKey: ['snorkelJobs', cId] }),
        queryClient.refetchQueries({ queryKey: ['classifierJobs', cId] }),
      ]);

      let allDone = true;

      for (const check of checks) {
        switch (check.type) {
          case 'index': {
            const list = queryClient.getQueryData<Index[]>(['indexes', cId]);
            const entity = list?.find((i) => i.index_id === check.entityId);
            if (!entity?.is_materialized) allDone = false;
            break;
          }
          case 'rule': {
            const list = queryClient.getQueryData<Rule[]>(['rules', cId]);
            const entity = list?.find((r) => r.r_id === check.entityId);
            if (!entity?.is_materialized) allDone = false;
            break;
          }
          case 'feature': {
            const list = queryClient.getQueryData<Feature[]>(['features', cId]);
            const entity = list?.find((f) => f.feature_id === check.entityId);
            if (!entity?.is_materialized) allDone = false;
            break;
          }
          case 'snorkel': {
            const list = queryClient.getQueryData<SnorkelJob[]>(['snorkelJobs', cId]);
            const job = list?.find((j) => j.job_id === check.entityId);
            if (!job || job.status === 'RUNNING' || job.status === 'PENDING') {
              allDone = false;
            } else if (job.status === 'FAILED') {
              throw new Error(job.error_message || `Snorkel job #${check.entityId} failed`);
            }
            break;
          }
          case 'classifier': {
            const list = queryClient.getQueryData<ClassifierJob[]>(['classifierJobs', cId]);
            const job = list?.find((j) => j.job_id === check.entityId);
            if (!job || job.status === 'RUNNING' || job.status === 'PENDING') {
              allDone = false;
            } else if (job.status === 'FAILED') {
              throw new Error(job.error_message || `Classifier job #${check.entityId} failed`);
            }
            break;
          }
        }
      }

      if (allDone) return;
    }

    throw new Error('Tier execution timed out');
  }, [queryClient]);

  const execute = useCallback(async (
    cId: number,
    plan: ExecutionStep[],
    snorkelConfig?: SnorkelConfig,
    classifierConfig?: ClassifierConfig,
  ) => {
    if (plan.length === 0) {
      toast.success('Everything is up to date');
      return;
    }

    cancelledRef.current = false;
    setStatus('running');
    setError(null);
    setCompletedSteps([]);

    // Group steps by tier
    const tiers = new Map<number, ExecutionStep[]>();
    for (const step of plan) {
      const group = tiers.get(step.tier) ?? [];
      group.push(step);
      tiers.set(step.tier, group);
    }
    const sortedTierKeys = [...tiers.keys()].sort((a, b) => a - b);

    // Track newly-created job IDs so downstream tiers can reference them
    let newSnorkelJobId: number | null = null;

    try {
      for (const tierKey of sortedTierKeys) {
        if (cancelledRef.current) break;

        setCurrentTier(tierKey);
        const tierSteps = tiers.get(tierKey)!;

        // Fire all steps in this tier in parallel.
        // Each promise returns the CompletionCheck we need to poll for.
        const results = await Promise.all(
          tierSteps.map(async (step): Promise<{ step: ExecutionStep; check: CompletionCheck | null }> => {
            let check: CompletionCheck | null = null;

            if (step.action === 'materialize') {
              switch (step.entityType) {
                case 'index':
                  await materializeIndex(cId, step.entityId);
                  check = { type: 'index', entityId: step.entityId };
                  break;
                case 'rule':
                  await materializeRule(cId, step.entityId);
                  check = { type: 'rule', entityId: step.entityId };
                  break;
                case 'feature':
                  await materializeFeature(cId, step.entityId);
                  check = { type: 'feature', entityId: step.entityId };
                  break;
              }
            } else if (step.action === 'run') {
              if (step.entityType === 'snorkel' && snorkelConfig) {
                const job = await runSnorkel(cId, snorkelConfig);
                newSnorkelJobId = job.job_id;
                check = { type: 'snorkel', entityId: job.job_id };
              } else if (step.entityType === 'classifier' && classifierConfig) {
                // Substitute the NEW snorkel job ID if one was created earlier
                const finalConfig = newSnorkelJobId
                  ? { ...classifierConfig, snorkel_job_id: newSnorkelJobId }
                  : classifierConfig;
                const job = await runClassifier(cId, finalConfig);
                check = { type: 'classifier', entityId: job.job_id };
              }
            }

            return { step, check };
          }),
        );

        // Collect the concrete checks for this tier
        const checks = results
          .map((r) => r.check)
          .filter((c): c is CompletionCheck => c !== null);

        // Block here until every job/materialization in this tier finishes
        await waitForTierCompletion(cId, checks);

        if (cancelledRef.current) break;

        setCompletedSteps((prev) => [...prev, ...tierSteps]);

        // Final refetch so the UI reflects the completed state before moving on
        await refetchAll(cId);
      }

      if (!cancelledRef.current) {
        setStatus('completed');
        toast.success('Pipeline execution completed');
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Pipeline execution failed';
      setError(msg);
      setStatus('failed');
      toast.error(msg);
      await refetchAll(cId);
    }
  }, [refetchAll, waitForTierCompletion]);

  const cancel = useCallback(() => {
    cancelledRef.current = true;
    setStatus('idle');
    toast('Pipeline execution cancelled');
  }, []);

  const reset = useCallback(() => {
    cancelledRef.current = false;
    setStatus('idle');
    setCurrentTier(-1);
    setCompletedSteps([]);
    setError(null);
  }, []);

  return {
    execute,
    status,
    currentTier,
    completedSteps,
    error,
    cancel,
    reset,
  };
}
