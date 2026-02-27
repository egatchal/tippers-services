import { useMemo } from 'react';
import { useForm } from 'react-hook-form';
import { X, Check, Loader2, Minus, AlertTriangle, Play } from 'lucide-react';
import type { SnorkelJob, ConceptValue } from '../../types/entities';
import type { ExecutionStep } from '../../utils/staleness';
import { buildExecutionPlan, buildExecutionPlanFromEntities, computeStaleness } from '../../utils/staleness';
import { usePipelineStore } from '../../stores/pipelineStore';
import { useConceptStore } from '../../stores/conceptStore';
import { usePipelineExecution } from '../../hooks/usePipelineExecution';

type Target =
  | { type: 'snorkel'; entity: SnorkelJob }
  | { type: 'cv'; cv: ConceptValue }
  | { type: 'all' };

interface Props {
  target: Target;
  onClose: () => void;
}

export default function RunPipelineModal({ target, onClose }: Props) {
  const nodes = usePipelineStore((s) => s.nodes);
  const cId = useConceptStore((s) => s.activeConcept?.c_id);
  const allEntities = useConceptStore((s) => s.allEntities);
  const conceptValues = useConceptStore((s) => s.conceptValues);

  const staleness = useMemo(
    () => computeStaleness(nodes, allEntities?.indexes, allEntities?.snorkelJobs),
    [nodes, allEntities],
  );

  const plan = useMemo(() => {
    // For single-snorkel targets, use the canvas-node-based plan (pipeline view)
    if (target.type === 'snorkel') {
      return buildExecutionPlan(target, nodes, staleness);
    }
    // For 'all' and 'cv' targets, use entity-based planning (tree view / per-CV)
    if (!allEntities) return [];
    const cvFilter = target.type === 'cv' ? target.cv.cv_id : undefined;
    return buildExecutionPlanFromEntities(allEntities, conceptValues, cvFilter);
  }, [target, nodes, staleness, allEntities, conceptValues]);

  const { execute, status, currentTier, completedSteps, error, cancel, reset } = usePipelineExecution();
  const isRunning = status === 'running';
  const isDone = status === 'completed' || status === 'failed';

  // Group plan by tier for display
  const tiers = useMemo(() => {
    const map = new Map<number, ExecutionStep[]>();
    for (const step of plan) {
      const group = map.get(step.tier) ?? [];
      group.push(step);
      map.set(step.tier, group);
    }
    return map;
  }, [plan]);

  const tierLabels = useMemo(() => {
    const labels: Record<number, string> = {};
    for (const step of plan) {
      if (!labels[step.tier]) {
        const entityTier = step.tier % 3;
        const entityLabel = ['Indexes', 'Rules', 'Snorkel'][entityTier];
        labels[step.tier] = step.cvName
          ? `${step.cvName} — ${entityLabel}`
          : entityLabel;
      }
    }
    // For snorkel-targeted or empty plans, always include the base 3 tiers
    if (target.type === 'snorkel' || plan.length === 0) {
      if (!labels[0]) labels[0] = 'Indexes';
      if (!labels[1]) labels[1] = 'Rules';
      if (!labels[2]) labels[2] = 'Snorkel';
    }
    return labels;
  }, [plan, target]);

  // Determine which tiers to show
  const allTierKeys = useMemo(() => {
    const keys = new Set(plan.map((s) => s.tier));
    // For snorkel-targeted plans, always show all 3 base tiers
    if (target.type === 'snorkel') {
      keys.add(0);
      keys.add(1);
      keys.add(2);
    }
    return [...keys].sort((a, b) => a - b);
  }, [plan, target]);

  // Snorkel config form
  const snorkelStep = plan.find((s) => s.entityType === 'snorkel');

  // Pre-fill from entity if available
  const snorkelDefaults = useMemo(() => {
    if (target.type === 'snorkel') {
      const e = target.entity;
      return { epochs: e.config.epochs ?? 100, lr: e.config.lr ?? 0.01 };
    }
    // For 'all' or 'cv', find most recent completed snorkel job from allEntities or canvas nodes
    const allJobs = allEntities?.snorkelJobs ?? [];
    const completed = allJobs
      .filter((j) => j.status === 'COMPLETED')
      .sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());
    if (completed.length > 0) {
      return { epochs: completed[0].config.epochs ?? 100, lr: completed[0].config.lr ?? 0.01 };
    }
    return { epochs: 100, lr: 0.01 };
  }, [target, allEntities]);

  const snorkelForm = useForm({ defaultValues: snorkelDefaults });

  const handleRun = () => {
    if (!cId) return;
    const snorkelValues = snorkelForm.getValues();
    // Pass epoch/lr overrides — the hook calls executeSnorkelJob(job_id) on the existing job
    execute(cId, plan, snorkelStep ? { epochs: snorkelValues.epochs, lr: snorkelValues.lr } : undefined);
  };

  const selectedCV = useConceptStore((s) => s.selectedCV);

  const title = target.type === 'snorkel'
    ? `Re-run to Snorkel${selectedCV ? ` (${selectedCV.name})` : ''}`
    : target.type === 'cv'
    ? `Re-run Pipeline — ${target.cv.name}`
    : selectedCV
    ? `Re-run Pipeline — ${selectedCV.name}`
    : 'Re-run Pipeline';

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-lg shadow-xl w-[520px] max-h-[80vh] overflow-y-auto">
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200">
          <h3 className="text-sm font-semibold">{title}</h3>
          <button onClick={onClose} className="p-1 rounded hover:bg-gray-100">
            <X className="w-4 h-4" />
          </button>
        </div>

        <div className="p-4 space-y-4">
          {plan.length === 0 && !isRunning && !isDone && (
            <div className="flex items-center gap-2 p-3 bg-green-50 border border-green-200 rounded text-sm text-green-700">
              <Check className="w-4 h-4" />
              Everything is up to date
            </div>
          )}

          {allTierKeys.map((tierKey) => {
            const tierSteps = tiers.get(tierKey);
            const isCurrentTier = tierKey === currentTier;
            const tierDone = completedSteps.some((s) => s.tier === tierKey);
            const hasFutureSteps = tierSteps && tierSteps.length > 0;

            return (
              <div key={tierKey} className="space-y-1">
                <div className="flex items-center gap-2">
                  <TierIcon tierKey={tierKey} currentTier={currentTier} tierDone={tierDone} isRunning={isRunning} hasFutureSteps={!!hasFutureSteps} />
                  <h4 className={`text-xs font-semibold uppercase ${isCurrentTier && isRunning ? 'text-blue-700' : 'text-gray-500'}`}>
                    {tierLabels[tierKey] ?? `Tier ${tierKey}`}
                  </h4>
                </div>

                {hasFutureSteps ? (
                  <div className="ml-6 space-y-1">
                    {tierSteps!.map((step, i) => (
                      <div key={i} className="flex items-center gap-2 text-sm">
                        <StepIcon step={step} completedSteps={completedSteps} currentTier={currentTier} isRunning={isRunning} />
                        <span className={completedSteps.includes(step) ? 'text-green-700' : 'text-gray-700'}>
                          {step.label}
                        </span>
                        {!isRunning && !isDone && (
                          <span className="ml-auto text-[10px] px-1.5 py-0.5 rounded bg-orange-100 text-orange-700 font-medium">
                            stale
                          </span>
                        )}
                      </div>
                    ))}
                  </div>
                ) : (
                  <div className="ml-6 text-xs text-green-600">Up to date</div>
                )}

                {/* Inline config for snorkel tier */}
                {tierKey % 3 === 2 && snorkelStep && !isRunning && !isDone && (
                  <div className="ml-6 mt-2 p-3 bg-gray-50 rounded border border-gray-200 space-y-2">
                    <p className="text-[10px] font-semibold text-gray-500 uppercase">Snorkel Config</p>
                    <div className="grid grid-cols-2 gap-2">
                      <div>
                        <label className="block text-[10px] text-gray-500 mb-0.5">Epochs</label>
                        <input type="number" {...snorkelForm.register('epochs', { valueAsNumber: true })} className="w-full px-2 py-1 text-xs border border-gray-300 rounded" />
                      </div>
                      <div>
                        <label className="block text-[10px] text-gray-500 mb-0.5">LR</label>
                        <input type="number" step="0.001" {...snorkelForm.register('lr', { valueAsNumber: true })} className="w-full px-2 py-1 text-xs border border-gray-300 rounded" />
                      </div>
                    </div>
                  </div>
                )}
              </div>
            );
          })}

          {error && (
            <div className="flex items-center gap-2 p-3 bg-red-50 border border-red-200 rounded text-sm text-red-700">
              <AlertTriangle className="w-4 h-4" />
              {error}
            </div>
          )}

          {isDone && status === 'completed' && (
            <div className="flex items-center gap-2 p-3 bg-green-50 border border-green-200 rounded text-sm text-green-700">
              <Check className="w-4 h-4" />
              Pipeline execution completed successfully
            </div>
          )}
        </div>

        <div className="flex justify-end gap-3 px-4 py-3 border-t border-gray-200">
          {isDone ? (
            <button onClick={() => { reset(); onClose(); }} className="px-4 py-2 text-sm bg-gray-600 text-white rounded-md hover:bg-gray-700">
              Close
            </button>
          ) : isRunning ? (
            <button onClick={cancel} className="px-4 py-2 text-sm bg-amber-600 text-white rounded-md hover:bg-amber-700">
              Cancel
            </button>
          ) : (
            <>
              <button onClick={onClose} className="px-4 py-2 text-sm border border-gray-300 rounded-md hover:bg-gray-50">
                Cancel
              </button>
              <button
                onClick={handleRun}
                disabled={plan.length === 0}
                className="flex items-center gap-2 px-4 py-2 text-sm bg-emerald-600 text-white rounded-md hover:bg-emerald-700 disabled:opacity-50"
              >
                <Play className="w-3.5 h-3.5" />
                Run
              </button>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

function TierIcon({ tierKey, currentTier, tierDone, isRunning, hasFutureSteps }: {
  tierKey: number; currentTier: number; tierDone: boolean; isRunning: boolean; hasFutureSteps: boolean;
}) {
  if (tierDone) {
    return <Check className="w-4 h-4 text-green-500" />;
  }
  if (tierKey === currentTier && isRunning) {
    return <Loader2 className="w-4 h-4 text-blue-500 animate-spin" />;
  }
  if (!hasFutureSteps) {
    return <Check className="w-4 h-4 text-green-400" />;
  }
  return <Minus className="w-4 h-4 text-gray-400" />;
}

function StepIcon({ step, completedSteps, currentTier, isRunning }: {
  step: ExecutionStep; completedSteps: ExecutionStep[]; currentTier: number; isRunning: boolean;
}) {
  if (completedSteps.includes(step)) {
    return <Check className="w-3.5 h-3.5 text-green-500" />;
  }
  if (step.tier === currentTier && isRunning) {
    return <Loader2 className="w-3.5 h-3.5 text-blue-500 animate-spin" />;
  }
  return <Minus className="w-3.5 h-3.5 text-gray-300" />;
}
