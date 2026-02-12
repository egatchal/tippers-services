import { useMemo } from 'react';
import { useForm } from 'react-hook-form';
import { X, Check, Loader2, Minus, AlertTriangle, Play } from 'lucide-react';
import type { SnorkelJob, ClassifierJob } from '../../types/entities';
import type { ExecutionStep } from '../../utils/staleness';
import { buildExecutionPlan, computeStaleness } from '../../utils/staleness';
import { usePipelineStore } from '../../stores/pipelineStore';
import { useConceptStore } from '../../stores/conceptStore';
import { usePipelineExecution, type SnorkelConfig, type ClassifierConfig } from '../../hooks/usePipelineExecution';

type Target =
  | { type: 'snorkel'; entity: SnorkelJob }
  | { type: 'classifier'; entity: ClassifierJob }
  | { type: 'all' };

interface Props {
  target: Target;
  onClose: () => void;
}

export default function RunPipelineModal({ target, onClose }: Props) {
  const nodes = usePipelineStore((s) => s.nodes);
  const cId = useConceptStore((s) => s.activeConcept?.c_id);

  const staleness = useMemo(() => computeStaleness(nodes), [nodes]);
  const plan = useMemo(() => buildExecutionPlan(target, nodes, staleness), [target, nodes, staleness]);

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

  const tierLabels: Record<number, string> = {
    0: 'Indexes',
    1: 'Rules & Features',
    2: 'Snorkel',
    3: 'Classifier',
  };

  // Determine which tiers to show based on target
  const maxTier = target.type === 'snorkel' ? 2 : target.type === 'classifier' ? 3 : 3;
  const allTierKeys = Array.from({ length: maxTier + 1 }, (_, i) => i);

  // Snorkel config form
  const snorkelStep = plan.find((s) => s.entityType === 'snorkel');
  const classifierStep = plan.find((s) => s.entityType === 'classifier');

  // Pre-fill from entity if available
  const snorkelDefaults = useMemo(() => {
    if (target.type === 'snorkel') {
      const e = target.entity;
      return { epochs: e.config.epochs ?? 100, lr: e.config.lr ?? 0.01, output_type: e.output_type || 'softmax' };
    }
    // For 'all', find most recent completed snorkel job
    const snorkelNodes = nodes.filter((n) => n.data.entityType === 'snorkel');
    const completed = snorkelNodes
      .map((n) => n.data.entity as SnorkelJob)
      .filter((j) => j.status === 'COMPLETED')
      .sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());
    if (completed.length > 0) {
      return { epochs: completed[0].config.epochs ?? 100, lr: completed[0].config.lr ?? 0.01, output_type: completed[0].output_type || 'softmax' };
    }
    return { epochs: 100, lr: 0.01, output_type: 'softmax' };
  }, [target, nodes]);

  const classifierDefaults = useMemo(() => {
    const extractConfig = (c: Record<string, unknown>) => ({
      threshold_method: String(c.threshold_method ?? 'max_confidence'),
      threshold_value: Number(c.threshold_value ?? 0.7),
      test_size: Number(c.test_size ?? 0.2),
      random_state: Number(c.random_state ?? 42),
      n_estimators: Number(c.n_estimators ?? 100),
      max_depth: c.max_depth != null ? String(c.max_depth) : '',
    });
    if (target.type === 'classifier') {
      return extractConfig(target.entity.config as Record<string, unknown>);
    }
    const classifierNodes = nodes.filter((n) => n.data.entityType === 'classifier');
    const completed = classifierNodes
      .map((n) => n.data.entity as ClassifierJob)
      .filter((j) => j.status === 'COMPLETED')
      .sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime());
    if (completed.length > 0) {
      return extractConfig(completed[0].config as Record<string, unknown>);
    }
    return { threshold_method: 'max_confidence', threshold_value: 0.7, test_size: 0.2, random_state: 42, n_estimators: 100, max_depth: '' };
  }, [target, nodes]);

  const snorkelForm = useForm({ defaultValues: snorkelDefaults });
  const classifierForm = useForm({ defaultValues: classifierDefaults });

  // Resolve the snorkel entity whose config we'll use (index_id, lf_ids, etc.)
  const snorkelEntity = useMemo((): SnorkelJob | null => {
    if (target.type === 'snorkel') return target.entity;
    // For 'classifier' or 'all': look up the entity from the plan step
    if (snorkelStep) {
      const n = nodes.find((nd) => nd.data.entityType === 'snorkel' && (nd.data.entity as SnorkelJob).job_id === snorkelStep.entityId);
      if (n) return n.data.entity as SnorkelJob;
    }
    // Fallback for 'classifier' target: the source snorkel job
    if (target.type === 'classifier') {
      const srcId = target.entity.snorkel_job_id;
      const n = nodes.find((nd) => nd.data.entityType === 'snorkel' && (nd.data.entity as SnorkelJob).job_id === srcId);
      if (n) return n.data.entity as SnorkelJob;
    }
    return null;
  }, [target, snorkelStep, nodes]);

  // Resolve the classifier entity whose config we'll use (feature_ids, etc.)
  const classifierEntity = useMemo((): ClassifierJob | null => {
    if (target.type === 'classifier') return target.entity;
    if (classifierStep) {
      const n = nodes.find((nd) => nd.data.entityType === 'classifier' && (nd.data.entity as ClassifierJob).job_id === classifierStep.entityId);
      if (n) return n.data.entity as ClassifierJob;
    }
    return null;
  }, [target, classifierStep, nodes]);

  const handleRun = () => {
    if (!cId) return;

    const snorkelValues = snorkelForm.getValues();
    const classifierValues = classifierForm.getValues();

    // Build snorkel config from the resolved entity + form overrides.
    // The hook creates a NEW snorkel job with these params.
    const snorkelConfig: SnorkelConfig | undefined = snorkelEntity ? {
      selectedIndex: snorkelEntity.index_id,
      selectedLFs: snorkelEntity.lf_ids,
      snorkel: {
        epochs: snorkelValues.epochs,
        lr: snorkelValues.lr,
        output_type: snorkelValues.output_type,
      },
    } : undefined;

    // Build classifier config. snorkel_job_id here is a *fallback*;
    // the hook will substitute the NEW snorkel job ID if one was created
    // during this execution.
    const classifierConfig: ClassifierConfig | undefined = classifierEntity ? {
      snorkel_job_id: classifierEntity.snorkel_job_id,
      feature_ids: classifierEntity.feature_ids,
      config: {
        threshold_method: classifierValues.threshold_method,
        threshold_value: classifierValues.threshold_value,
        test_size: classifierValues.test_size,
        random_state: classifierValues.random_state,
        n_estimators: classifierValues.n_estimators,
        max_depth: classifierValues.max_depth === '' ? null : Number(classifierValues.max_depth),
      },
    } : undefined;

    execute(cId, plan, snorkelConfig, classifierConfig);
  };

  const title = target.type === 'snorkel'
    ? 'Re-run to Snorkel'
    : target.type === 'classifier'
    ? 'Re-run to Classifier'
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
                {tierKey === 2 && snorkelStep && !isRunning && !isDone && (
                  <div className="ml-6 mt-2 p-3 bg-gray-50 rounded border border-gray-200 space-y-2">
                    <p className="text-[10px] font-semibold text-gray-500 uppercase">Snorkel Config</p>
                    <div className="grid grid-cols-3 gap-2">
                      <div>
                        <label className="block text-[10px] text-gray-500 mb-0.5">Epochs</label>
                        <input type="number" {...snorkelForm.register('epochs', { valueAsNumber: true })} className="w-full px-2 py-1 text-xs border border-gray-300 rounded" />
                      </div>
                      <div>
                        <label className="block text-[10px] text-gray-500 mb-0.5">LR</label>
                        <input type="number" step="0.001" {...snorkelForm.register('lr', { valueAsNumber: true })} className="w-full px-2 py-1 text-xs border border-gray-300 rounded" />
                      </div>
                      <div>
                        <label className="block text-[10px] text-gray-500 mb-0.5">Output</label>
                        <select {...snorkelForm.register('output_type')} className="w-full px-2 py-1 text-xs border border-gray-300 rounded">
                          <option value="softmax">Softmax</option>
                          <option value="hard_labels">Hard Labels</option>
                        </select>
                      </div>
                    </div>
                  </div>
                )}

                {/* Inline config for classifier tier */}
                {tierKey === 3 && classifierStep && !isRunning && !isDone && (
                  <div className="ml-6 mt-2 p-3 bg-gray-50 rounded border border-gray-200 space-y-2">
                    <p className="text-[10px] font-semibold text-gray-500 uppercase">Classifier Config</p>
                    <div className="grid grid-cols-2 gap-2">
                      <div>
                        <label className="block text-[10px] text-gray-500 mb-0.5">Threshold Method</label>
                        <select {...classifierForm.register('threshold_method')} className="w-full px-2 py-1 text-xs border border-gray-300 rounded">
                          <option value="max_confidence">Max Confidence</option>
                          <option value="entropy">Entropy</option>
                        </select>
                      </div>
                      <div>
                        <label className="block text-[10px] text-gray-500 mb-0.5">Threshold</label>
                        <input type="number" step="0.05" {...classifierForm.register('threshold_value', { valueAsNumber: true })} className="w-full px-2 py-1 text-xs border border-gray-300 rounded" />
                      </div>
                      <div>
                        <label className="block text-[10px] text-gray-500 mb-0.5">Test Size</label>
                        <input type="number" step="0.05" {...classifierForm.register('test_size', { valueAsNumber: true })} className="w-full px-2 py-1 text-xs border border-gray-300 rounded" />
                      </div>
                      <div>
                        <label className="block text-[10px] text-gray-500 mb-0.5">Random State</label>
                        <input type="number" {...classifierForm.register('random_state', { valueAsNumber: true })} className="w-full px-2 py-1 text-xs border border-gray-300 rounded" />
                      </div>
                      <div>
                        <label className="block text-[10px] text-gray-500 mb-0.5">Estimators</label>
                        <input type="number" {...classifierForm.register('n_estimators', { valueAsNumber: true })} className="w-full px-2 py-1 text-xs border border-gray-300 rounded" />
                      </div>
                      <div>
                        <label className="block text-[10px] text-gray-500 mb-0.5">Max Depth</label>
                        <input type="number" placeholder="None" {...classifierForm.register('max_depth')} className="w-full px-2 py-1 text-xs border border-gray-300 rounded" />
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
