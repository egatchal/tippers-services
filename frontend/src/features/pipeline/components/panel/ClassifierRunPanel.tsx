import { useState } from 'react';
import { useForm } from 'react-hook-form';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import toast from 'react-hot-toast';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell } from 'recharts';
import { RefreshCw } from 'lucide-react';
import type { ClassifierJob } from '../../types/entities';
import { runClassifier, getClassifierResults, deleteClassifierJob, cancelClassifierJob } from '../../api/classifiers';
import { useConceptStore } from '../../stores/conceptStore';
import { usePipelineStore } from '../../stores/pipelineStore';
import StatusBadge from '../../../../shared/components/StatusBadge';
import { getStatusFromEntity } from '../../../../shared/utils/statusColors';
import ConfirmDialog from '../../../../shared/components/ConfirmDialog';
import RunPipelineModal from '../modals/RunPipelineModal';

export default function ClassifierRunPanel({ entity }: { entity: ClassifierJob }) {
  const cId = useConceptStore((s) => s.activeConcept?.c_id);
  const queryClient = useQueryClient();
  const closePanel = usePipelineStore((s) => s.closePanel);
  const [showDelete, setShowDelete] = useState(false);
  const [showRerun, setShowRerun] = useState(false);
  const isCompleted = entity.status === 'COMPLETED';
  const isRunning = entity.status === 'RUNNING' || entity.status === 'PENDING';

  const { data: results } = useQuery({
    queryKey: ['classifierResults', cId, entity.job_id],
    queryFn: () => getClassifierResults(cId!, entity.job_id),
    enabled: isCompleted && !!cId,
  });

  const cancelMutation = useMutation({
    mutationFn: () => cancelClassifierJob(cId!, entity.job_id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['classifierJobs', cId] });
      toast.success('Job cancelled');
    },
    onError: () => toast.error('Failed to cancel'),
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteClassifierJob(cId!, entity.job_id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['classifierJobs', cId] });
      toast.success('Job deleted');
      closePanel();
    },
    onError: () => toast.error('Failed to delete'),
  });

  // Build model scores chart data (sorted by accuracy descending)
  const modelChartData = results?.model_scores
    ? [...results.model_scores]
        .sort((a, b) => Number(b.Accuracy ?? 0) - Number(a.Accuracy ?? 0))
        .slice(0, 15)
        .map((row) => ({
          name: String(row.Model ?? 'Unknown'),
          accuracy: Number(row.Accuracy ?? 0),
        }))
    : [];

  const bestModel = modelChartData.length > 0 ? modelChartData[0] : null;

  // Filtering stats
  const fs = results?.filtering_stats as Record<string, unknown> | undefined;
  const samplesBefore = fs?.samples_before_filter as number | undefined;
  const samplesAfter = fs?.samples_after_filter as number | undefined;
  const retentionRate = fs?.filter_retention_rate as number | undefined;
  const confAfter = fs?.confidence_after_filter as number | undefined;

  return (
    <div className="p-4 space-y-4">
      <div className="flex items-center justify-between">
        <StatusBadge status={getStatusFromEntity(entity)} />
        <span className="text-xs text-gray-500">#{entity.job_id}</span>
      </div>

      <div className="grid grid-cols-2 gap-2 text-xs">
        <div className="bg-gray-50 rounded p-2">
          <span className="text-gray-500">Snorkel Job:</span> #{entity.snorkel_job_id}
        </div>
        <div className="bg-gray-50 rounded p-2">
          <span className="text-gray-500">Features:</span> {entity.feature_ids.length}
        </div>
      </div>

      {entity.error_message && (
        <div className="p-3 bg-red-50 border border-red-200 rounded text-xs text-red-700">
          {entity.error_message}
        </div>
      )}

      {results && isCompleted && (
        <div className="space-y-4">
          {/* Best Model Highlight */}
          {bestModel && (
            <div className="bg-emerald-50 border border-emerald-300 rounded-lg p-3">
              <p className="text-[10px] text-emerald-600 uppercase font-semibold">Best Model</p>
              <p className="text-base font-bold text-emerald-800">{bestModel.name}</p>
              <p className="text-sm text-emerald-700">{(bestModel.accuracy * 100).toFixed(2)}% accuracy</p>
            </div>
          )}

          {/* Filtering Stats Cards */}
          {fs && (
            <div className="grid grid-cols-4 gap-2">
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-2 text-center">
                <p className="text-lg font-bold text-blue-700">{samplesBefore ?? '-'}</p>
                <p className="text-[10px] text-blue-600 uppercase font-medium">Before</p>
              </div>
              <div className="bg-emerald-50 border border-emerald-200 rounded-lg p-2 text-center">
                <p className="text-lg font-bold text-emerald-700">{samplesAfter ?? '-'}</p>
                <p className="text-[10px] text-emerald-600 uppercase font-medium">After</p>
              </div>
              <div className="bg-amber-50 border border-amber-200 rounded-lg p-2 text-center">
                <p className="text-lg font-bold text-amber-700">{retentionRate != null ? (retentionRate * 100).toFixed(1) + '%' : '-'}</p>
                <p className="text-[10px] text-amber-600 uppercase font-medium">Retained</p>
              </div>
              <div className="bg-purple-50 border border-purple-200 rounded-lg p-2 text-center">
                <p className="text-lg font-bold text-purple-700">{confAfter != null ? (confAfter * 100).toFixed(1) + '%' : '-'}</p>
                <p className="text-[10px] text-purple-600 uppercase font-medium">Confidence</p>
              </div>
            </div>
          )}

          {/* Model Accuracy Chart */}
          {modelChartData.length > 0 && (
            <div>
              <h5 className="text-[11px] font-semibold text-gray-500 mb-2 uppercase">
                Model Accuracy ({results.num_models_trained} models)
              </h5>
              <ResponsiveContainer width="100%" height={Math.max(150, modelChartData.length * 24)}>
                <BarChart data={modelChartData} layout="vertical" margin={{ top: 5, right: 5, bottom: 5, left: 100 }}>
                  <XAxis type="number" tick={{ fontSize: 10 }} domain={[0, 1]} tickFormatter={(v) => (Number(v) * 100).toFixed(0) + '%'} />
                  <YAxis type="category" dataKey="name" tick={{ fontSize: 9 }} width={95} />
                  <Tooltip contentStyle={{ fontSize: 11 }} formatter={(v) => (Number(v) * 100).toFixed(2) + '%'} />
                  <Bar dataKey="accuracy" radius={[0, 3, 3, 0]}>
                    {modelChartData.map((_, index) => (
                      <Cell key={index} fill={index === 0 ? '#10b981' : '#93c5fd'} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}
        </div>
      )}

      <div className="flex gap-2 pt-2">
        {isRunning && (
          <button onClick={() => cancelMutation.mutate()} disabled={cancelMutation.isPending} className="px-4 py-2 text-sm bg-amber-600 text-white rounded-md hover:bg-amber-700 disabled:opacity-50">
            Cancel
          </button>
        )}
        {!isRunning && (
          <button onClick={() => setShowRerun(true)} className="flex items-center gap-1.5 px-4 py-2 text-sm border border-emerald-300 text-emerald-600 rounded-md hover:bg-emerald-50">
            <RefreshCw className="w-3.5 h-3.5" />
            Re-run
          </button>
        )}
        <button onClick={() => setShowDelete(true)} className="px-4 py-2 text-sm border border-red-300 text-red-600 rounded-md hover:bg-red-50 ml-auto">Delete</button>
      </div>

      <ConfirmDialog open={showDelete} title="Delete Classifier Job" message={`Delete job #${entity.job_id}?`} onConfirm={() => deleteMutation.mutate()} onCancel={() => setShowDelete(false)} />
      {showRerun && <RunPipelineModal target={{ type: 'classifier', entity }} onClose={() => setShowRerun(false)} />}
    </div>
  );
}

export function NewClassifierRunForm({ onClose }: { onClose: () => void }) {
  const cId = useConceptStore((s) => s.activeConcept?.c_id);
  const queryClient = useQueryClient();
  const nodes = usePipelineStore((s) => s.nodes);

  const snorkelJobs = nodes.filter((n) => n.data.entityType === 'snorkel' && n.data.status === 'materialized');
  const features = nodes.filter((n) => n.data.entityType === 'feature' && n.data.status === 'materialized');

  const [selectedSnorkelJobId, setSelectedSnorkelJobId] = useState<number | null>(null);
  const [selectedFeatureIds, setSelectedFeatureIds] = useState<number[]>([]);

  const { register, handleSubmit } = useForm({
    defaultValues: {
      threshold_method: 'max_confidence',
      threshold_value: 0.7,
      test_size: 0.2,
      random_state: 42,
      n_estimators: 100,
      max_depth: '' as string | number,
    },
  });

  const runMutation = useMutation({
    mutationFn: (values: { threshold_method: string; threshold_value: number; test_size: number; random_state: number; n_estimators: number; max_depth: string | number }) =>
      runClassifier(cId!, {
        snorkel_job_id: selectedSnorkelJobId!,
        feature_ids: selectedFeatureIds,
        config: {
          threshold_method: values.threshold_method,
          threshold_value: values.threshold_value,
          test_size: values.test_size,
          random_state: values.random_state,
          n_estimators: values.n_estimators,
          max_depth: values.max_depth === '' ? null : Number(values.max_depth),
        },
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['classifierJobs', cId] });
      toast.success('Classifier run started');
      onClose();
    },
    onError: () => toast.error('Failed to start classifier'),
  });

  return (
    <form onSubmit={handleSubmit((v) => runMutation.mutate(v))} className="space-y-4 p-4">
      <div>
        <label className="block text-xs font-medium text-gray-700 mb-1">Snorkel Job (completed)</label>
        <select value={selectedSnorkelJobId ?? ''} onChange={(e) => setSelectedSnorkelJobId(Number(e.target.value))} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md">
          <option value="">Select completed Snorkel job...</option>
          {snorkelJobs.map((n) => (
            <option key={n.id} value={(n.data.entity as { job_id: number }).job_id}>{n.data.label}</option>
          ))}
        </select>
      </div>

      <div>
        <label className="block text-xs font-medium text-gray-700 mb-1">Materialized Features</label>
        <div className="space-y-1 max-h-32 overflow-y-auto border border-gray-200 rounded p-2">
          {features.length === 0 && <p className="text-xs text-gray-400">No materialized features</p>}
          {features.map((n) => {
            const fId = (n.data.entity as { feature_id: number }).feature_id;
            return (
              <label key={n.id} className="flex items-center gap-2 text-sm">
                <input type="checkbox" checked={selectedFeatureIds.includes(fId)} onChange={(e) => {
                  setSelectedFeatureIds(e.target.checked ? [...selectedFeatureIds, fId] : selectedFeatureIds.filter((id) => id !== fId));
                }} />
                {n.data.label}
              </label>
            );
          })}
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3">
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Threshold Method</label>
          <select {...register('threshold_method')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md">
            <option value="max_confidence">Max Confidence</option>
            <option value="entropy">Entropy</option>
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Threshold</label>
          <input type="number" step="0.05" {...register('threshold_value', { valueAsNumber: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md" />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Test Size</label>
          <input type="number" step="0.05" {...register('test_size', { valueAsNumber: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md" />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Random State</label>
          <input type="number" {...register('random_state', { valueAsNumber: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md" />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Estimators</label>
          <input type="number" {...register('n_estimators', { valueAsNumber: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md" />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Max Depth</label>
          <input type="number" placeholder="None" {...register('max_depth')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md" />
        </div>
      </div>

      <div className="flex justify-end gap-3 pt-2">
        <button type="button" onClick={onClose} className="px-4 py-2 text-sm border border-gray-300 rounded-md hover:bg-gray-50">Cancel</button>
        <button type="submit" disabled={!selectedSnorkelJobId || selectedFeatureIds.length === 0 || runMutation.isPending} className="px-4 py-2 text-sm bg-pink-600 text-white rounded-md hover:bg-pink-700 disabled:opacity-50">
          Run Classifier
        </button>
      </div>
    </form>
  );
}
