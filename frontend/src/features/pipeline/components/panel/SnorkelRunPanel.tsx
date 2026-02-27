import { useState, useMemo } from 'react';
import { useForm } from 'react-hook-form';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import toast from 'react-hot-toast';
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Legend } from 'recharts';
import { RefreshCw, Check, X as XIcon } from 'lucide-react';
import type { SnorkelJob, LabelingFunction, Index } from '../../types/entities';
import { runSnorkel, getSnorkelResults, deleteSnorkelJob, cancelSnorkelJob } from '../../api/snorkel';
import { useConceptStore } from '../../stores/conceptStore';
import { usePipelineStore } from '../../stores/pipelineStore';
import StatusBadge from '../../../../shared/components/StatusBadge';
import { getStatusFromEntity } from '../../../../shared/utils/statusColors';
import ConfirmDialog from '../../../../shared/components/ConfirmDialog';
import RunPipelineModal from '../modals/RunPipelineModal';

export default function SnorkelRunPanel({ entity }: { entity: SnorkelJob }) {
  const cId = useConceptStore((s) => s.activeConcept?.c_id);
  const queryClient = useQueryClient();
  const closePanel = usePipelineStore((s) => s.closePanel);
  const nodes = usePipelineStore((s) => s.nodes);
  const [showDelete, setShowDelete] = useState(false);
  const [showLFTable, setShowLFTable] = useState(false);
  const [showCVLFMapping, setShowCVLFMapping] = useState(false);
  const [showRerun, setShowRerun] = useState(false);
  const isCompleted = entity.status === 'COMPLETED';
  const isRunning = entity.status === 'RUNNING' || entity.status === 'PENDING';

  const { data: results } = useQuery({
    queryKey: ['snorkelResults', cId, entity.job_id],
    queryFn: () => getSnorkelResults(cId!, entity.job_id),
    enabled: isCompleted && !!cId,
  });

  const cancelMutation = useMutation({
    mutationFn: () => cancelSnorkelJob(cId!, entity.job_id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['snorkelJobs', cId] });
      toast.success('Job cancelled');
    },
    onError: () => toast.error('Failed to cancel'),
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteSnorkelJob(cId!, entity.job_id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['snorkelJobs', cId] });
      toast.success('Job deleted');
      closePanel();
    },
    onError: () => toast.error('Failed to delete'),
  });

  // Build CV → LF mapping
  const cvLFMapping = useMemo(() => {
    if (!results?.cv_id_to_name) return [];
    const lfEntities = nodes
      .filter((n) => n.data.entityType === 'lf')
      .map((n) => n.data.entity as LabelingFunction);
    const lfNameSet = new Set(results.lf_summary?.map((lf) => String(lf.name)) ?? []);

    return Object.entries(results.cv_id_to_name).map(([cvIdStr, cvName]) => {
      const cvId = Number(cvIdStr);
      const matchingLFs = lfEntities
        .filter((lf) => lf.applicable_cv_ids.includes(cvId))
        .map((lf) => ({
          name: lf.name,
          is_active: lf.is_active,
          inResults: lfNameSet.has(lf.name),
        }));
      return { cvId, cvName: String(cvName), lfs: matchingLFs };
    });
  }, [results, nodes]);

  // Build chart data
  const classDistData = results
    ? (() => {
        const lmDist = results.label_matrix_class_distribution ?? {};
        const modelDist = results.model_class_distribution ?? {};
        const allLabels = [...new Set([...Object.keys(lmDist), ...Object.keys(modelDist)])];
        return allLabels.map((label) => ({
          name: label,
          'Label Matrix': lmDist[label] ?? 0,
          'Model': modelDist[label] ?? 0,
        }));
      })()
    : [];

  const lfCoverageData = results?.lf_summary
    ? results.lf_summary.map((lf) => ({
        name: String(lf.name ?? 'LF'),
        coverage: Number(lf.coverage ?? 0),
        conflicts: Number(lf.conflicts ?? 0),
        weight: Number(lf.learned_weight ?? 0),
      }))
    : [];

  return (
    <div className="p-4 space-y-4">
      <div className="flex items-center justify-between">
        <StatusBadge status={getStatusFromEntity(entity)} />
        <span className="text-xs text-gray-500">#{entity.job_id}</span>
      </div>

      <div className="grid grid-cols-2 gap-2 text-xs">
        <div className="bg-gray-50 rounded p-2">
          <span className="text-gray-500">Epochs:</span> {entity.config.epochs ?? 100}
        </div>
        <div className="bg-gray-50 rounded p-2">
          <span className="text-gray-500">LR:</span> {entity.config.lr ?? 0.01}
        </div>
        <div className="bg-gray-50 rounded p-2">
          <span className="text-gray-500">LFs:</span> {entity.lf_ids.length}
        </div>
        <div className="bg-gray-50 rounded p-2">
          <span className="text-gray-500">Output:</span> {entity.output_type}
        </div>
      </div>

      {entity.error_message && (
        <div className="p-3 bg-red-50 border border-red-200 rounded text-xs text-red-700">
          {entity.error_message}
        </div>
      )}

      {results && isCompleted && (
        <div className="space-y-4">
          {/* Overall Stats Cards */}
          {results.overall_stats && (
            <div className="grid grid-cols-4 gap-2">
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-2 text-center">
                <p className="text-lg font-bold text-blue-700">{String(results.overall_stats.n_samples ?? '-')}</p>
                <p className="text-[10px] text-blue-600 uppercase font-medium">Samples</p>
              </div>
              <div className="bg-emerald-50 border border-emerald-200 rounded-lg p-2 text-center">
                <p className="text-lg font-bold text-emerald-700">{typeof results.overall_stats.total_coverage === 'number' ? (Number(results.overall_stats.total_coverage) * 100).toFixed(1) + '%' : '-'}</p>
                <p className="text-[10px] text-emerald-600 uppercase font-medium">Coverage</p>
              </div>
              <div className="bg-amber-50 border border-amber-200 rounded-lg p-2 text-center">
                <p className="text-lg font-bold text-amber-700">{typeof results.overall_stats.mean_lf_overlaps === 'number' ? (Number(results.overall_stats.mean_lf_overlaps) * 100).toFixed(1) + '%' : '-'}</p>
                <p className="text-[10px] text-amber-600 uppercase font-medium">Overlaps</p>
              </div>
              <div className="bg-red-50 border border-red-200 rounded-lg p-2 text-center">
                <p className="text-lg font-bold text-red-700">{typeof results.overall_stats.mean_lf_conflicts === 'number' ? (Number(results.overall_stats.mean_lf_conflicts) * 100).toFixed(1) + '%' : '-'}</p>
                <p className="text-[10px] text-red-600 uppercase font-medium">Conflicts</p>
              </div>
            </div>
          )}

          {/* Class Distribution Chart */}
          {classDistData.length > 0 && (
            <div>
              <h5 className="text-[11px] font-semibold text-gray-500 mb-2 uppercase">Class Distribution</h5>
              <ResponsiveContainer width="100%" height={180}>
                <BarChart data={classDistData} margin={{ top: 5, right: 5, bottom: 5, left: 5 }}>
                  <XAxis dataKey="name" tick={{ fontSize: 10 }} />
                  <YAxis tick={{ fontSize: 10 }} />
                  <Tooltip contentStyle={{ fontSize: 11 }} />
                  <Legend wrapperStyle={{ fontSize: 10 }} />
                  <Bar dataKey="Label Matrix" fill="#93c5fd" radius={[2, 2, 0, 0]} />
                  <Bar dataKey="Model" fill="#34d399" radius={[2, 2, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* CV → LF Mapping */}
          {cvLFMapping.length > 0 && (
            <div>
              <button
                onClick={() => setShowCVLFMapping(!showCVLFMapping)}
                className="text-[11px] font-semibold text-indigo-600 hover:text-indigo-800 uppercase"
              >
                {showCVLFMapping ? 'Hide' : 'Show'} CV → LF Mapping
              </button>
              {showCVLFMapping && (
                <div className="mt-2 space-y-2">
                  {cvLFMapping.map((group) => (
                    <div key={group.cvId} className="border border-gray-200 rounded p-2">
                      <p className="text-xs font-semibold text-gray-700 uppercase">{group.cvName} <span className="text-gray-400 font-normal">(cv_id: {group.cvId})</span></p>
                      {group.lfs.length === 0 ? (
                        <p className="text-[10px] text-gray-400 mt-1">No LFs for this label</p>
                      ) : (
                        <div className="mt-1 space-y-0.5">
                          {group.lfs.map((lf) => (
                            <div key={lf.name} className="flex items-center gap-1.5 text-xs">
                              {lf.is_active ? (
                                <Check className="w-3 h-3 text-green-500" />
                              ) : (
                                <XIcon className="w-3 h-3 text-gray-400" />
                              )}
                              <span className={lf.is_active ? 'text-gray-700' : 'text-gray-400'}>
                                {lf.name}
                              </span>
                              {!lf.is_active && <span className="text-[9px] text-gray-400">(inactive)</span>}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* LF Coverage/Conflicts Chart */}
          {lfCoverageData.length > 0 && (
            <div>
              <h5 className="text-[11px] font-semibold text-gray-500 mb-2 uppercase">LF Performance</h5>
              <ResponsiveContainer width="100%" height={Math.max(120, lfCoverageData.length * 28)}>
                <BarChart data={lfCoverageData} layout="vertical" margin={{ top: 5, right: 5, bottom: 5, left: 60 }}>
                  <XAxis type="number" tick={{ fontSize: 10 }} domain={[0, 1]} />
                  <YAxis type="category" dataKey="name" tick={{ fontSize: 10 }} width={55} />
                  <Tooltip contentStyle={{ fontSize: 11 }} formatter={(v) => (Number(v) * 100).toFixed(1) + '%'} />
                  <Legend wrapperStyle={{ fontSize: 10 }} />
                  <Bar dataKey="coverage" fill="#3b82f6" radius={[0, 2, 2, 0]} name="Coverage" />
                  <Bar dataKey="conflicts" fill="#ef4444" radius={[0, 2, 2, 0]} name="Conflicts" />
                </BarChart>
              </ResponsiveContainer>
            </div>
          )}

          {/* Collapsible LF Detail Table */}
          {results.lf_summary?.length > 0 && (
            <div>
              <button
                onClick={() => setShowLFTable(!showLFTable)}
                className="text-[11px] font-semibold text-blue-600 hover:text-blue-800 uppercase"
              >
                {showLFTable ? 'Hide' : 'Show'} LF Detail Table
              </button>
              {showLFTable && (
                <div className="overflow-x-auto mt-2">
                  <table className="min-w-full text-xs">
                    <thead>
                      <tr className="border-b">
                        <th className="text-left px-2 py-1">LF</th>
                        <th className="text-left px-2 py-1">Coverage</th>
                        <th className="text-left px-2 py-1">Overlaps</th>
                        <th className="text-left px-2 py-1">Conflicts</th>
                        <th className="text-left px-2 py-1">Weight</th>
                      </tr>
                    </thead>
                    <tbody>
                      {results.lf_summary.map((lf, i) => (
                        <tr key={i} className="border-b border-gray-50">
                          <td className="px-2 py-1 font-medium">{String(lf.name ?? `LF ${i}`)}</td>
                          <td className="px-2 py-1">{typeof lf.coverage === 'number' ? (lf.coverage * 100).toFixed(1) + '%' : '-'}</td>
                          <td className="px-2 py-1">{String(lf.overlaps ?? '-')}</td>
                          <td className="px-2 py-1">{String(lf.conflicts ?? '-')}</td>
                          <td className="px-2 py-1">{typeof lf.learned_weight === 'number' ? lf.learned_weight.toFixed(3) : '-'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
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

      <ConfirmDialog open={showDelete} title="Delete Snorkel Job" message={`Delete job #${entity.job_id}?`} onConfirm={() => deleteMutation.mutate()} onCancel={() => setShowDelete(false)} />
      {showRerun && <RunPipelineModal target={{ type: 'snorkel', entity }} onClose={() => setShowRerun(false)} />}
    </div>
  );
}

// New Snorkel Run form (used in CreateEntityModal context)
// Creates a DRAFT snorkel job — index and LFs are connected via canvas edges later.
export function NewSnorkelRunForm({ onClose }: { onClose: () => void }) {
  const cId = useConceptStore((s) => s.activeConcept?.c_id);
  const queryClient = useQueryClient();

  const { register, handleSubmit } = useForm({
    defaultValues: { epochs: 100, lr: 0.01 },
  });

  const createMutation = useMutation({
    mutationFn: (values: { epochs: number; lr: number }) =>
      runSnorkel(cId!, {
        selectedLFs: [],
        snorkel: { epochs: values.epochs, lr: values.lr },
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['snorkelJobs', cId] });
      toast.success('Snorkel job created — connect LFs via edges on canvas');
      onClose();
    },
    onError: () => toast.error('Failed to create Snorkel job'),
  });

  return (
    <form onSubmit={handleSubmit((v) => createMutation.mutate(v))} className="space-y-4 p-4">
      <p className="text-xs text-gray-500">Index and LFs are connected via edges on the canvas after creation.</p>

      <div className="grid grid-cols-2 gap-3">
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Epochs</label>
          <input type="number" {...register('epochs', { valueAsNumber: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md" />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Learning Rate</label>
          <input type="number" step="0.001" {...register('lr', { valueAsNumber: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md" />
        </div>
      </div>

      <div className="flex justify-end gap-3 pt-2">
        <button type="button" onClick={onClose} className="px-4 py-2 text-sm border border-gray-300 rounded-md hover:bg-gray-50">Cancel</button>
        <button type="submit" disabled={createMutation.isPending} className="px-4 py-2 text-sm bg-emerald-600 text-white rounded-md hover:bg-emerald-700 disabled:opacity-50">
          Create
        </button>
      </div>
    </form>
  );
}
