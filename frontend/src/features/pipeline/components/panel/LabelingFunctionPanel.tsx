import { useState } from 'react';
import { useForm } from 'react-hook-form';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import toast from 'react-hot-toast';
import { Check, Power, X } from 'lucide-react';
import type { LabelingFunction } from '../../types/entities';
import { updateLF, deleteLF, approveLF, toggleLF } from '../../api/labelingFunctions';
import { useConceptStore } from '../../stores/conceptStore';
import { usePipelineStore } from '../../stores/pipelineStore';
import CodeEditor from '../../../../shared/components/CodeEditor';
import ConfirmDialog from '../../../../shared/components/ConfirmDialog';

export default function LabelingFunctionPanel({ entity }: { entity: LabelingFunction }) {
  const cId = useConceptStore((s) => s.activeConcept?.c_id);
  const conceptValues = useConceptStore((s) => s.conceptValues);
  const queryClient = useQueryClient();
  const closePanel = usePipelineStore((s) => s.closePanel);
  const [showDelete, setShowDelete] = useState(false);

  const { register, handleSubmit } = useForm({
    defaultValues: {
      name: entity.name,
      code: entity.lf_config?.code ?? '',
    },
  });

  const invalidate = () => queryClient.invalidateQueries({ queryKey: ['lfs', cId] });

  const saveMutation = useMutation({
    mutationFn: (values: { name: string; code: string }) =>
      updateLF(cId!, entity.lf_id, {
        name: values.name,
        lf_config: { ...entity.lf_config, code: values.code },
      }),
    onSuccess: () => { invalidate(); toast.success('LF updated'); },
    onError: () => toast.error('Failed to update LF'),
  });

  const approveMutation = useMutation({
    mutationFn: () => approveLF(cId!, entity.lf_id),
    onSuccess: () => { invalidate(); toast.success('LF approved'); },
    onError: () => toast.error('Failed to approve LF'),
  });

  const toggleMutation = useMutation({
    mutationFn: () => toggleLF(cId!, entity.lf_id),
    onSuccess: () => { invalidate(); toast.success(`LF ${entity.is_active ? 'deactivated' : 'activated'}`); },
    onError: () => toast.error('Failed to toggle LF'),
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteLF(cId!, entity.lf_id),
    onSuccess: () => { invalidate(); toast.success('LF deleted'); closePanel(); },
    onError: () => toast.error('Failed to delete LF'),
  });

  const applicableCVs = entity.applicable_cv_ids
    .map((id) => conceptValues.find((cv) => cv.cv_id === id))
    .filter(Boolean);
  const applicableLabels = applicableCVs
    .map((cv) => cv?.name ?? 'unknown')
    .join(', ');
  const cvLevels = [...new Set(applicableCVs.map((cv) => cv?.level).filter(Boolean))];

  return (
    <div className="p-4 space-y-4">
      <div className="flex items-center gap-2 flex-wrap">
        <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${entity.is_active ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-600'}`}>
          {entity.is_active ? <Check className="w-3 h-3" /> : <X className="w-3 h-3" />}
          {entity.is_active ? 'Active' : 'Inactive'}
        </span>
        <span className="text-xs text-gray-500">v{entity.version}</span>
        {cvLevels.map((lvl) => (
          <span key={lvl} className="text-xs bg-indigo-100 text-indigo-700 px-2 py-0.5 rounded-full">
            Level {lvl}
          </span>
        ))}
        {entity.requires_approval && (
          <span className="text-xs bg-amber-100 text-amber-800 px-2 py-0.5 rounded-full">Needs Approval</span>
        )}
      </div>

      <div className="text-xs text-gray-500">
        <span className="font-medium">Labels:</span> {applicableLabels}
      </div>

      {entity.coverage != null && (
        <div className="grid grid-cols-3 gap-2 text-center">
          <div className="bg-gray-50 rounded p-2">
            <p className="text-lg font-semibold">{(entity.coverage * 100).toFixed(1)}%</p>
            <p className="text-[10px] text-gray-500 uppercase">Coverage</p>
          </div>
          <div className="bg-gray-50 rounded p-2">
            <p className="text-lg font-semibold">{entity.estimated_accuracy != null ? (entity.estimated_accuracy * 100).toFixed(1) + '%' : '—'}</p>
            <p className="text-[10px] text-gray-500 uppercase">Accuracy</p>
          </div>
          <div className="bg-gray-50 rounded p-2">
            <p className="text-lg font-semibold">{entity.conflicts ?? 0}</p>
            <p className="text-[10px] text-gray-500 uppercase">Conflicts</p>
          </div>
        </div>
      )}

      <form onSubmit={handleSubmit((v) => saveMutation.mutate(v))} className="space-y-3">
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Name</label>
          <input {...register('name')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500" />
        </div>
        <CodeEditor label="Python Code" {...register('code')} />

        <div className="flex gap-2 pt-2 flex-wrap">
          <button type="submit" disabled={saveMutation.isPending} className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50">Save</button>
          {entity.requires_approval && (
            <button type="button" onClick={() => approveMutation.mutate()} disabled={approveMutation.isPending} className="px-4 py-2 text-sm bg-green-600 text-white rounded-md hover:bg-green-700 disabled:opacity-50">
              Approve
            </button>
          )}
          <button type="button" onClick={() => toggleMutation.mutate()} disabled={toggleMutation.isPending} className="inline-flex items-center gap-1 px-4 py-2 text-sm border border-gray-300 rounded-md hover:bg-gray-50">
            <Power className="w-3.5 h-3.5" />
            {entity.is_active ? 'Deactivate' : 'Activate'}
          </button>
          <button type="button" onClick={() => setShowDelete(true)} className="px-4 py-2 text-sm border border-red-300 text-red-600 rounded-md hover:bg-red-50 ml-auto">Delete</button>
        </div>
      </form>

      <ConfirmDialog open={showDelete} title="Delete Labeling Function" message={`Delete "${entity.name}"?`} onConfirm={() => deleteMutation.mutate()} onCancel={() => setShowDelete(false)} />
    </div>
  );
}
