import { useState } from 'react';
import { useForm } from 'react-hook-form';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import toast from 'react-hot-toast';
import type { ConceptValue } from '../../types/entities';
import type { PipelineNodeData } from '../../types/nodes';
import { updateConceptValue, deleteConceptValue } from '../../api/concepts';
import { useConceptStore } from '../../stores/conceptStore';
import { usePipelineStore } from '../../stores/pipelineStore';
import ConfirmDialog from '../../../../shared/components/ConfirmDialog';

export default function ConceptValuePanel({ entity }: { entity: ConceptValue }) {
  const cId = useConceptStore((s) => s.activeConcept?.c_id);
  const queryClient = useQueryClient();
  const nodes = usePipelineStore((s) => s.nodes);
  const closePanel = usePipelineStore((s) => s.closePanel);
  const [showDelete, setShowDelete] = useState(false);

  const { register, handleSubmit } = useForm({
    defaultValues: {
      name: entity.name,
      description: entity.description ?? '',
      level: entity.level,
      display_order: entity.display_order ?? '',
    },
  });

  const saveMutation = useMutation({
    mutationFn: (values: { name: string; description: string; level: number; display_order: number | string }) =>
      updateConceptValue(cId!, entity.cv_id, {
        name: values.name,
        description: values.description || undefined,
        level: Number(values.level),
        display_order: values.display_order !== '' ? Number(values.display_order) : undefined,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['conceptValues', cId] });
      toast.success('Concept value updated');
    },
    onError: () => toast.error('Failed to update concept value'),
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteConceptValue(cId!, entity.cv_id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['conceptValues', cId] });
      toast.success('Concept value deleted');
      closePanel();
    },
    onError: () => toast.error('Failed to delete concept value'),
  });

  const linkedLFs = nodes.filter((n) => {
    if (n.data.entityType !== 'lf') return false;
    const lf = (n.data as PipelineNodeData & { entity: { applicable_cv_ids: number[] } }).entity;
    return lf.applicable_cv_ids?.includes(entity.cv_id);
  });

  return (
    <div className="p-4 space-y-4">
      <form onSubmit={handleSubmit((v) => saveMutation.mutate(v))} className="space-y-3">
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Name</label>
          <input {...register('name')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500" />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Description</label>
          <input {...register('description')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500" placeholder="Optional" />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Level</label>
          <input type="number" {...register('level', { valueAsNumber: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500" />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Display Order</label>
          <input type="number" {...register('display_order')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500" placeholder="Optional" />
        </div>

        <div className="flex gap-2 pt-2">
          <button type="submit" disabled={saveMutation.isPending} className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50">Save</button>
          <button type="button" onClick={() => setShowDelete(true)} className="px-4 py-2 text-sm border border-red-300 text-red-600 rounded-md hover:bg-red-50 ml-auto">Delete</button>
        </div>
      </form>

      <div>
        <h4 className="text-xs font-semibold text-gray-600 uppercase mb-2">
          Labeling Functions ({linkedLFs.length})
        </h4>
        {linkedLFs.length === 0 ? (
          <p className="text-xs text-gray-400">No LFs reference this label</p>
        ) : (
          <div className="space-y-1">
            {linkedLFs.map((n) => (
              <div key={n.id} className="flex items-center gap-2 text-sm bg-gray-50 rounded px-2 py-1.5">
                <span className={`w-2 h-2 rounded-full shrink-0 ${n.data.status === 'materialized' ? 'bg-green-500' : 'bg-gray-400'}`} />
                <span className="truncate">{n.data.label}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      <ConfirmDialog open={showDelete} title="Delete Concept Value" message={`Delete "${entity.name}"?`} onConfirm={() => deleteMutation.mutate()} onCancel={() => setShowDelete(false)} />
    </div>
  );
}
