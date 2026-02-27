import { useState } from 'react';
import { useForm } from 'react-hook-form';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import toast from 'react-hot-toast';
import { ArrowRight, RefreshCw } from 'lucide-react';
import { useConceptStore } from '../../stores/conceptStore';
import { usePipelineStore } from '../../stores/pipelineStore';
import { updateConceptValue, deleteConceptValue } from '../../api/concepts';
import type { ConceptValue } from '../../types/entities';
import type { CVTreeNodeData } from '../../types/nodes';
import ConfirmDialog from '../../../../shared/components/ConfirmDialog';
import RunPipelineModal from '../modals/RunPipelineModal';

interface Props {
  entity: CVTreeNodeData;
}

export default function CVTreePanel({ entity }: Props) {
  const cv = entity.entity as ConceptValue;
  const cId = useConceptStore((s) => s.activeConcept?.c_id);
  const conceptValues = useConceptStore((s) => s.conceptValues);
  const setSelectedCV = useConceptStore((s) => s.setSelectedCV);
  const setCanvasView = useConceptStore((s) => s.setCanvasView);
  const closePanel = usePipelineStore((s) => s.closePanel);
  const queryClient = useQueryClient();
  const [showDelete, setShowDelete] = useState(false);
  const [showRerun, setShowRerun] = useState(false);

  const parentCV = conceptValues.find((c) => c.cv_id === cv.parent_cv_id);
  const childCount = conceptValues.filter((c) => c.parent_cv_id === cv.cv_id).length;

  const { register, handleSubmit } = useForm({
    defaultValues: {
      name: cv.name,
      description: cv.description ?? '',
      display_order: cv.display_order ?? '',
    },
  });

  const saveMutation = useMutation({
    mutationFn: (values: { name: string; description: string; display_order: number | string }) =>
      updateConceptValue(cId!, cv.cv_id, {
        name: values.name,
        description: values.description || undefined,
        level: cv.level,
        display_order: values.display_order !== '' ? Number(values.display_order) : undefined,
        parent_cv_id: cv.parent_cv_id ?? undefined,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['conceptValues', cId] });
      queryClient.invalidateQueries({ queryKey: ['cvTree', cId] });
      toast.success('Concept value updated');
    },
    onError: () => toast.error('Failed to update concept value'),
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteConceptValue(cId!, cv.cv_id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['conceptValues', cId] });
      queryClient.invalidateQueries({ queryKey: ['cvTree', cId] });
      toast.success('Concept value deleted');
      closePanel();
    },
    onError: () => toast.error('Failed to delete concept value'),
  });

  const handleOpenPipeline = () => {
    setSelectedCV(cv);
    setCanvasView('pipeline');
  };

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

        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">ID</label>
            <input value={cv.cv_id} readOnly className="w-full px-3 py-2 text-sm border border-gray-200 rounded-md bg-gray-50 text-gray-500 cursor-not-allowed font-mono" />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Level</label>
            <input value={cv.level} readOnly className="w-full px-3 py-2 text-sm border border-gray-200 rounded-md bg-gray-50 text-gray-500 cursor-not-allowed" />
          </div>
        </div>

        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Parent</label>
          <input
            value={parentCV ? `${parentCV.name} (#${parentCV.cv_id})` : 'None (root)'}
            readOnly
            className="w-full px-3 py-2 text-sm border border-gray-200 rounded-md bg-gray-50 text-gray-500 cursor-not-allowed"
          />
        </div>

        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Display Order</label>
          <input type="number" {...register('display_order')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500" placeholder="Optional" />
        </div>

        {childCount > 0 && (
          <p className="text-xs text-gray-500">{childCount} child concept value{childCount > 1 ? 's' : ''}</p>
        )}

        <div className="flex gap-2 pt-2">
          <button type="submit" disabled={saveMutation.isPending} className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50">Save</button>
          <button type="button" onClick={() => setShowDelete(true)} className="px-4 py-2 text-sm border border-red-300 text-red-600 rounded-md hover:bg-red-50 ml-auto">Delete</button>
        </div>
      </form>

      <div className="flex items-center gap-3 text-xs text-gray-600">
        <span className="font-medium">{entity.ruleCount} rules</span>
        <span className="text-gray-300">&middot;</span>
        <span className="font-medium">{entity.lfCount} LFs</span>
        <span className="text-gray-300">&middot;</span>
        <span className="font-medium">Snorkel: {entity.snorkelStatus}</span>
      </div>

      <button
        onClick={handleOpenPipeline}
        className="w-full flex items-center justify-center gap-2 px-3 py-2 text-sm font-medium bg-indigo-600 text-white rounded-md hover:bg-indigo-700"
      >
        Open Pipeline
        <ArrowRight className="w-4 h-4" />
      </button>

      {entity.status === 'stale' && (
        <button
          onClick={() => setShowRerun(true)}
          className="w-full flex items-center justify-center gap-2 px-3 py-2 text-sm font-medium bg-amber-600 text-white rounded-md hover:bg-amber-700"
        >
          <RefreshCw className="w-4 h-4" />
          Re-run Stale
        </button>
      )}

      <ConfirmDialog open={showDelete} title="Delete Concept Value" message={`Delete "${cv.name}"?`} onConfirm={() => deleteMutation.mutate()} onCancel={() => setShowDelete(false)} />

      {showRerun && (
        <RunPipelineModal
          target={{ type: 'cv', cv }}
          onClose={() => setShowRerun(false)}
        />
      )}
    </div>
  );
}
