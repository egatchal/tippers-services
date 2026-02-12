import { useState } from 'react';
import { useForm } from 'react-hook-form';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import toast from 'react-hot-toast';
import type { Rule } from '../../types/entities';
import { updateRule, materializeRule, deleteRule } from '../../api/rules';
import { useConceptStore } from '../../stores/conceptStore';
import { usePipelineStore } from '../../stores/pipelineStore';
import StatusBadge from '../../../../shared/components/StatusBadge';
import SqlEditor from '../../../../shared/components/SqlEditor';
import { getStatusFromEntity } from '../../../../shared/utils/statusColors';
import ConfirmDialog from '../../../../shared/components/ConfirmDialog';
import ColumnStatsTable from './ColumnStatsTable';

export default function RulePanel({ entity }: { entity: Rule }) {
  const cId = useConceptStore((s) => s.activeConcept?.c_id);
  const queryClient = useQueryClient();
  const closePanel = usePipelineStore((s) => s.closePanel);
  const [showDelete, setShowDelete] = useState(false);

  const { register, handleSubmit } = useForm({
    defaultValues: {
      name: entity.name,
      sql_query: entity.sql_query,
      index_column: entity.index_column ?? '',
    },
  });

  const saveMutation = useMutation({
    mutationFn: (values: { name: string; sql_query: string; index_column: string }) =>
      updateRule(cId!, entity.r_id, values),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['rules', cId] });
      toast.success('Rule updated');
    },
    onError: () => toast.error('Failed to update rule'),
  });

  const materializeMutation = useMutation({
    mutationFn: () => materializeRule(cId!, entity.r_id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['rules', cId] });
      toast.success('Materialization started');
    },
    onError: () => toast.error('Failed to materialize'),
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteRule(cId!, entity.r_id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['rules', cId] });
      toast.success('Rule deleted');
      closePanel();
    },
    onError: () => toast.error('Failed to delete rule'),
  });

  return (
    <div className="p-4 space-y-4">
      <div className="flex items-center justify-between">
        <StatusBadge status={getStatusFromEntity(entity)} />
        {entity.is_materialized && entity.row_count != null && (
          <span className="text-xs text-gray-500">{entity.row_count} rows</span>
        )}
      </div>

      {entity.is_materialized && <ColumnStatsTable stats={entity.column_stats} />}

      <form onSubmit={handleSubmit((v) => saveMutation.mutate(v))} className="space-y-3">
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Name</label>
          <input {...register('name')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500" />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Index Column</label>
          <input {...register('index_column')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500" placeholder="e.g., mac_address" />
        </div>
        <SqlEditor label="SQL Query (use :index_values)" {...register('sql_query')} />

        <div className="flex gap-2 pt-2">
          <button type="submit" disabled={saveMutation.isPending} className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50">Save</button>
          <button type="button" onClick={() => materializeMutation.mutate()} disabled={materializeMutation.isPending} className="px-4 py-2 text-sm bg-green-600 text-white rounded-md hover:bg-green-700 disabled:opacity-50">Materialize</button>
          <button type="button" onClick={() => setShowDelete(true)} className="px-4 py-2 text-sm border border-red-300 text-red-600 rounded-md hover:bg-red-50 ml-auto">Delete</button>
        </div>
      </form>

      <ConfirmDialog open={showDelete} title="Delete Rule" message={`Delete "${entity.name}"?`} onConfirm={() => deleteMutation.mutate()} onCancel={() => setShowDelete(false)} />
    </div>
  );
}
