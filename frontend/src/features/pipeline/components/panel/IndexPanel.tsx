import { useState } from 'react';
import { useForm } from 'react-hook-form';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import toast from 'react-hot-toast';
import type { Index } from '../../types/entities';
import { updateIndex, materializeIndex, deleteIndex, updateLabelFilter, getEntityPreview } from '../../api/indexes';
import { listConnections } from '../../../connections/api/connections';
import { useConceptStore } from '../../stores/conceptStore';
import { usePipelineStore } from '../../stores/pipelineStore';
import StatusBadge from '../../../../shared/components/StatusBadge';
import SqlEditor from '../../../../shared/components/SqlEditor';
import { getStatusFromEntity } from '../../../../shared/utils/statusColors';
import ConfirmDialog from '../../../../shared/components/ConfirmDialog';
import ColumnStatsTable from './ColumnStatsTable';

interface Props {
  entity: Index;
}

export default function IndexPanel({ entity }: Props) {
  const cId = useConceptStore((s) => s.activeConcept?.c_id);
  const queryClient = useQueryClient();
  const closePanel = usePipelineStore((s) => s.closePanel);
  const [showDelete, setShowDelete] = useState(false);
  const isDerived = entity.source_type === 'derived';

  const { data: connections } = useQuery({
    queryKey: ['connections'],
    queryFn: listConnections,
    enabled: !isDerived,
  });

  const { register, handleSubmit } = useForm({
    defaultValues: {
      name: entity.name,
      conn_id: entity.conn_id,
      key_column: entity.key_column ?? '',
      sql_query: entity.sql_query,
    },
  });

  const [threshold, setThreshold] = useState(() => {
    const filter = entity.label_filter as { labels?: Record<string, { min_confidence?: number }> } | null;
    if (filter?.labels) {
      const first = Object.values(filter.labels)[0];
      return first?.min_confidence ?? 0.5;
    }
    return 0.5;
  });

  const { data: previewData } = useQuery({
    queryKey: ['entityPreview', cId, entity.index_id],
    queryFn: () => getEntityPreview(cId!, entity.index_id, 20),
    enabled: isDerived && !!cId,
  });

  const saveMutation = useMutation({
    mutationFn: (values: { name: string; conn_id: number; key_column: string; sql_query: string }) =>
      updateIndex(cId!, entity.index_id, values),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['indexes', cId] });
      toast.success('Index updated');
    },
    onError: () => toast.error('Failed to update index'),
  });

  const materializeMutation = useMutation({
    mutationFn: () => materializeIndex(cId!, entity.index_id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['indexes', cId] });
      toast.success('Materialization started');
    },
    onError: () => toast.error('Failed to materialize'),
  });

  const filterMutation = useMutation({
    mutationFn: () => {
      const label_filter = { labels: { [String(entity.cv_id)]: { min_confidence: threshold } } };
      return updateLabelFilter(cId!, entity.index_id, { label_filter });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['indexes', cId] });
      toast.success('Label filter updated');
    },
    onError: () => toast.error('Failed to update label filter'),
  });

  const deleteMutation = useMutation({
    mutationFn: () => deleteIndex(cId!, entity.index_id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['indexes', cId] });
      toast.success('Index deleted');
      closePanel();
    },
    onError: () => toast.error('Failed to delete index'),
  });

  if (isDerived) {
    return (
      <div className="p-4 space-y-4">
        <div className="flex items-center justify-between">
          <StatusBadge status={getStatusFromEntity(entity)} />
          <span className="text-xs text-gray-500 bg-gray-100 px-2 py-0.5 rounded">Derived</span>
        </div>

        <div className="grid grid-cols-2 gap-3 text-sm">
          <div>
            <span className="text-gray-500 text-xs">CV ID</span>
            <p className="font-medium text-gray-900">{entity.cv_id ?? '—'}</p>
          </div>
          <div>
            <span className="text-gray-500 text-xs">Entities</span>
            <p className="font-medium text-gray-900">{entity.filtered_count ?? 0}</p>
          </div>
          {entity.parent_index_id && (
            <div>
              <span className="text-gray-500 text-xs">Parent Index</span>
              <p className="font-medium text-gray-900">#{entity.parent_index_id}</p>
            </div>
          )}
          {entity.parent_snorkel_job_id && (
            <div>
              <span className="text-gray-500 text-xs">Parent Snorkel Job</span>
              <p className="font-medium text-gray-900">#{entity.parent_snorkel_job_id}</p>
            </div>
          )}
        </div>

        {entity.output_type && (
          <div>
            <span className="text-gray-500 text-xs">Output Type</span>
            <p className="font-medium text-gray-900">{entity.output_type === 'softmax' ? 'Softmax' : entity.output_type === 'argmax' ? 'Argmax' : entity.output_type}</p>
          </div>
        )}

        {entity.output_type === 'softmax' && (
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Min Confidence</label>
            <input
              type="number"
              step="0.05"
              min="0"
              max="1"
              value={threshold}
              onChange={(e) => setThreshold(Number(e.target.value))}
              className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
            />
            <p className="text-[10px] text-gray-400 mt-0.5">Entities with probability &ge; this value are included</p>
            <button
              onClick={() => filterMutation.mutate()}
              disabled={filterMutation.isPending}
              className="mt-1 px-3 py-1.5 text-xs bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50"
            >
              Save
            </button>
          </div>
        )}

        {previewData && previewData.entities.length > 0 && (
          <div>
            <h4 className="text-xs font-semibold text-gray-600 uppercase mb-2">Entity Preview ({previewData.total_count} total)</h4>
            <div className="overflow-x-auto border border-gray-200 rounded">
              <table className="text-xs w-full">
                <thead>
                  <tr className="bg-gray-50">
                    {Object.keys(previewData.entities[0]).map((col) => (
                      <th key={col} className="px-2 py-1 text-left font-medium text-gray-600 border-b">{col}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {previewData.entities.map((row, i) => (
                    <tr key={i} className="border-b last:border-0">
                      {Object.values(row).map((val, j) => (
                        <td key={j} className="px-2 py-1 text-gray-700 truncate max-w-[120px]">{String(val ?? '')}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        <div className="flex gap-2 pt-2">
          <button type="button" onClick={() => setShowDelete(true)} className="px-4 py-2 text-sm border border-red-300 text-red-600 rounded-md hover:bg-red-50 ml-auto">
            Delete
          </button>
        </div>

        <ConfirmDialog
          open={showDelete}
          title="Delete Derived Index"
          message={`Delete "${entity.name}"?`}
          onConfirm={() => deleteMutation.mutate()}
          onCancel={() => setShowDelete(false)}
        />
      </div>
    );
  }

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
          <label className="block text-xs font-medium text-gray-700 mb-1">Connection</label>
          <select {...register('conn_id', { valueAsNumber: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500">
            {connections?.map((c) => (
              <option key={c.conn_id} value={c.conn_id}>{c.name}</option>
            ))}
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Key Column</label>
          <input {...register('key_column')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500" placeholder='e.g. "user"' />
          <p className="text-[10px] text-gray-400 mt-0.5">Column containing unique entity identifiers</p>
        </div>
        <SqlEditor label="SQL Query" {...register('sql_query')} />

        <div className="flex gap-2 pt-2">
          <button type="submit" disabled={saveMutation.isPending} className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50">
            Save
          </button>
          <button type="button" onClick={() => materializeMutation.mutate()} disabled={materializeMutation.isPending} className="px-4 py-2 text-sm bg-green-600 text-white rounded-md hover:bg-green-700 disabled:opacity-50">
            Materialize
          </button>
          <button type="button" onClick={() => setShowDelete(true)} className="px-4 py-2 text-sm border border-red-300 text-red-600 rounded-md hover:bg-red-50 ml-auto">
            Delete
          </button>
        </div>
      </form>

      <ConfirmDialog
        open={showDelete}
        title="Delete Index"
        message={`Delete "${entity.name}"? This will also remove dependent rules and labeling functions.`}
        onConfirm={() => deleteMutation.mutate()}
        onCancel={() => setShowDelete(false)}
      />
    </div>
  );
}
