import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Eye, Trash2 } from 'lucide-react';
import toast from 'react-hot-toast';
import type { Dataset } from '../types/dataset';
import { deleteDataset } from '../api/datasets';
import ConfirmDialog from '../../../shared/components/ConfirmDialog';
import ServiceStatusBadge from '../../../shared/components/ServiceStatusBadge';

interface Props {
  datasets: Dataset[];
  onPreview: (dataset: Dataset) => void;
}

export default function DatasetTable({ datasets, onPreview }: Props) {
  const queryClient = useQueryClient();
  const [deleteTarget, setDeleteTarget] = useState<Dataset | null>(null);

  const deleteMutation = useMutation({
    mutationFn: (id: number) => deleteDataset(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['datasets'] });
      toast.success('Dataset deleted');
      setDeleteTarget(null);
    },
    onError: () => toast.error('Failed to delete dataset'),
  });

  return (
    <>
      <div className="overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 bg-gray-50">
              <th className="text-left px-4 py-3 font-medium text-gray-600">Name</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Service</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Type</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Format</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Rows</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Status</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Created</th>
              <th className="text-right px-4 py-3 font-medium text-gray-600">Actions</th>
            </tr>
          </thead>
          <tbody>
            {datasets.map((ds) => (
              <tr key={ds.dataset_id} className="border-b border-gray-100 hover:bg-gray-50">
                <td className="px-4 py-3 font-medium">{ds.name}</td>
                <td className="px-4 py-3">
                  <span className="px-2 py-0.5 bg-blue-50 text-blue-700 rounded text-xs">{ds.service}</span>
                </td>
                <td className="px-4 py-3 text-gray-600">{ds.dataset_type}</td>
                <td className="px-4 py-3 text-gray-600">{ds.format}</td>
                <td className="px-4 py-3 text-gray-600">{ds.row_count?.toLocaleString() ?? '—'}</td>
                <td className="px-4 py-3"><ServiceStatusBadge status={ds.status} /></td>
                <td className="px-4 py-3 text-gray-600">{new Date(ds.created_at).toLocaleDateString()}</td>
                <td className="px-4 py-3 text-right">
                  <div className="flex items-center justify-end gap-1">
                    <button onClick={() => onPreview(ds)} className="p-1.5 rounded hover:bg-gray-100" title="Preview">
                      <Eye className="w-4 h-4 text-blue-500" />
                    </button>
                    <button onClick={() => setDeleteTarget(ds)} className="p-1.5 rounded hover:bg-gray-100" title="Delete">
                      <Trash2 className="w-4 h-4 text-red-400" />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <ConfirmDialog
        open={!!deleteTarget}
        title="Delete Dataset"
        message={`Delete "${deleteTarget?.name}"? This will soft-delete the dataset.`}
        onConfirm={() => deleteTarget && deleteMutation.mutate(deleteTarget.dataset_id)}
        onCancel={() => setDeleteTarget(null)}
      />
    </>
  );
}
