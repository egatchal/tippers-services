import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Plus } from 'lucide-react';
import toast from 'react-hot-toast';
import { listDatasets, registerDataset } from '../api/datasets';
import type { Dataset, DatasetCreate } from '../types/dataset';
import DatasetTable from '../components/DatasetTable';
import DatasetRegisterForm from '../components/DatasetRegisterForm';
import DatasetPreviewModal from '../components/DatasetPreviewModal';
import ServiceTabs from '../../../shared/components/ServiceTabs';
import LoadingSpinner from '../../../shared/components/LoadingSpinner';
import EmptyState from '../../../shared/components/EmptyState';

export default function DatasetsPage() {
  const queryClient = useQueryClient();
  const [formMode, setFormMode] = useState<'closed' | 'create'>('closed');
  const [activeService, setActiveService] = useState<string | null>(null);
  const [previewTarget, setPreviewTarget] = useState<Dataset | null>(null);

  const { data: datasets, isLoading } = useQuery({
    queryKey: ['datasets', { service: activeService }],
    queryFn: () => listDatasets(activeService ? { service: activeService } : undefined),
  });

  const createMutation = useMutation({
    mutationFn: (body: DatasetCreate) => registerDataset(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['datasets'] });
      toast.success('Dataset registered');
      setFormMode('closed');
    },
    onError: () => toast.error('Failed to register dataset'),
  });

  return (
    <div className="h-full overflow-y-auto p-6">
      <div className="max-w-5xl mx-auto">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-lg font-semibold">Datasets</h2>
            <p className="text-sm text-gray-500">Browse and manage data artifacts across all services.</p>
          </div>
          {formMode === 'closed' && (
            <button onClick={() => setFormMode('create')} className="flex items-center gap-2 px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700">
              <Plus className="w-4 h-4" /> Register Dataset
            </button>
          )}
        </div>

        <ServiceTabs active={activeService} onChange={setActiveService} />

        {formMode !== 'closed' && (
          <div className="bg-white border border-gray-200 rounded-lg p-6 mb-6">
            <h3 className="text-sm font-semibold mb-4">Register Dataset</h3>
            <DatasetRegisterForm
              onSubmit={(values) => createMutation.mutate(values)}
              onCancel={() => setFormMode('closed')}
              loading={createMutation.isPending}
            />
          </div>
        )}

        <div className="bg-white border border-gray-200 rounded-lg">
          {isLoading ? (
            <LoadingSpinner />
          ) : !datasets?.length ? (
            <EmptyState title="No datasets" message="Datasets will appear here as they are created by pipeline runs." />
          ) : (
            <DatasetTable datasets={datasets} onPreview={setPreviewTarget} />
          )}
        </div>
      </div>

      {previewTarget && (
        <DatasetPreviewModal dataset={previewTarget} onClose={() => setPreviewTarget(null)} />
      )}
    </div>
  );
}
