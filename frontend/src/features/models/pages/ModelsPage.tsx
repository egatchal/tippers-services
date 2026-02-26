import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Plus } from 'lucide-react';
import toast from 'react-hot-toast';
import { listDeployments, createDeployment } from '../api/serving';
import type { Deployment, DeploymentCreate } from '../types/deployment';
import DeploymentTable from '../components/DeploymentTable';
import DeployForm from '../components/DeployForm';
import PredictModal from '../components/PredictModal';
import ServiceTabs from '../../../shared/components/ServiceTabs';
import LoadingSpinner from '../../../shared/components/LoadingSpinner';
import EmptyState from '../../../shared/components/EmptyState';

export default function ModelsPage() {
  const queryClient = useQueryClient();
  const [formMode, setFormMode] = useState<'closed' | 'create'>('closed');
  const [activeService, setActiveService] = useState<string | null>(null);
  const [predictTarget, setPredictTarget] = useState<Deployment | null>(null);

  const { data: deployments, isLoading } = useQuery({
    queryKey: ['deployments', { service: activeService }],
    queryFn: () => listDeployments(activeService ? { service: activeService } : undefined),
  });

  const createMutation = useMutation({
    mutationFn: (body: DeploymentCreate) => createDeployment(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['deployments'] });
      toast.success('Model deployed');
      setFormMode('closed');
    },
    onError: () => toast.error('Failed to deploy model'),
  });

  return (
    <div className="h-full overflow-y-auto p-6">
      <div className="max-w-5xl mx-auto">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-lg font-semibold">Model Deployments</h2>
            <p className="text-sm text-gray-500">Deploy and manage MLflow models for real-time inference.</p>
          </div>
          {formMode === 'closed' && (
            <button onClick={() => setFormMode('create')} className="flex items-center gap-2 px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700">
              <Plus className="w-4 h-4" /> Deploy Model
            </button>
          )}
        </div>

        <ServiceTabs active={activeService} onChange={setActiveService} />

        {formMode !== 'closed' && (
          <div className="bg-white border border-gray-200 rounded-lg p-6 mb-6">
            <h3 className="text-sm font-semibold mb-4">Deploy Model</h3>
            <DeployForm
              onSubmit={(values) => createMutation.mutate(values)}
              onCancel={() => setFormMode('closed')}
              loading={createMutation.isPending}
            />
          </div>
        )}

        <div className="bg-white border border-gray-200 rounded-lg">
          {isLoading ? (
            <LoadingSpinner />
          ) : !deployments?.length ? (
            <EmptyState title="No deployments" message="Deploy a model to start serving predictions." />
          ) : (
            <DeploymentTable deployments={deployments} onTestPredict={setPredictTarget} />
          )}
        </div>
      </div>

      {predictTarget && (
        <PredictModal deployment={predictTarget} onClose={() => setPredictTarget(null)} />
      )}
    </div>
  );
}
