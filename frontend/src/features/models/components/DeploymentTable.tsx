import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Power, FlaskConical } from 'lucide-react';
import toast from 'react-hot-toast';
import type { Deployment } from '../types/deployment';
import { deactivateDeployment } from '../api/serving';
import ConfirmDialog from '../../../shared/components/ConfirmDialog';
import ServiceStatusBadge from '../../../shared/components/ServiceStatusBadge';

interface Props {
  deployments: Deployment[];
  onTestPredict: (deployment: Deployment) => void;
}

export default function DeploymentTable({ deployments, onTestPredict }: Props) {
  const queryClient = useQueryClient();
  const [deactivateTarget, setDeactivateTarget] = useState<Deployment | null>(null);

  const deactivateMutation = useMutation({
    mutationFn: (id: number) => deactivateDeployment(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['deployments'] });
      toast.success('Deployment deactivated');
      setDeactivateTarget(null);
    },
    onError: () => toast.error('Failed to deactivate deployment'),
  });

  return (
    <>
      <div className="overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 bg-gray-50">
              <th className="text-left px-4 py-3 font-medium text-gray-600">ID</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Model ID</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Version</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Service</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Status</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Model URI</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Created</th>
              <th className="text-right px-4 py-3 font-medium text-gray-600">Actions</th>
            </tr>
          </thead>
          <tbody>
            {deployments.map((dep) => (
              <tr key={dep.deployment_id} className="border-b border-gray-100 hover:bg-gray-50">
                <td className="px-4 py-3 font-mono text-gray-600">{dep.deployment_id}</td>
                <td className="px-4 py-3 text-gray-600">{dep.model_id}</td>
                <td className="px-4 py-3 text-gray-600">{dep.model_version_id}</td>
                <td className="px-4 py-3">
                  <span className="px-2 py-0.5 bg-blue-50 text-blue-700 rounded text-xs">{dep.service}</span>
                </td>
                <td className="px-4 py-3"><ServiceStatusBadge status={dep.status} /></td>
                <td className="px-4 py-3 font-mono text-xs text-gray-500 max-w-[180px] truncate">
                  {dep.mlflow_model_uri ?? '—'}
                </td>
                <td className="px-4 py-3 text-gray-600">{new Date(dep.created_at).toLocaleDateString()}</td>
                <td className="px-4 py-3 text-right">
                  <div className="flex items-center justify-end gap-1">
                    {dep.status.toUpperCase() === 'ACTIVE' && (
                      <button onClick={() => onTestPredict(dep)} className="p-1.5 rounded hover:bg-gray-100" title="Test Predict">
                        <FlaskConical className="w-4 h-4 text-blue-500" />
                      </button>
                    )}
                    {dep.status.toUpperCase() === 'ACTIVE' && (
                      <button onClick={() => setDeactivateTarget(dep)} className="p-1.5 rounded hover:bg-gray-100" title="Deactivate">
                        <Power className="w-4 h-4 text-red-400" />
                      </button>
                    )}
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <ConfirmDialog
        open={!!deactivateTarget}
        title="Deactivate Deployment"
        message={`Deactivate deployment ${deactivateTarget?.deployment_id}? The model will no longer serve predictions.`}
        confirmLabel="Deactivate"
        onConfirm={() => deactivateTarget && deactivateMutation.mutate(deactivateTarget.deployment_id)}
        onCancel={() => setDeactivateTarget(null)}
      />
    </>
  );
}
