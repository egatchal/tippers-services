import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { XCircle, ChevronDown, ChevronRight } from 'lucide-react';
import toast from 'react-hot-toast';
import type { WorkflowRun } from '../types/workflow';
import { cancelWorkflowRun } from '../api/workflows';
import ServiceStatusBadge from '../../../shared/components/ServiceStatusBadge';
import StepStatusBreakdown from './StepStatusBreakdown';

interface Props {
  runs: WorkflowRun[];
}

export default function WorkflowRunTable({ runs }: Props) {
  const queryClient = useQueryClient();
  const [expandedId, setExpandedId] = useState<number | null>(null);

  const cancelMutation = useMutation({
    mutationFn: (runId: number) => cancelWorkflowRun(runId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['workflow-runs'] });
      toast.success('Workflow run cancelled');
    },
    onError: () => toast.error('Failed to cancel workflow run'),
  });

  const getStepProgress = (run: WorkflowRun) => {
    if (!run.step_statuses) return '0/0';
    const entries = Object.values(run.step_statuses);
    const completed = entries.filter((s) => s.status === 'COMPLETED' || s.status === 'SUCCESS').length;
    return `${completed}/${entries.length} steps`;
  };

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b border-gray-200 bg-gray-50">
            <th className="w-8 px-2 py-3" />
            <th className="text-left px-4 py-3 font-medium text-gray-600">ID</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Template</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Service</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Status</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Progress</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Created</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Completed</th>
            <th className="text-right px-4 py-3 font-medium text-gray-600">Actions</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((run) => (
            <>
              <tr
                key={run.run_id}
                className="border-b border-gray-100 hover:bg-gray-50 cursor-pointer"
                onClick={() => setExpandedId(expandedId === run.run_id ? null : run.run_id)}
              >
                <td className="px-2 py-3">
                  {expandedId === run.run_id ? (
                    <ChevronDown className="w-4 h-4 text-gray-400" />
                  ) : (
                    <ChevronRight className="w-4 h-4 text-gray-400" />
                  )}
                </td>
                <td className="px-4 py-3 font-mono text-gray-600">{run.run_id}</td>
                <td className="px-4 py-3 text-gray-600">Template {run.template_id}</td>
                <td className="px-4 py-3">
                  <span className="px-2 py-0.5 bg-blue-50 text-blue-700 rounded text-xs">{run.service}</span>
                </td>
                <td className="px-4 py-3"><ServiceStatusBadge status={run.status} /></td>
                <td className="px-4 py-3 text-gray-600">{getStepProgress(run)}</td>
                <td className="px-4 py-3 text-gray-600">{new Date(run.created_at).toLocaleDateString()}</td>
                <td className="px-4 py-3 text-gray-600">
                  {run.completed_at ? new Date(run.completed_at).toLocaleDateString() : '—'}
                </td>
                <td className="px-4 py-3 text-right" onClick={(e) => e.stopPropagation()}>
                  {run.status.toUpperCase() === 'RUNNING' && (
                    <button
                      onClick={() => cancelMutation.mutate(run.run_id)}
                      className="p-1.5 rounded hover:bg-gray-100"
                      title="Cancel"
                    >
                      <XCircle className="w-4 h-4 text-red-400" />
                    </button>
                  )}
                </td>
              </tr>
              {expandedId === run.run_id && (
                <tr key={`${run.run_id}-detail`} className="border-b border-gray-100 bg-gray-50/50">
                  <td colSpan={9} className="px-8 py-4">
                    <div className="space-y-3">
                      {run.error_message && (
                        <div className="text-xs">
                          <span className="font-medium text-red-600">Error: </span>
                          <span className="text-red-500">{run.error_message}</span>
                        </div>
                      )}
                      <div>
                        <span className="text-xs font-medium text-gray-700 block mb-2">Step Statuses</span>
                        <StepStatusBreakdown stepStatuses={run.step_statuses ?? {}} />
                      </div>
                    </div>
                  </td>
                </tr>
              )}
            </>
          ))}
        </tbody>
      </table>
    </div>
  );
}
