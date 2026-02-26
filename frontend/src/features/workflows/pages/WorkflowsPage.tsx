import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Plus } from 'lucide-react';
import toast from 'react-hot-toast';
import { listTemplates, createTemplate } from '../api/workflows';
import type { WorkflowTemplate, WorkflowTemplateCreate, WorkflowRun } from '../types/workflow';
import TemplateTable from '../components/TemplateTable';
import TemplateForm from '../components/TemplateForm';
import WorkflowRunTable from '../components/WorkflowRunTable';
import RunWorkflowModal from '../components/RunWorkflowModal';
import ServiceTabs from '../../../shared/components/ServiceTabs';
import LoadingSpinner from '../../../shared/components/LoadingSpinner';
import EmptyState from '../../../shared/components/EmptyState';
import { POLL_INTERVAL } from '../../../shared/utils/constants';
import client from '../../../shared/api/client';

export default function WorkflowsPage() {
  const queryClient = useQueryClient();
  const [formMode, setFormMode] = useState<'closed' | 'create'>('closed');
  const [activeService, setActiveService] = useState<string | null>(null);
  const [subTab, setSubTab] = useState<'templates' | 'runs'>('templates');
  const [runTarget, setRunTarget] = useState<WorkflowTemplate | null>(null);

  const { data: templates, isLoading: templatesLoading } = useQuery({
    queryKey: ['workflow-templates', { service: activeService }],
    queryFn: () => listTemplates(activeService ? { service: activeService } : undefined),
    enabled: subTab === 'templates',
  });

  const { data: runs, isLoading: runsLoading } = useQuery({
    queryKey: ['workflow-runs', { service: activeService }],
    queryFn: async () => {
      // The backend doesn't have a list-runs endpoint directly, but we can
      // fetch runs via individual run lookups or by querying jobs.
      // For now, we'll use a simple approach: list recent workflow runs
      const { data } = await client.get<WorkflowRun[]>('/workflows/runs', {
        params: activeService ? { service: activeService } : undefined,
      });
      return data;
    },
    enabled: subTab === 'runs',
    refetchInterval: (query) => {
      const data = query.state.data;
      if (!data) return false;
      const hasActive = data.some((r) => r.status.toUpperCase() === 'RUNNING');
      return hasActive ? POLL_INTERVAL : false;
    },
  });

  const createMutation = useMutation({
    mutationFn: (body: WorkflowTemplateCreate) => createTemplate(body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['workflow-templates'] });
      toast.success('Template created');
      setFormMode('closed');
    },
    onError: () => toast.error('Failed to create template'),
  });

  return (
    <div className="h-full overflow-y-auto p-6">
      <div className="max-w-5xl mx-auto">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-lg font-semibold">Workflows</h2>
            <p className="text-sm text-gray-500">Define and execute multi-step pipeline workflows.</p>
          </div>
          {formMode === 'closed' && subTab === 'templates' && (
            <button onClick={() => setFormMode('create')} className="flex items-center gap-2 px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700">
              <Plus className="w-4 h-4" /> New Template
            </button>
          )}
        </div>

        <ServiceTabs active={activeService} onChange={setActiveService} />

        {/* Sub-tabs */}
        <div className="flex gap-4 mb-4">
          <button
            onClick={() => setSubTab('templates')}
            className={`text-sm font-medium pb-1 ${subTab === 'templates' ? 'text-blue-600 border-b-2 border-blue-600' : 'text-gray-500 hover:text-gray-700'}`}
          >
            Templates
          </button>
          <button
            onClick={() => setSubTab('runs')}
            className={`text-sm font-medium pb-1 ${subTab === 'runs' ? 'text-blue-600 border-b-2 border-blue-600' : 'text-gray-500 hover:text-gray-700'}`}
          >
            Runs
          </button>
        </div>

        {formMode !== 'closed' && subTab === 'templates' && (
          <div className="bg-white border border-gray-200 rounded-lg p-6 mb-6">
            <h3 className="text-sm font-semibold mb-4">New Workflow Template</h3>
            <TemplateForm
              onSubmit={(values) => createMutation.mutate(values)}
              onCancel={() => setFormMode('closed')}
              loading={createMutation.isPending}
            />
          </div>
        )}

        <div className="bg-white border border-gray-200 rounded-lg">
          {subTab === 'templates' ? (
            templatesLoading ? (
              <LoadingSpinner />
            ) : !templates?.length ? (
              <EmptyState title="No templates" message="Create a workflow template to get started." />
            ) : (
              <TemplateTable templates={templates} onRun={setRunTarget} />
            )
          ) : (
            runsLoading ? (
              <LoadingSpinner />
            ) : !runs?.length ? (
              <EmptyState title="No runs" message="Execute a workflow template to see runs here." />
            ) : (
              <WorkflowRunTable runs={runs} />
            )
          )}
        </div>
      </div>

      {runTarget && (
        <RunWorkflowModal template={runTarget} onClose={() => setRunTarget(null)} />
      )}
    </div>
  );
}
