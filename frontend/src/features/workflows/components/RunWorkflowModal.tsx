import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { X, Play, Loader2 } from 'lucide-react';
import toast from 'react-hot-toast';
import type { WorkflowTemplate } from '../types/workflow';
import { runWorkflow } from '../api/workflows';
import CodeEditor from '../../../shared/components/CodeEditor';

interface Props {
  template: WorkflowTemplate;
  onClose: () => void;
}

export default function RunWorkflowModal({ template, onClose }: Props) {
  const queryClient = useQueryClient();

  // Extract placeholder keys from steps config_template
  const placeholders = extractPlaceholders(template.steps);
  const defaultParams = Object.fromEntries(placeholders.map((p) => [p, '']));
  const [paramsJson, setParamsJson] = useState(JSON.stringify(defaultParams, null, 2));

  const runMutation = useMutation({
    mutationFn: (params: Record<string, unknown>) => runWorkflow({ template_id: template.template_id, params }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['workflow-runs'] });
      toast.success('Workflow started');
      onClose();
    },
    onError: () => toast.error('Failed to start workflow'),
  });

  const handleRun = () => {
    try {
      const params = JSON.parse(paramsJson);
      runMutation.mutate(params);
    } catch {
      toast.error('Invalid JSON params');
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-lg shadow-xl w-[600px] max-h-[80vh] flex flex-col">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
          <div>
            <h3 className="text-lg font-semibold">Run Workflow</h3>
            <p className="text-xs text-gray-500">{template.name} &middot; {template.service}</p>
          </div>
          <button onClick={onClose} className="p-1.5 rounded hover:bg-gray-100">
            <X className="w-5 h-5 text-gray-500" />
          </button>
        </div>

        <div className="flex-1 overflow-auto p-6 space-y-4">
          {template.description && (
            <p className="text-sm text-gray-600">{template.description}</p>
          )}

          <div className="text-xs text-gray-500">
            <span className="font-medium">Steps: </span>
            {template.steps?.steps?.map((s) => s.key).join(' → ') ?? 'None'}
          </div>

          <div>
            <CodeEditor
              label="Runtime Parameters (JSON)"
              value={paramsJson}
              onChange={(e) => setParamsJson(e.target.value)}
            />
            {placeholders.length > 0 && (
              <p className="text-xs text-gray-400 mt-1">
                Placeholders: {placeholders.map((p) => `{{${p}}}`).join(', ')}
              </p>
            )}
          </div>

          <button
            onClick={handleRun}
            disabled={runMutation.isPending}
            className="flex items-center gap-2 px-4 py-2 text-sm bg-green-600 text-white rounded-md hover:bg-green-700 disabled:opacity-50"
          >
            {runMutation.isPending ? <Loader2 className="w-4 h-4 animate-spin" /> : <Play className="w-4 h-4" />}
            {runMutation.isPending ? 'Starting...' : 'Start Workflow'}
          </button>
        </div>
      </div>
    </div>
  );
}

function extractPlaceholders(steps: WorkflowTemplate['steps']): string[] {
  const text = JSON.stringify(steps);
  const matches = text.match(/\{\{(\w+)\}\}/g) ?? [];
  const keys = matches.map((m) => m.replace(/\{\{|\}\}/g, ''));
  return [...new Set(keys)];
}
