import { Play } from 'lucide-react';
import type { WorkflowTemplate } from '../types/workflow';

interface Props {
  templates: WorkflowTemplate[];
  onRun: (template: WorkflowTemplate) => void;
}

export default function TemplateTable({ templates, onRun }: Props) {
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b border-gray-200 bg-gray-50">
            <th className="text-left px-4 py-3 font-medium text-gray-600">ID</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Name</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Service</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Steps</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Active</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Created</th>
            <th className="text-right px-4 py-3 font-medium text-gray-600">Actions</th>
          </tr>
        </thead>
        <tbody>
          {templates.map((t) => (
            <tr key={t.template_id} className="border-b border-gray-100 hover:bg-gray-50">
              <td className="px-4 py-3 font-mono text-gray-600">{t.template_id}</td>
              <td className="px-4 py-3 font-medium">{t.name}</td>
              <td className="px-4 py-3">
                <span className="px-2 py-0.5 bg-blue-50 text-blue-700 rounded text-xs">{t.service}</span>
              </td>
              <td className="px-4 py-3 text-gray-600">{t.steps?.steps?.length ?? 0} steps</td>
              <td className="px-4 py-3">
                <span className={`px-2 py-0.5 rounded text-xs font-medium ${t.is_active ? 'bg-green-100 text-green-800' : 'bg-gray-100 text-gray-600'}`}>
                  {t.is_active ? 'Yes' : 'No'}
                </span>
              </td>
              <td className="px-4 py-3 text-gray-600">{new Date(t.created_at).toLocaleDateString()}</td>
              <td className="px-4 py-3 text-right">
                {t.is_active && (
                  <button onClick={() => onRun(t)} className="p-1.5 rounded hover:bg-gray-100" title="Run workflow">
                    <Play className="w-4 h-4 text-green-600" />
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
