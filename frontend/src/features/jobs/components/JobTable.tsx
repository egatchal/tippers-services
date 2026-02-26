import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { XCircle, ChevronDown, ChevronRight } from 'lucide-react';
import toast from 'react-hot-toast';
import type { Job } from '../types/job';
import { cancelJob } from '../api/jobs';
import ServiceStatusBadge from '../../../shared/components/ServiceStatusBadge';

interface Props {
  jobs: Job[];
}

export default function JobTable({ jobs }: Props) {
  const queryClient = useQueryClient();
  const [expandedId, setExpandedId] = useState<number | null>(null);

  const cancelMutation = useMutation({
    mutationFn: (jobId: number) => cancelJob(jobId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['jobs'] });
      toast.success('Job cancelled');
    },
    onError: () => toast.error('Failed to cancel job'),
  });

  const canCancel = (status: string) => {
    const s = status.toUpperCase();
    return s === 'RUNNING' || s === 'PENDING';
  };

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full text-sm">
        <thead>
          <tr className="border-b border-gray-200 bg-gray-50">
            <th className="w-8 px-2 py-3" />
            <th className="text-left px-4 py-3 font-medium text-gray-600">ID</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Service</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Type</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Dagster Run</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Status</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Created</th>
            <th className="text-left px-4 py-3 font-medium text-gray-600">Completed</th>
            <th className="text-right px-4 py-3 font-medium text-gray-600">Actions</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => (
            <>
              <tr
                key={job.job_id}
                className="border-b border-gray-100 hover:bg-gray-50 cursor-pointer"
                onClick={() => setExpandedId(expandedId === job.job_id ? null : job.job_id)}
              >
                <td className="px-2 py-3">
                  {expandedId === job.job_id ? (
                    <ChevronDown className="w-4 h-4 text-gray-400" />
                  ) : (
                    <ChevronRight className="w-4 h-4 text-gray-400" />
                  )}
                </td>
                <td className="px-4 py-3 font-mono text-gray-600">{job.job_id}</td>
                <td className="px-4 py-3">
                  <span className="px-2 py-0.5 bg-blue-50 text-blue-700 rounded text-xs">{job.service}</span>
                </td>
                <td className="px-4 py-3 text-gray-600">{job.job_type}</td>
                <td className="px-4 py-3 font-mono text-xs text-gray-500 max-w-[120px] truncate">
                  {job.dagster_run_id ?? '—'}
                </td>
                <td className="px-4 py-3"><ServiceStatusBadge status={job.status} /></td>
                <td className="px-4 py-3 text-gray-600">{new Date(job.created_at).toLocaleDateString()}</td>
                <td className="px-4 py-3 text-gray-600">
                  {job.completed_at ? new Date(job.completed_at).toLocaleDateString() : '—'}
                </td>
                <td className="px-4 py-3 text-right" onClick={(e) => e.stopPropagation()}>
                  {canCancel(job.status) && (
                    <button
                      onClick={() => cancelMutation.mutate(job.job_id)}
                      className="p-1.5 rounded hover:bg-gray-100"
                      title="Cancel"
                    >
                      <XCircle className="w-4 h-4 text-red-400" />
                    </button>
                  )}
                </td>
              </tr>
              {expandedId === job.job_id && (
                <tr key={`${job.job_id}-detail`} className="border-b border-gray-100 bg-gray-50/50">
                  <td colSpan={9} className="px-8 py-4">
                    <div className="grid grid-cols-2 gap-4 text-xs">
                      {job.error_message && (
                        <div className="col-span-2">
                          <span className="font-medium text-red-600">Error: </span>
                          <span className="text-red-500">{job.error_message}</span>
                        </div>
                      )}
                      {job.config && (
                        <div className="col-span-2">
                          <span className="font-medium text-gray-700">Config:</span>
                          <pre className="mt-1 p-2 bg-gray-100 rounded text-gray-600 overflow-x-auto">
                            {JSON.stringify(job.config, null, 2)}
                          </pre>
                        </div>
                      )}
                      {job.input_dataset_ids?.length ? (
                        <div>
                          <span className="font-medium text-gray-700">Input Datasets: </span>
                          <span className="text-gray-600">{job.input_dataset_ids.join(', ')}</span>
                        </div>
                      ) : null}
                      {job.output_dataset_ids?.length ? (
                        <div>
                          <span className="font-medium text-gray-700">Output Datasets: </span>
                          <span className="text-gray-600">{job.output_dataset_ids.join(', ')}</span>
                        </div>
                      ) : null}
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
