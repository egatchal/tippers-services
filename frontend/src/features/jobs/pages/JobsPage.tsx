import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { listJobs } from '../api/jobs';
import JobTable from '../components/JobTable';
import ServiceTabs from '../../../shared/components/ServiceTabs';
import LoadingSpinner from '../../../shared/components/LoadingSpinner';
import EmptyState from '../../../shared/components/EmptyState';
import { POLL_INTERVAL } from '../../../shared/utils/constants';

export default function JobsPage() {
  const [activeService, setActiveService] = useState<string | null>(null);

  const { data: jobs, isLoading } = useQuery({
    queryKey: ['jobs', { service: activeService }],
    queryFn: () => listJobs(activeService ? { service: activeService } : undefined),
    refetchInterval: (query) => {
      const data = query.state.data;
      if (!data) return false;
      const hasActive = data.some((j) => {
        const s = j.status.toUpperCase();
        return s === 'RUNNING' || s === 'PENDING';
      });
      return hasActive ? POLL_INTERVAL : false;
    },
  });

  return (
    <div className="h-full overflow-y-auto p-6">
      <div className="max-w-5xl mx-auto">
        <div className="mb-6">
          <h2 className="text-lg font-semibold">Jobs</h2>
          <p className="text-sm text-gray-500">Track and monitor jobs across all services.</p>
        </div>

        <ServiceTabs active={activeService} onChange={setActiveService} />

        <div className="bg-white border border-gray-200 rounded-lg">
          {isLoading ? (
            <LoadingSpinner />
          ) : !jobs?.length ? (
            <EmptyState title="No jobs" message="Jobs will appear here when pipeline runs are submitted." />
          ) : (
            <JobTable jobs={jobs} />
          )}
        </div>
      </div>
    </div>
  );
}
