import { useQuery } from '@tanstack/react-query';
import { listOccupancyDatasets } from '../api/occupancy';
import type { OccupancyDataset } from '../types/occupancy';
import ServiceStatusBadge from '../../../shared/components/ServiceStatusBadge';
import LoadingSpinner from '../../../shared/components/LoadingSpinner';
import { POLL_INTERVAL } from '../../../shared/utils/constants';

const INTERVAL_LABELS: Record<number, string> = {
  900: '15m',
  1800: '30m',
  3600: '1hr',
  7200: '2hr',
  14400: '4hr',
  28800: '8hr',
  86400: '1d',
};

interface Props {
  rootSpaceId: number;
  selectedDatasetId: number | null;
  onSelectDataset: (dataset: OccupancyDataset) => void;
}

export default function OccupancyDatasetList({ rootSpaceId, selectedDatasetId, onSelectDataset }: Props) {
  const { data: datasets = [], isLoading } = useQuery({
    queryKey: ['occupancy-datasets', rootSpaceId],
    queryFn: () => listOccupancyDatasets(rootSpaceId),
    refetchInterval: (query) => {
      const hasRunning = query.state.data?.some(
        (d) => d.status.toUpperCase() === 'RUNNING'
      );
      return hasRunning ? POLL_INTERVAL : false;
    },
  });

  if (isLoading) return <LoadingSpinner className="py-4" />;

  if (datasets.length === 0) {
    return (
      <p className="text-xs text-gray-400 italic py-2">
        No datasets for this space yet.
      </p>
    );
  }

  return (
    <div className="space-y-1">
      {datasets.map((ds) => (
        <button
          key={ds.dataset_id}
          onClick={() => onSelectDataset(ds)}
          className={`w-full text-left px-3 py-2 rounded-md text-sm flex items-center gap-2 transition-colors ${
            selectedDatasetId === ds.dataset_id
              ? 'bg-blue-50 ring-1 ring-blue-200'
              : 'hover:bg-gray-50'
          }`}
        >
          <span className="truncate flex-1 font-medium text-gray-800">{ds.name}</span>
          <ServiceStatusBadge status={ds.status} />
          <span className="text-xs text-gray-400 shrink-0">
            {INTERVAL_LABELS[ds.interval_seconds] ?? `${ds.interval_seconds}s`}
          </span>
        </button>
      ))}
    </div>
  );
}
