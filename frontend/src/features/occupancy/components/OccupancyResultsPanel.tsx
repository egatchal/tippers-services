import { useQuery } from '@tanstack/react-query';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { getOccupancyResults } from '../api/occupancy';
import type { OccupancyDataset } from '../types/occupancy';
import LoadingSpinner from '../../../shared/components/LoadingSpinner';
import ServiceStatusBadge from '../../../shared/components/ServiceStatusBadge';
import { POLL_INTERVAL } from '../../../shared/utils/constants';

interface Props {
  dataset: OccupancyDataset;
  spaceId?: number;
}

function formatTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export default function OccupancyResultsPanel({ dataset, spaceId }: Props) {
  const { data: results, isLoading } = useQuery({
    queryKey: ['occupancy-results', dataset.dataset_id, spaceId],
    queryFn: () => getOccupancyResults(dataset.dataset_id, spaceId),
    refetchInterval: (query) => {
      const status = query.state.data?.status;
      return status === 'RUNNING' || status === 'PENDING' ? POLL_INTERVAL : false;
    },
  });

  if (isLoading) return <LoadingSpinner />;
  if (!results) return null;

  // Progress bar while computing
  if (results.status === 'RUNNING' || results.status === 'PENDING') {
    const pct = results.total_chunks > 0
      ? Math.round((results.completed_chunks / results.total_chunks) * 100)
      : 0;

    return (
      <div className="bg-white border border-gray-200 rounded-lg p-4">
        <div className="flex items-center justify-between mb-2">
          <h4 className="text-sm font-semibold">Computing...</h4>
          <ServiceStatusBadge status={results.status} />
        </div>
        <div className="w-full bg-gray-100 rounded-full h-2.5">
          <div
            className="bg-blue-500 h-2.5 rounded-full transition-all duration-300"
            style={{ width: `${pct}%` }}
          />
        </div>
        <p className="text-xs text-gray-500 mt-1">
          {results.completed_chunks} / {results.total_chunks} chunks completed ({pct}%)
        </p>
      </div>
    );
  }

  if (results.status === 'FAILED') {
    return (
      <div className="bg-white border border-red-200 rounded-lg p-4">
        <div className="flex items-center justify-between mb-2">
          <h4 className="text-sm font-semibold text-red-700">Failed</h4>
          <ServiceStatusBadge status="FAILED" />
        </div>
        <p className="text-xs text-red-600">{results.error ?? 'Unknown error'}</p>
      </div>
    );
  }

  // COMPLETED — render chart + table
  const rows = results.rows ?? [];

  if (rows.length === 0) {
    return (
      <div className="bg-white border border-gray-200 rounded-lg p-4">
        <h4 className="text-sm font-semibold mb-1">Results</h4>
        <p className="text-xs text-gray-500">No data rows for this dataset.</p>
      </div>
    );
  }

  const chartData = rows.map((r) => ({
    time: formatTime(r.interval_begin_time),
    connections: r.number_connections,
    rawTime: r.interval_begin_time,
  }));

  return (
    <div className="space-y-4">
      {/* Chart */}
      <div className="bg-white border border-gray-200 rounded-lg p-4">
        <h4 className="text-sm font-semibold mb-3">Connection Count Over Time</h4>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={chartData} margin={{ top: 4, right: 12, bottom: 4, left: 0 }}>
              <defs>
                <linearGradient id="fillGradient" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis
                dataKey="time"
                tick={{ fontSize: 10 }}
                interval="preserveStartEnd"
              />
              <YAxis tick={{ fontSize: 10 }} />
              <Tooltip
                labelFormatter={(_, payload) => {
                  const entry = payload?.[0]?.payload;
                  return entry?.rawTime
                    ? new Date(entry.rawTime).toLocaleString()
                    : '';
                }}
                formatter={(value) => [value ?? 0, 'Connections']}
                contentStyle={{ fontSize: 12 }}
              />
              <Area
                type="monotone"
                dataKey="connections"
                stroke="#3b82f6"
                fill="url(#fillGradient)"
                strokeWidth={1.5}
              />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Data table */}
      <div className="bg-white border border-gray-200 rounded-lg">
        <div className="px-4 py-2 border-b border-gray-100">
          <h4 className="text-sm font-semibold">
            Data Table
            <span className="text-xs font-normal text-gray-400 ml-2">
              {rows.length} row{rows.length !== 1 ? 's' : ''}
            </span>
          </h4>
        </div>
        <div className="max-h-64 overflow-y-auto">
          <table className="w-full text-sm">
            <thead className="sticky top-0 bg-gray-50">
              <tr>
                <th className="text-left px-4 py-2 text-xs font-medium text-gray-500">Time</th>
                <th className="text-right px-4 py-2 text-xs font-medium text-gray-500">Connections</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, i) => (
                <tr key={i} className="border-t border-gray-50 hover:bg-gray-50">
                  <td className="px-4 py-1.5 text-xs text-gray-700">
                    {new Date(row.interval_begin_time).toLocaleString()}
                  </td>
                  <td className="px-4 py-1.5 text-xs text-gray-900 text-right font-mono">
                    {row.number_connections}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
