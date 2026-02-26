import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import toast from 'react-hot-toast';
import { createOccupancyDataset } from '../api/occupancy';
import type { Space, OccupancyDataset } from '../types/occupancy';

const INTERVALS = [
  { label: '15 minutes', value: 900 },
  { label: '30 minutes', value: 1800 },
  { label: '1 hour', value: 3600 },
  { label: '2 hours', value: 7200 },
  { label: '4 hours', value: 14400 },
  { label: '8 hours', value: 28800 },
  { label: '1 day', value: 86400 },
];

interface Props {
  space: Space;
  onCreated: (dataset: OccupancyDataset) => void;
}

export default function CreateDatasetForm({ space, onCreated }: Props) {
  const queryClient = useQueryClient();
  const [name, setName] = useState(`${space.space_name ?? 'Space'} Occupancy`);
  const [startTime, setStartTime] = useState('');
  const [endTime, setEndTime] = useState('');
  const [interval, setInterval] = useState(3600);
  const [forceOverwrite, setForceOverwrite] = useState(false);

  const mutation = useMutation({
    mutationFn: () =>
      createOccupancyDataset({
        name,
        root_space_id: space.space_id,
        ...(startTime ? { start_time: new Date(startTime).toISOString() } : {}),
        ...(endTime ? { end_time: new Date(endTime).toISOString() } : {}),
        interval_seconds: interval,
        force_overwrite: forceOverwrite,
      }),
    onSuccess: (dataset) => {
      queryClient.invalidateQueries({ queryKey: ['occupancy-datasets'] });
      toast.success('Dataset creation started');
      onCreated(dataset);
    },
    onError: () => toast.error('Failed to create occupancy dataset'),
  });

  return (
    <div className="bg-white border border-gray-200 rounded-lg p-4">
      <h3 className="text-sm font-semibold mb-3">Create Occupancy Dataset</h3>

      <div className="space-y-3">
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Dataset Name</label>
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            className="w-full px-3 py-1.5 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500"
          />
        </div>

        <div className="grid grid-cols-2 gap-2">
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">Start Time</label>
            <input
              type="datetime-local"
              value={startTime}
              onChange={(e) => setStartTime(e.target.value)}
              className="w-full px-3 py-1.5 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500"
            />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-600 mb-1">End Time</label>
            <input
              type="datetime-local"
              value={endTime}
              onChange={(e) => setEndTime(e.target.value)}
              className="w-full px-3 py-1.5 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500"
            />
          </div>
        </div>
        <p className="text-[11px] text-gray-400 -mt-1.5">Leave blank to use all available data</p>

        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Aggregation Interval</label>
          <select
            value={interval}
            onChange={(e) => setInterval(Number(e.target.value))}
            className="w-full px-3 py-1.5 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500 bg-white"
          >
            {INTERVALS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {opt.label}
              </option>
            ))}
          </select>
        </div>

        <label className="flex items-center gap-2 text-xs text-gray-600 cursor-pointer">
          <input
            type="checkbox"
            checked={forceOverwrite}
            onChange={(e) => setForceOverwrite(e.target.checked)}
            className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
          />
          Force overwrite (discard cached data)
        </label>

        <button
          onClick={() => mutation.mutate()}
          disabled={mutation.isPending || !name.trim()}
          className="w-full px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {mutation.isPending ? 'Creating...' : 'Create Dataset'}
        </button>
      </div>
    </div>
  );
}
