export type NodeStatus = 'materialized' | 'running' | 'pending' | 'failed' | 'stale' | 'default';

export function getStatusFromEntity(entity: {
  is_materialized?: boolean;
  status?: string;
}): NodeStatus {
  if (entity.status) {
    const s = entity.status.toUpperCase();
    if (s === 'COMPLETED') return 'materialized';
    if (s === 'RUNNING' || s === 'PENDING') return 'running';
    if (s === 'FAILED') return 'failed';
  }
  if (entity.is_materialized) return 'materialized';
  return 'default';
}

export const statusBorderClass: Record<NodeStatus, string> = {
  materialized: 'border-green-500',
  running: 'border-amber-500',
  pending: 'border-amber-500',
  failed: 'border-red-500',
  stale: 'border-orange-500',
  default: 'border-gray-400',
};

export const statusBgClass: Record<NodeStatus, string> = {
  materialized: 'bg-green-100 text-green-800',
  running: 'bg-amber-100 text-amber-800',
  pending: 'bg-amber-100 text-amber-800',
  failed: 'bg-red-100 text-red-800',
  stale: 'bg-orange-100 text-orange-800',
  default: 'bg-gray-100 text-gray-600',
};

export const statusLabel: Record<NodeStatus, string> = {
  materialized: 'Materialized',
  running: 'Running',
  pending: 'Pending',
  failed: 'Failed',
  stale: 'Stale',
  default: 'Not Materialized',
};
