import { Check, Loader2, Circle, X, AlertTriangle } from 'lucide-react';
import { type NodeStatus, statusBgClass, statusLabel } from '../utils/statusColors';

const icons: Record<NodeStatus, React.ReactNode> = {
  materialized: <Check className="w-3 h-3" />,
  running: <Loader2 className="w-3 h-3 animate-spin" />,
  pending: <Loader2 className="w-3 h-3 animate-spin" />,
  failed: <X className="w-3 h-3" />,
  stale: <AlertTriangle className="w-3 h-3" />,
  default: <Circle className="w-3 h-3" />,
};

export default function StatusBadge({ status }: { status: NodeStatus }) {
  return (
    <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${statusBgClass[status]}`}>
      {icons[status]}
      {statusLabel[status]}
    </span>
  );
}
