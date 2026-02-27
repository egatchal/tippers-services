import { GripVertical } from 'lucide-react';
import type { NodeStatus } from '../../../../shared/utils/statusColors';

const dotColors: Record<NodeStatus, string> = {
  materialized: 'bg-green-500',
  running: 'bg-amber-500 animate-pulse',
  pending: 'bg-amber-500 animate-pulse',
  failed: 'bg-red-500',
  stale: 'bg-orange-500',
  default: 'bg-gray-400',
};

interface Props {
  nodeId: string;
  label: string;
  status: NodeStatus;
  onClick: () => void;
}

export default function SidebarAssetItem({ nodeId, label, status, onClick }: Props) {
  const handleDragStart = (e: React.DragEvent) => {
    e.dataTransfer.setData('application/pipeline-node-id', nodeId);
    e.dataTransfer.setData('application/pipeline-node-label', label);
    e.dataTransfer.effectAllowed = 'move';
  };

  return (
    <div
      draggable
      onDragStart={handleDragStart}
      onClick={onClick}
      className="flex items-center gap-2 px-2 py-1.5 rounded text-sm cursor-grab hover:bg-gray-100 transition-colors"
    >
      <GripVertical className="w-3 h-3 text-gray-400 shrink-0" />
      <span className={`w-2 h-2 rounded-full shrink-0 ${dotColors[status]}`} />
      <span className="truncate text-gray-700">{label}</span>
    </div>
  );
}
