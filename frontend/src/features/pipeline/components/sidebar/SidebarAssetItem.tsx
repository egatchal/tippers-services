import { useDraggable } from '@dnd-kit/core';
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
  const { attributes, listeners, setNodeRef, isDragging } = useDraggable({ id: nodeId });

  return (
    <div
      ref={setNodeRef}
      onClick={onClick}
      className={`flex items-center gap-2 px-2 py-1.5 rounded text-sm cursor-pointer hover:bg-gray-100 transition-colors ${
        isDragging ? 'opacity-50' : ''
      }`}
    >
      <button {...listeners} {...attributes} className="cursor-grab p-0.5 text-gray-400 hover:text-gray-600" onClick={(e) => e.stopPropagation()}>
        <GripVertical className="w-3 h-3" />
      </button>
      <span className={`w-2 h-2 rounded-full shrink-0 ${dotColors[status]}`} />
      <span className="truncate text-gray-700">{label}</span>
    </div>
  );
}
