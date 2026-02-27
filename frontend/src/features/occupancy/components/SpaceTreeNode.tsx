import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ChevronRight, ChevronDown } from 'lucide-react';
import { getSpaceChildren } from '../api/spaces';
import type { Space } from '../types/occupancy';

interface Props {
  space: Space;
  depth: number;
  selectedSpaceId: number | null;
  onSelect: (space: Space) => void;
}

export default function SpaceTreeNode({ space, depth, selectedSpaceId, onSelect }: Props) {
  const [isExpanded, setIsExpanded] = useState(false);
  const isSelected = selectedSpaceId === space.space_id;

  const { data: children, isLoading } = useQuery({
    queryKey: ['space-children', space.space_id],
    queryFn: () => getSpaceChildren(space.space_id),
    enabled: isExpanded,
    staleTime: 5 * 60 * 1000,
  });

  return (
    <div>
      <div
        className={`flex items-center gap-1 py-1.5 pr-2 cursor-pointer text-sm transition-colors ${
          isSelected
            ? 'bg-blue-50 border-l-2 border-blue-500 text-blue-900'
            : 'border-l-2 border-transparent hover:bg-gray-50'
        }`}
        style={{ paddingLeft: depth * 20 + 4 }}
      >
        <button
          onClick={(e) => {
            e.stopPropagation();
            setIsExpanded(!isExpanded);
          }}
          className="p-0.5 rounded hover:bg-gray-200 shrink-0"
        >
          {isLoading ? (
            <span className="w-4 h-4 block border-2 border-gray-300 border-t-gray-600 rounded-full animate-spin" />
          ) : isExpanded ? (
            <ChevronDown className="w-4 h-4 text-gray-500" />
          ) : (
            <ChevronRight className="w-4 h-4 text-gray-400" />
          )}
        </button>

        <span
          onClick={() => onSelect(space)}
          className="truncate flex-1 hover:text-blue-600"
        >
          {space.space_name ?? `Space ${space.space_id}`}
        </span>
      </div>

      {isExpanded && children && children.length > 0 && (
        <div>
          {children.map((child) => (
            <SpaceTreeNode
              key={child.space_id}
              space={child}
              depth={depth + 1}
              selectedSpaceId={selectedSpaceId}
              onSelect={onSelect}
            />
          ))}
        </div>
      )}

      {isExpanded && children && children.length === 0 && (
        <div
          className="text-xs text-gray-400 italic py-1"
          style={{ paddingLeft: (depth + 1) * 20 + 4 }}
        >
          No child spaces
        </div>
      )}
    </div>
  );
}
