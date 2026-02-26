import { useQuery } from '@tanstack/react-query';
import { listRootSpaces } from '../api/spaces';
import type { Space } from '../types/occupancy';
import SpaceTreeNode from './SpaceTreeNode';
import LoadingSpinner from '../../../shared/components/LoadingSpinner';
import EmptyState from '../../../shared/components/EmptyState';

interface Props {
  selectedSpaceId: number | null;
  onSelect: (space: Space) => void;
}

export default function SpaceTree({ selectedSpaceId, onSelect }: Props) {
  const { data: roots, isLoading, error } = useQuery({
    queryKey: ['space-roots'],
    queryFn: listRootSpaces,
    staleTime: 5 * 60 * 1000,
  });

  if (isLoading) return <LoadingSpinner />;

  if (error) {
    return (
      <div className="p-4 text-sm text-red-600">
        Failed to load spaces. Check TIPPERS_DB_URL.
      </div>
    );
  }

  if (!roots?.length) {
    return <EmptyState title="No spaces" message="No root spaces found in the tippers database." />;
  }

  return (
    <div className="py-1">
      {roots.map((space) => (
        <SpaceTreeNode
          key={space.space_id}
          space={space}
          depth={0}
          selectedSpaceId={selectedSpaceId}
          onSelect={onSelect}
        />
      ))}
    </div>
  );
}
