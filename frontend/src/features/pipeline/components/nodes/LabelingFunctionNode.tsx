import { memo, useCallback } from 'react';
import { Tag } from 'lucide-react';
import { useQueryClient } from '@tanstack/react-query';
import toast from 'react-hot-toast';
import BaseNode from './BaseNode';
import type { PipelineNodeData } from '../../types/nodes';
import type { LabelingFunction } from '../../types/entities';
import { toggleLF } from '../../api/labelingFunctions';
import { useConceptStore } from '../../stores/conceptStore';

function LabelingFunctionNode({ data }: { data: PipelineNodeData }) {
  const cId = useConceptStore((s) => s.activeConcept?.c_id);
  const queryClient = useQueryClient();
  const lf = data.entity as LabelingFunction;

  const handleToggle = useCallback((e: React.MouseEvent) => {
    e.stopPropagation();
    if (!cId) return;
    toggleLF(cId, lf.lf_id)
      .then(() => {
        queryClient.invalidateQueries({ queryKey: ['lfs', cId] });
        toast.success(lf.is_active ? 'LF deactivated' : 'LF activated');
      })
      .catch(() => toast.error('Failed to toggle LF'));
  }, [cId, lf.lf_id, lf.is_active, queryClient]);

  return (
    <div>
      <BaseNode data={data} icon={<Tag className="w-4 h-4 text-amber-500" />} />
      <div className="absolute bottom-1 right-3 z-10">
        <button
          onClick={handleToggle}
          className={`w-8 h-4 rounded-full flex items-center transition-colors ${lf.is_active ? 'bg-green-500 justify-end' : 'bg-gray-300 justify-start'}`}
          title={lf.is_active ? 'Active — click to deactivate' : 'Inactive — click to activate'}
        >
          <span className="w-3 h-3 bg-white rounded-full shadow mx-0.5" />
        </button>
      </div>
    </div>
  );
}

export default memo(LabelingFunctionNode);
