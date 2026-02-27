import { memo } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import { Plus } from 'lucide-react';
import type { PlaceholderNodeData, PipelineNodeType } from '../../types/nodes';

const targetLabels: Partial<Record<PipelineNodeType, string>> = {
  index: 'Create Index',
  cv: 'Create Concept Value',
  rule: 'Create Rule',
  lf: 'Create Labeling Function',
  snorkel: 'Create Snorkel Run',
};

function PlaceholderNode({ data }: NodeProps) {
  const phData = data as unknown as PlaceholderNodeData;

  return (
    <div className="relative w-[280px] rounded-lg border-2 border-dashed px-3 py-4 border-gray-300 bg-gray-50">
      {/* Invisible handles — needed so placeholder edges can render, but not connectable */}
      {!phData.hideTopHandle && (
        <Handle type="target" position={Position.Top} isConnectable={false} className="!bg-transparent !border-0 !w-0 !h-0" />
      )}

      <div className="flex items-center justify-center gap-2 text-gray-400">
        <Plus className="w-4 h-4" />
        <span className="text-sm font-medium">
          {phData.label || targetLabels[phData.targetType] || `Create ${phData.targetType}`}
        </span>
      </div>

      {!phData.hideBottomHandle && (
        <Handle type="source" position={Position.Bottom} isConnectable={false} className="!bg-transparent !border-0 !w-0 !h-0" />
      )}
    </div>
  );
}

export default memo(PlaceholderNode);
