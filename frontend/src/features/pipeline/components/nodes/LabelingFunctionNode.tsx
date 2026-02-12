import { memo } from 'react';
import { Tag } from 'lucide-react';
import BaseNode from './BaseNode';
import type { PipelineNodeData } from '../../types/nodes';

function LabelingFunctionNode({ data }: { data: PipelineNodeData }) {
  return <BaseNode data={data} icon={<Tag className="w-4 h-4 text-amber-500" />} />;
}

export default memo(LabelingFunctionNode);
