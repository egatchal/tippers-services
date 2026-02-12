import { memo } from 'react';
import { Brain } from 'lucide-react';
import BaseNode from './BaseNode';
import type { PipelineNodeData } from '../../types/nodes';

function ClassifierRunNode({ data }: { data: PipelineNodeData }) {
  return <BaseNode data={data} icon={<Brain className="w-4 h-4 text-pink-500" />} />;
}

export default memo(ClassifierRunNode);
