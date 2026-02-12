import { memo } from 'react';
import { Filter } from 'lucide-react';
import BaseNode from './BaseNode';
import type { PipelineNodeData } from '../../types/nodes';

function RuleNode({ data }: { data: PipelineNodeData }) {
  return <BaseNode data={data} icon={<Filter className="w-4 h-4 text-purple-500" />} />;
}

export default memo(RuleNode);
