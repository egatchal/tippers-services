import { memo } from 'react';
import { Sparkles } from 'lucide-react';
import BaseNode from './BaseNode';
import type { PipelineNodeData } from '../../types/nodes';

function SnorkelRunNode({ data }: { data: PipelineNodeData }) {
  return <BaseNode data={data} icon={<Sparkles className="w-4 h-4 text-emerald-500" />} />;
}

export default memo(SnorkelRunNode);
