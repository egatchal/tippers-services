import { memo } from 'react';
import { BarChart3 } from 'lucide-react';
import BaseNode from './BaseNode';
import type { PipelineNodeData } from '../../types/nodes';

function FeatureNode({ data }: { data: PipelineNodeData }) {
  return <BaseNode data={data} icon={<BarChart3 className="w-4 h-4 text-cyan-500" />} />;
}

export default memo(FeatureNode);
