import { memo } from 'react';
import { Database } from 'lucide-react';
import BaseNode from './BaseNode';
import type { PipelineNodeData } from '../../types/nodes';

function IndexNode({ data }: { data: PipelineNodeData }) {
  return <BaseNode data={data} icon={<Database className="w-4 h-4 text-blue-500" />} />;
}

export default memo(IndexNode);
