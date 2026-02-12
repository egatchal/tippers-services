import { memo } from 'react';
import { Handle, Position } from '@xyflow/react';
import { Check, Loader2, Circle, X, AlertTriangle } from 'lucide-react';
import type { PipelineNodeData, PipelineNodeType } from '../../types/nodes';
import { statusBorderClass, type NodeStatus } from '../../../../shared/utils/statusColors';

const typeColors: Record<PipelineNodeType, string> = {
  index: 'bg-blue-50',
  rule: 'bg-purple-50',
  feature: 'bg-cyan-50',
  lf: 'bg-amber-50',
  snorkel: 'bg-emerald-50',
  classifier: 'bg-pink-50',
  cv: 'bg-indigo-50',
};

const typeLabels: Record<PipelineNodeType, string> = {
  index: 'Index',
  rule: 'Rule',
  feature: 'Feature',
  lf: 'Labeling Function',
  snorkel: 'Snorkel Run',
  classifier: 'Classifier',
  cv: 'Concept Value',
};

const statusIcons: Record<NodeStatus, React.ReactNode> = {
  materialized: <Check className="w-3.5 h-3.5 text-green-600" />,
  running: <Loader2 className="w-3.5 h-3.5 text-amber-600 animate-spin" />,
  pending: <Loader2 className="w-3.5 h-3.5 text-amber-600 animate-spin" />,
  failed: <X className="w-3.5 h-3.5 text-red-600" />,
  stale: <AlertTriangle className="w-3.5 h-3.5 text-orange-600" />,
  default: <Circle className="w-3.5 h-3.5 text-gray-400" />,
};

interface BaseNodeProps {
  data: PipelineNodeData;
  icon: React.ReactNode;
}

function BaseNode({ data, icon }: BaseNodeProps) {
  return (
    <div className={`relative w-[280px] rounded-lg border-2 shadow-sm ${statusBorderClass[data.status]} ${typeColors[data.entityType]} px-3 py-2`}>
      <Handle type="target" position={Position.Top} className="!bg-gray-400 !w-2 !h-2" />

      <div className="flex items-center gap-2">
        <div className="shrink-0">{icon}</div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-1.5">
            <span className="text-[10px] uppercase font-semibold text-gray-500 tracking-wide">
              {typeLabels[data.entityType]}
            </span>
            {statusIcons[data.status]}
          </div>
          <p className="text-sm font-medium text-gray-900 truncate">{data.label}</p>
          {data.metrics && data.metrics.length > 0 ? (
            <div className="flex gap-1 mt-0.5 flex-wrap">
              {data.metrics.map((m) => (
                <span
                  key={m.label}
                  className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[10px] bg-white/70 text-gray-600 border border-gray-200"
                >
                  <span className="font-medium text-gray-800">{m.value}</span>
                  <span className="text-gray-400">{m.label}</span>
                </span>
              ))}
            </div>
          ) : data.summary ? (
            <p className="text-[11px] text-gray-500 truncate">{data.summary}</p>
          ) : null}
        </div>
      </div>

      <Handle type="source" position={Position.Bottom} className="!bg-gray-400 !w-2 !h-2" />
    </div>
  );
}

export default memo(BaseNode);
