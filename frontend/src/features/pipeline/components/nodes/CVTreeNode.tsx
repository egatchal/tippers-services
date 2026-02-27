import { memo } from 'react';
import { Handle, Position } from '@xyflow/react';
import { Tag, Filter, Sparkles, AlertTriangle } from 'lucide-react';
import type { CVTreeNodeData } from '../../types/nodes';
import { statusBorderClass } from '../../../../shared/utils/statusColors';

function CVTreeNode({ data }: { data: CVTreeNodeData }) {
  const statusColor =
    data.status === 'stale' ? 'bg-orange-500' :
    data.snorkelStatus === 'completed' ? 'bg-green-500' :
    data.snorkelStatus === 'running' ? 'bg-amber-500' :
    'bg-gray-300';

  const borderClass = statusBorderClass[data.status] || 'border-indigo-200';

  return (
    <div
      className={`relative w-[280px] rounded-lg border-2 ${borderClass} bg-indigo-50 shadow-sm px-3 py-3 cursor-pointer hover:border-indigo-400 transition-colors`}
    >
      <Handle type="target" position={Position.Top} style={{ width: 12, height: 12, background: '#9ca3af', border: '2px solid white', borderRadius: '50%', top: -6 }} />

      <div className="flex items-start gap-2.5">
        <div className="shrink-0 mt-0.5">
          <Tag className="w-5 h-5 text-indigo-500" />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-gray-900 truncate">{data.label}</span>
            <span className="text-[10px] text-gray-400 font-mono shrink-0">#{data.entity.cv_id}</span>
            <span className={`w-2.5 h-2.5 rounded-full shrink-0 ${statusColor}`} title={data.status === 'stale' ? 'Pipeline stale — parent updated' : `Snorkel: ${data.snorkelStatus}`} />
            {data.status === 'stale' && <AlertTriangle className="w-3 h-3 text-orange-500 shrink-0" />}
          </div>
          <span className="text-[10px] uppercase font-semibold text-gray-500 tracking-wide">
            Level {data.entity.level}
          </span>

          <div className="flex gap-1.5 mt-1.5">
            {data.ruleCount > 0 && (
              <span className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[10px] bg-white/70 text-gray-600 border border-gray-200">
                <Filter className="w-2.5 h-2.5" />
                <span className="font-medium">{data.ruleCount}</span> rules
              </span>
            )}
            {data.lfCount > 0 && (
              <span className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[10px] bg-white/70 text-gray-600 border border-gray-200">
                <Tag className="w-2.5 h-2.5" />
                <span className="font-medium">{data.lfCount}</span> LFs
              </span>
            )}
            {data.snorkelStatus !== 'none' && (
              <span className="inline-flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[10px] bg-white/70 text-gray-600 border border-gray-200">
                <Sparkles className="w-2.5 h-2.5" />
                {data.snorkelStatus}
              </span>
            )}
          </div>
        </div>
      </div>

      <Handle type="source" position={Position.Bottom} style={{ width: 12, height: 12, background: '#9ca3af', border: '2px solid white', borderRadius: '50%', bottom: -6 }} />
    </div>
  );
}

export default memo(CVTreeNode);
