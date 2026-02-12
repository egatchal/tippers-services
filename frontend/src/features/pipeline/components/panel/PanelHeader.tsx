import { X, GripHorizontal } from 'lucide-react';
import type { PipelineNodeType } from '../../types/nodes';

const typeLabels: Record<PipelineNodeType, string> = {
  index: 'Index',
  rule: 'Rule',
  feature: 'Feature',
  lf: 'Labeling Function',
  snorkel: 'Snorkel Run',
  classifier: 'Classifier',
  cv: 'Concept Value',
};

interface Props {
  entityType: PipelineNodeType;
  label: string;
  onClose: () => void;
  dragHandleProps?: Record<string, unknown>;
}

export default function PanelHeader({ entityType, label, onClose, dragHandleProps }: Props) {
  return (
    <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200 bg-gray-50 rounded-t-lg">
      <div className="flex items-center gap-2">
        <button {...dragHandleProps} className="cursor-grab text-gray-400 hover:text-gray-600">
          <GripHorizontal className="w-4 h-4" />
        </button>
        <div>
          <span className="text-[10px] uppercase font-semibold text-gray-500 tracking-wide">{typeLabels[entityType]}</span>
          <p className="text-sm font-semibold text-gray-800">{label}</p>
        </div>
      </div>
      <button onClick={onClose} className="p-1 rounded hover:bg-gray-200 text-gray-500">
        <X className="w-4 h-4" />
      </button>
    </div>
  );
}
