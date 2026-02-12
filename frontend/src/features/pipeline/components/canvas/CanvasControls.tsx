import { useReactFlow } from '@xyflow/react';
import { Maximize2, LayoutGrid } from 'lucide-react';
import { useAutoLayout } from '../../hooks/useAutoLayout';

export default function CanvasControls() {
  const { fitView } = useReactFlow();
  const autoLayout = useAutoLayout();

  return (
    <div className="absolute bottom-4 right-4 flex gap-1 z-10">
      <button
        onClick={() => fitView({ padding: 0.2 })}
        className="p-2 bg-white border border-gray-200 rounded-md shadow-sm hover:bg-gray-50"
        title="Fit view"
      >
        <Maximize2 className="w-4 h-4 text-gray-600" />
      </button>
      <button
        onClick={() => { autoLayout(); setTimeout(() => fitView({ padding: 0.2 }), 50); }}
        className="p-2 bg-white border border-gray-200 rounded-md shadow-sm hover:bg-gray-50"
        title="Auto layout"
      >
        <LayoutGrid className="w-4 h-4 text-gray-600" />
      </button>
    </div>
  );
}
