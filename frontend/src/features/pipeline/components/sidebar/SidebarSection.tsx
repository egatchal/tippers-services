import { useState } from 'react';
import { ChevronDown, ChevronRight, Plus } from 'lucide-react';

interface Props {
  title: string;
  count: number;
  children: React.ReactNode;
  onCreateClick?: () => void;
}

export default function SidebarSection({ title, count, children, onCreateClick }: Props) {
  const [open, setOpen] = useState(true);

  return (
    <div className="border-b border-gray-100 last:border-b-0">
      <div
        className="flex items-center gap-1.5 cursor-pointer px-3 py-2 hover:bg-gray-50"
        onClick={() => setOpen(!open)}
      >
        {open ? <ChevronDown className="w-3.5 h-3.5 text-gray-400" /> : <ChevronRight className="w-3.5 h-3.5 text-gray-400" />}
        <span className="text-xs font-semibold text-gray-600 uppercase tracking-wide">{title}</span>
        <span className="text-[10px] text-gray-400 bg-gray-100 px-1.5 rounded-full">{count}</span>
        {onCreateClick && (
          <button
            className="ml-auto p-0.5 rounded hover:bg-indigo-100 text-gray-400 hover:text-indigo-600 transition-colors"
            onClick={(e) => { e.stopPropagation(); onCreateClick(); }}
            title="Create new"
          >
            <Plus className="w-3.5 h-3.5" />
          </button>
        )}
      </div>
      {open && <div className="px-2 pb-2">{children}</div>}
    </div>
  );
}
