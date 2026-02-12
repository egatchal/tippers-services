import { useState } from 'react';
import { ChevronDown, ChevronRight, Plus } from 'lucide-react';

interface Props {
  title: string;
  count: number;
  onAdd?: () => void;
  children: React.ReactNode;
}

export default function SidebarSection({ title, count, onAdd, children }: Props) {
  const [open, setOpen] = useState(true);

  return (
    <div className="border-b border-gray-100 last:border-b-0">
      <div className="flex items-center justify-between px-3 py-2 cursor-pointer hover:bg-gray-50" onClick={() => setOpen(!open)}>
        <div className="flex items-center gap-1.5">
          {open ? <ChevronDown className="w-3.5 h-3.5 text-gray-400" /> : <ChevronRight className="w-3.5 h-3.5 text-gray-400" />}
          <span className="text-xs font-semibold text-gray-600 uppercase tracking-wide">{title}</span>
          <span className="text-[10px] text-gray-400 bg-gray-100 px-1.5 rounded-full">{count}</span>
        </div>
        {onAdd && (
          <button
            onClick={(e) => { e.stopPropagation(); onAdd(); }}
            className="p-0.5 rounded hover:bg-gray-200 text-gray-400 hover:text-gray-600"
          >
            <Plus className="w-3.5 h-3.5" />
          </button>
        )}
      </div>
      {open && <div className="px-2 pb-2">{children}</div>}
    </div>
  );
}
