import { useLocation } from 'react-router-dom';
import { RefreshCw } from 'lucide-react';
import { useQueryClient } from '@tanstack/react-query';

const breadcrumbMap: Record<string, string> = {
  '/pipeline': 'Pipeline Builder',
  '/connections': 'Database Connections',
};

export default function AppHeader() {
  const location = useLocation();
  const queryClient = useQueryClient();
  const label = breadcrumbMap[location.pathname] ?? 'Tippers Services';

  return (
    <header className="h-14 border-b border-gray-200 bg-white flex items-center justify-between px-6 shrink-0">
      <h1 className="text-sm font-semibold text-gray-800">{label}</h1>
      <button
        onClick={() => queryClient.invalidateQueries()}
        className="p-2 rounded-md hover:bg-gray-100 text-gray-500 transition-colors"
        title="Refresh all data"
      >
        <RefreshCw className="w-4 h-4" />
      </button>
    </header>
  );
}
