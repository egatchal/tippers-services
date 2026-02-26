import { useQuery } from '@tanstack/react-query';
import { X } from 'lucide-react';
import { previewDataset } from '../api/datasets';
import type { Dataset } from '../types/dataset';
import LoadingSpinner from '../../../shared/components/LoadingSpinner';

interface Props {
  dataset: Dataset;
  onClose: () => void;
}

export default function DatasetPreviewModal({ dataset, onClose }: Props) {
  const { data: preview, isLoading, error } = useQuery({
    queryKey: ['dataset-preview', dataset.dataset_id],
    queryFn: () => previewDataset(dataset.dataset_id),
  });

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-lg shadow-xl w-[90vw] max-w-5xl max-h-[80vh] flex flex-col">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200">
          <div>
            <h3 className="text-lg font-semibold">{dataset.name}</h3>
            <p className="text-xs text-gray-500">{preview ? `${preview.total_rows} total rows` : ''}</p>
          </div>
          <button onClick={onClose} className="p-1.5 rounded hover:bg-gray-100">
            <X className="w-5 h-5 text-gray-500" />
          </button>
        </div>

        <div className="flex-1 overflow-auto p-4">
          {isLoading ? (
            <LoadingSpinner />
          ) : error ? (
            <p className="text-sm text-red-500 text-center p-8">Failed to load preview</p>
          ) : preview && preview.columns.length > 0 ? (
            <table className="min-w-full text-xs font-mono">
              <thead>
                <tr className="border-b border-gray-200 bg-gray-50">
                  {preview.columns.map((col) => (
                    <th key={col} className="text-left px-3 py-2 font-medium text-gray-600 whitespace-nowrap">{col}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {preview.rows.map((row, i) => (
                  <tr key={i} className="border-b border-gray-100">
                    {preview.columns.map((col) => (
                      <td key={col} className="px-3 py-1.5 text-gray-800 whitespace-nowrap max-w-[200px] truncate">
                        {String(row[col] ?? '')}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <p className="text-sm text-gray-500 text-center p-8">No data to preview</p>
          )}
        </div>
      </div>
    </div>
  );
}
