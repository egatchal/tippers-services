import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { Pencil, Trash2, Zap, Loader2, CheckCircle, XCircle } from 'lucide-react';
import toast from 'react-hot-toast';
import type { DatabaseConnection } from '../types/connection';
import { deleteConnection, testConnection } from '../api/connections';
import ConfirmDialog from '../../../shared/components/ConfirmDialog';

interface Props {
  connections: DatabaseConnection[];
  onEdit: (conn: DatabaseConnection) => void;
}

export default function ConnectionTable({ connections, onEdit }: Props) {
  const queryClient = useQueryClient();
  const [deleteTarget, setDeleteTarget] = useState<DatabaseConnection | null>(null);
  const [testingId, setTestingId] = useState<number | null>(null);
  const [testResults, setTestResults] = useState<Record<number, 'success' | 'failed'>>({});

  const deleteMutation = useMutation({
    mutationFn: (connId: number) => deleteConnection(connId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['connections'] });
      toast.success('Connection deleted');
      setDeleteTarget(null);
    },
    onError: () => toast.error('Failed to delete connection'),
  });

  const handleTest = async (connId: number) => {
    setTestingId(connId);
    try {
      const result = await testConnection(connId);
      setTestResults((r) => ({ ...r, [connId]: result.status as 'success' | 'failed' }));
      toast[result.status === 'success' ? 'success' : 'error'](result.message);
    } catch {
      setTestResults((r) => ({ ...r, [connId]: 'failed' }));
      toast.error('Connection test failed');
    } finally {
      setTestingId(null);
    }
  };

  return (
    <>
      <div className="overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 bg-gray-50">
              <th className="text-left px-4 py-3 font-medium text-gray-600">Name</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Type</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Host</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Port</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">Database</th>
              <th className="text-left px-4 py-3 font-medium text-gray-600">User</th>
              <th className="text-right px-4 py-3 font-medium text-gray-600">Actions</th>
            </tr>
          </thead>
          <tbody>
            {connections.map((conn) => (
              <tr key={conn.conn_id} className="border-b border-gray-100 hover:bg-gray-50">
                <td className="px-4 py-3 font-medium">{conn.name}</td>
                <td className="px-4 py-3">
                  <span className="px-2 py-0.5 bg-blue-50 text-blue-700 rounded text-xs">{conn.connection_type}</span>
                </td>
                <td className="px-4 py-3 text-gray-600">{conn.host}</td>
                <td className="px-4 py-3 text-gray-600">{conn.port}</td>
                <td className="px-4 py-3 text-gray-600">{conn.database}</td>
                <td className="px-4 py-3 text-gray-600">{conn.user}</td>
                <td className="px-4 py-3 text-right">
                  <div className="flex items-center justify-end gap-1">
                    <button onClick={() => handleTest(conn.conn_id)} disabled={testingId === conn.conn_id} className="p-1.5 rounded hover:bg-gray-100" title="Test connection">
                      {testingId === conn.conn_id ? (
                        <Loader2 className="w-4 h-4 animate-spin text-gray-400" />
                      ) : testResults[conn.conn_id] === 'success' ? (
                        <CheckCircle className="w-4 h-4 text-green-500" />
                      ) : testResults[conn.conn_id] === 'failed' ? (
                        <XCircle className="w-4 h-4 text-red-500" />
                      ) : (
                        <Zap className="w-4 h-4 text-amber-500" />
                      )}
                    </button>
                    <button onClick={() => onEdit(conn)} className="p-1.5 rounded hover:bg-gray-100" title="Edit">
                      <Pencil className="w-4 h-4 text-gray-500" />
                    </button>
                    <button onClick={() => setDeleteTarget(conn)} className="p-1.5 rounded hover:bg-gray-100" title="Delete">
                      <Trash2 className="w-4 h-4 text-red-400" />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <ConfirmDialog
        open={!!deleteTarget}
        title="Delete Connection"
        message={`Delete "${deleteTarget?.name}"? This cannot be undone.`}
        onConfirm={() => deleteTarget && deleteMutation.mutate(deleteTarget.conn_id)}
        onCancel={() => setDeleteTarget(null)}
      />
    </>
  );
}
