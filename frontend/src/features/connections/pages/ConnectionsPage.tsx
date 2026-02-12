import { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Plus } from 'lucide-react';
import toast from 'react-hot-toast';
import { listConnections, createConnection, updateConnection } from '../api/connections';
import type { DatabaseConnection, DatabaseConnectionCreate } from '../types/connection';
import ConnectionTable from '../components/ConnectionTable';
import ConnectionForm from '../components/ConnectionForm';
import LoadingSpinner from '../../../shared/components/LoadingSpinner';
import EmptyState from '../../../shared/components/EmptyState';

export default function ConnectionsPage() {
  const queryClient = useQueryClient();
  const [formMode, setFormMode] = useState<'closed' | 'create' | 'edit'>('closed');
  const [editTarget, setEditTarget] = useState<DatabaseConnection | null>(null);

  const { data: connections, isLoading } = useQuery({
    queryKey: ['connections'],
    queryFn: listConnections,
  });

  const createMutation = useMutation({
    mutationFn: createConnection,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['connections'] });
      toast.success('Connection created');
      setFormMode('closed');
    },
    onError: () => toast.error('Failed to create connection'),
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, body }: { id: number; body: DatabaseConnectionCreate }) => updateConnection(id, body),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['connections'] });
      toast.success('Connection updated');
      setFormMode('closed');
      setEditTarget(null);
    },
    onError: () => toast.error('Failed to update connection'),
  });

  const handleEdit = (conn: DatabaseConnection) => {
    setEditTarget(conn);
    setFormMode('edit');
  };

  return (
    <div className="h-full overflow-y-auto p-6">
      <div className="max-w-5xl mx-auto">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h2 className="text-lg font-semibold">Database Connections</h2>
            <p className="text-sm text-gray-500">Manage external database connections for indexes and rules.</p>
          </div>
          {formMode === 'closed' && (
            <button onClick={() => setFormMode('create')} className="flex items-center gap-2 px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700">
              <Plus className="w-4 h-4" /> New Connection
            </button>
          )}
        </div>

        {formMode !== 'closed' && (
          <div className="bg-white border border-gray-200 rounded-lg p-6 mb-6">
            <h3 className="text-sm font-semibold mb-4">{formMode === 'create' ? 'New Connection' : 'Edit Connection'}</h3>
            <ConnectionForm
              initial={editTarget ?? undefined}
              onSubmit={(values) => {
                if (formMode === 'edit' && editTarget) {
                  updateMutation.mutate({ id: editTarget.conn_id, body: values });
                } else {
                  createMutation.mutate(values);
                }
              }}
              onCancel={() => { setFormMode('closed'); setEditTarget(null); }}
              loading={createMutation.isPending || updateMutation.isPending}
            />
          </div>
        )}

        <div className="bg-white border border-gray-200 rounded-lg">
          {isLoading ? (
            <LoadingSpinner />
          ) : !connections?.length ? (
            <EmptyState title="No connections" message="Create a database connection to get started." />
          ) : (
            <ConnectionTable connections={connections} onEdit={handleEdit} />
          )}
        </div>
      </div>
    </div>
  );
}
