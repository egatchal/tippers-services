import { useEffect, useMemo, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import toast from 'react-hot-toast';
import { ChevronDown, Plus } from 'lucide-react';
import { listConcepts, createConcept } from '../api/concepts';
import { useConceptStore } from '../stores/conceptStore';

export default function ConceptSelector() {
  const { data: concepts } = useQuery({
    queryKey: ['concepts'],
    queryFn: listConcepts,
  });

  const { activeConcept, setActiveConcept, setConcepts, conceptValues, activeLevel, setActiveLevel } = useConceptStore();
  const queryClient = useQueryClient();
  const [showCreate, setShowCreate] = useState(false);
  const [newName, setNewName] = useState('');
  const [newDesc, setNewDesc] = useState('');

  const createMutation = useMutation({
    mutationFn: () => createConcept({ name: newName, description: newDesc || undefined }),
    onSuccess: (created) => {
      queryClient.invalidateQueries({ queryKey: ['concepts'] });
      setActiveConcept(created);
      setShowCreate(false);
      setNewName('');
      setNewDesc('');
      toast.success('Concept created');
    },
    onError: () => toast.error('Failed to create concept'),
  });

  useEffect(() => {
    if (concepts?.length) {
      setConcepts(concepts);
      if (!activeConcept) setActiveConcept(concepts[0]);
    }
  }, [concepts]);

  const levels = useMemo(() => {
    const lvls = [...new Set(conceptValues.map((cv) => cv.level))].sort((a, b) => a - b);
    return lvls.length > 0 ? lvls : [1];
  }, [conceptValues]);

  return (
    <div className="flex items-center gap-2">
      <div className="relative inline-flex items-center">
        <select
          value={activeConcept?.c_id ?? ''}
          onChange={(e) => {
            const c = concepts?.find((c) => c.c_id === Number(e.target.value));
            if (c) setActiveConcept(c);
          }}
          className="appearance-none bg-white border border-gray-300 rounded-md pl-3 pr-8 py-1.5 text-sm font-medium focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          {concepts?.map((c) => (
            <option key={c.c_id} value={c.c_id}>
              {c.name}
            </option>
          ))}
        </select>
        <ChevronDown className="w-4 h-4 text-gray-400 absolute right-2 pointer-events-none" />
      </div>

      <button
        type="button"
        onClick={() => setShowCreate(true)}
        className="p-1.5 rounded-md border border-gray-300 hover:bg-gray-50 text-gray-500 hover:text-gray-700"
        title="New concept"
      >
        <Plus className="w-3.5 h-3.5" />
      </button>

      {levels.length > 1 && (
        <div className="relative inline-flex items-center">
          <select
            value={activeLevel}
            onChange={(e) => setActiveLevel(Number(e.target.value))}
            className="appearance-none bg-white border border-gray-300 rounded-md pl-3 pr-8 py-1.5 text-sm font-medium focus:outline-none focus:ring-2 focus:ring-blue-500"
          >
            {levels.map((lvl) => (
              <option key={lvl} value={lvl}>
                Level {lvl}
              </option>
            ))}
          </select>
          <ChevronDown className="w-4 h-4 text-gray-400 absolute right-2 pointer-events-none" />
        </div>
      )}

      {showCreate && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-white rounded-lg shadow-xl w-[400px]">
            <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200">
              <h3 className="text-sm font-semibold">New Concept</h3>
              <button onClick={() => setShowCreate(false)} className="p-1 rounded hover:bg-gray-100 text-gray-400 hover:text-gray-600">&#x2715;</button>
            </div>
            <form
              onSubmit={(e) => {
                e.preventDefault();
                if (newName.trim()) createMutation.mutate();
              }}
              className="p-4 space-y-3"
            >
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">Name</label>
                <input
                  value={newName}
                  onChange={(e) => setNewName(e.target.value)}
                  className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                  autoFocus
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">Description</label>
                <input
                  value={newDesc}
                  onChange={(e) => setNewDesc(e.target.value)}
                  className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                  placeholder="Optional"
                />
              </div>
              <div className="flex justify-end gap-3 pt-2">
                <button type="button" onClick={() => setShowCreate(false)} className="px-4 py-2 text-sm border border-gray-300 rounded-md hover:bg-gray-50">Cancel</button>
                <button type="submit" disabled={!newName.trim() || createMutation.isPending} className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50">Create</button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
