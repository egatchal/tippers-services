import { useState } from 'react';
import { useForm } from 'react-hook-form';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import toast from 'react-hot-toast';
import { X } from 'lucide-react';
import type { PipelineNodeType } from '../../types/nodes';
import { useConceptStore } from '../../stores/conceptStore';
import { usePipelineStore } from '../../stores/pipelineStore';
import { createIndex } from '../../api/indexes';
import { createRule } from '../../api/rules';
import { createFeature } from '../../api/features';
import { createLF } from '../../api/labelingFunctions';
import { createConceptValue } from '../../api/concepts';
import { listConnections } from '../../../connections/api/connections';
import SqlEditor from '../../../../shared/components/SqlEditor';
import CodeEditor from '../../../../shared/components/CodeEditor';
import { NewSnorkelRunForm } from '../panel/SnorkelRunPanel';
import { NewClassifierRunForm } from '../panel/ClassifierRunPanel';

interface Props {
  entityType: PipelineNodeType;
  onClose: () => void;
}

export default function CreateEntityModal({ entityType, onClose }: Props) {
  if (entityType === 'snorkel') return <ModalWrapper title="New Snorkel Run" onClose={onClose}><NewSnorkelRunForm onClose={onClose} /></ModalWrapper>;
  if (entityType === 'classifier') return <ModalWrapper title="New Classifier Run" onClose={onClose}><NewClassifierRunForm onClose={onClose} /></ModalWrapper>;

  return (
    <ModalWrapper title={`Create ${typeLabels[entityType]}`} onClose={onClose}>
      <CreateForm entityType={entityType} onClose={onClose} />
    </ModalWrapper>
  );
}

const typeLabels: Record<PipelineNodeType, string> = {
  index: 'Index', rule: 'Rule', feature: 'Feature', lf: 'Labeling Function', snorkel: 'Snorkel Run', classifier: 'Classifier', cv: 'Concept Value',
};

function ModalWrapper({ title, onClose, children }: { title: string; onClose: () => void; children: React.ReactNode }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-lg shadow-xl w-[520px] max-h-[80vh] overflow-y-auto">
        <div className="flex items-center justify-between px-4 py-3 border-b border-gray-200">
          <h3 className="text-sm font-semibold">{title}</h3>
          <button onClick={onClose} className="p-1 rounded hover:bg-gray-100"><X className="w-4 h-4" /></button>
        </div>
        {children}
      </div>
    </div>
  );
}

function CreateForm({ entityType, onClose }: { entityType: PipelineNodeType; onClose: () => void }) {
  const cId = useConceptStore((s) => s.activeConcept?.c_id);
  const conceptValues = useConceptStore((s) => s.conceptValues);
  const activeLevel = useConceptStore((s) => s.activeLevel);
  const filteredCVs = conceptValues.filter((cv) => cv.level === activeLevel);
  const queryClient = useQueryClient();
  const nodes = usePipelineStore((s) => s.nodes);

  const { data: connections } = useQuery({ queryKey: ['connections'], queryFn: listConnections, enabled: entityType === 'index' });

  const indexes = nodes.filter((n) => n.data.entityType === 'index');
  const rules = nodes.filter((n) => n.data.entityType === 'rule');

  const { register, handleSubmit } = useForm<Record<string, string>>();
  const [selectedCvIds, setSelectedCvIds] = useState<number[]>([]);

  const mutation = useMutation({
    mutationFn: async (values: Record<string, string>) => {
      if (!cId) throw new Error('No concept selected');
      switch (entityType) {
        case 'index':
          return createIndex(cId, { name: values.name, conn_id: Number(values.conn_id), sql_query: values.sql_query });
        case 'rule':
          return createRule(cId, { name: values.name, index_id: Number(values.index_id), sql_query: values.sql_query, index_column: values.index_column || undefined });
        case 'feature':
          return createFeature(cId, { name: values.name, index_id: Number(values.index_id), sql_query: values.sql_query, index_column: values.index_column || undefined });
        case 'lf':
          return createLF(cId, { name: values.name, rule_id: Number(values.rule_id), applicable_cv_ids: selectedCvIds, code: values.code || undefined });
        case 'cv':
          return createConceptValue(cId, { name: values.name, description: values.description || undefined, level: values.level ? Number(values.level) : 1, display_order: values.display_order ? Number(values.display_order) : undefined });
        default:
          throw new Error('Unknown type');
      }
    },
    onSuccess: () => {
      const keyMap: Record<string, string> = { index: 'indexes', rule: 'rules', feature: 'features', lf: 'lfs', snorkel: 'snorkelJobs', classifier: 'classifierJobs', cv: 'conceptValues' };
      const key = keyMap[entityType];
      queryClient.invalidateQueries({ queryKey: [key, cId] });
      toast.success(`${typeLabels[entityType]} created`);
      onClose();
    },
    onError: (err: Error) => toast.error(err.message || 'Failed to create'),
  });

  return (
    <form onSubmit={handleSubmit((v) => mutation.mutate(v))} className="p-4 space-y-3">
      <div>
        <label className="block text-xs font-medium text-gray-700 mb-1">Name</label>
        <input {...register('name', { required: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500" />
      </div>

      {entityType === 'index' && (
        <div>
          <label className="block text-xs font-medium text-gray-700 mb-1">Connection</label>
          <select {...register('conn_id', { required: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md">
            <option value="">Select...</option>
            {connections?.map((c) => <option key={c.conn_id} value={c.conn_id}>{c.name}</option>)}
          </select>
        </div>
      )}

      {(entityType === 'rule' || entityType === 'feature') && (
        <>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Index</label>
            <select {...register('index_id', { required: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md">
              <option value="">Select...</option>
              {indexes.map((n) => <option key={n.id} value={(n.data.entity as { index_id: number }).index_id}>{n.data.label}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Index Column</label>
            <input {...register('index_column')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md" placeholder="Optional" />
          </div>
        </>
      )}

      {entityType === 'lf' && (
        <>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Rule</label>
            <select {...register('rule_id', { required: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md">
              <option value="">Select...</option>
              {rules.map((n) => <option key={n.id} value={(n.data.entity as { r_id: number }).r_id}>{n.data.label}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Applicable Labels</label>
            <div className="space-y-1 border border-gray-200 rounded p-2 max-h-32 overflow-y-auto">
              {filteredCVs.map((cv) => (
                <label key={cv.cv_id} className="flex items-center gap-2 text-sm">
                  <input type="checkbox" checked={selectedCvIds.includes(cv.cv_id)} onChange={(e) => {
                    setSelectedCvIds(e.target.checked ? [...selectedCvIds, cv.cv_id] : selectedCvIds.filter((id) => id !== cv.cv_id));
                  }} />
                  {cv.name}
                </label>
              ))}
            </div>
          </div>
        </>
      )}

      {entityType === 'cv' && (
        <>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Description</label>
            <input {...register('description')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500" placeholder="Optional" />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Level</label>
            <input type="number" defaultValue={1} {...register('level')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500" />
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Display Order</label>
            <input type="number" {...register('display_order')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500" placeholder="Optional" />
          </div>
        </>
      )}

      {(entityType === 'index' || entityType === 'rule' || entityType === 'feature') && (
        <SqlEditor label="SQL Query" {...register('sql_query', { required: true })} />
      )}

      {entityType === 'lf' && (
        <CodeEditor label="Python Code (optional, auto-generates if empty)" {...register('code')} />
      )}

      <div className="flex justify-end gap-3 pt-2">
        <button type="button" onClick={onClose} className="px-4 py-2 text-sm border border-gray-300 rounded-md hover:bg-gray-50">Cancel</button>
        <button type="submit" disabled={mutation.isPending} className="px-4 py-2 text-sm bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50">
          Create
        </button>
      </div>
    </form>
  );
}
