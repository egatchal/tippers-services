import { useState } from 'react';
import { useForm } from 'react-hook-form';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import toast from 'react-hot-toast';
import { X } from 'lucide-react';
import type { PipelineNodeType } from '../../types/nodes';
import type { Index, Rule } from '../../types/entities';
import { useConceptStore } from '../../stores/conceptStore';
import { usePipelineStore } from '../../stores/pipelineStore';
import { createIndex, createDerivedIndex } from '../../api/indexes';
import { createRule } from '../../api/rules';
import { createLF } from '../../api/labelingFunctions';
import { createConceptValue } from '../../api/concepts';
import { listConnections } from '../../../connections/api/connections';
import SqlEditor from '../../../../shared/components/SqlEditor';
import CodeEditor from '../../../../shared/components/CodeEditor';
import { NewSnorkelRunForm } from '../panel/SnorkelRunPanel';

interface Props {
  entityType: PipelineNodeType;
  onClose: () => void;
}

export default function CreateEntityModal({ entityType, onClose }: Props) {
  if (entityType === 'snorkel') return <ModalWrapper title="New Snorkel Run" onClose={onClose}><NewSnorkelRunForm onClose={onClose} /></ModalWrapper>;

  return (
    <ModalWrapper title={`Create ${typeLabels[entityType] ?? entityType}`} onClose={onClose}>
      <CreateForm entityType={entityType} onClose={onClose} />
    </ModalWrapper>
  );
}

const typeLabels: Record<string, string> = {
  index: 'Index', rule: 'Rule', lf: 'Labeling Function', snorkel: 'Snorkel Run', cv: 'Concept Value', cvTree: 'Concept Value',
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
  const selectedCV = useConceptStore((s) => s.selectedCV);
  const allEntities = useConceptStore((s) => s.allEntities);
  const queryClient = useQueryClient();
  const nodes = usePipelineStore((s) => s.nodes);

  // Index mode: root CV → SQL index owned by CV, child CV → derived, no CV → SQL (backward compat)
  const isRootCVSelected = !!selectedCV && selectedCV.parent_cv_id == null;
  const isChildCVSelected = !!selectedCV && selectedCV.parent_cv_id != null;
  const isDerivedMode = entityType === 'index' && isChildCVSelected;
  const isSqlMode = entityType === 'index' && (isRootCVSelected || !selectedCV);

  const { data: connections } = useQuery({ queryKey: ['connections'], queryFn: listConnections, enabled: isSqlMode });

  // --- Inferred context for all entity types ---

  // Indexes on canvas (for rule creation)
  const canvasIndexes = nodes
    .filter((n) => n.data.entityType === 'index')
    .map((n) => n.data.entity as Index);

  // Rules on canvas (for LF creation)
  const canvasRules = nodes
    .filter((n) => n.data.entityType === 'rule')
    .map((n) => n.data.entity as Rule);

  // Applicable CVs for LF: children of selectedCV, or root CVs if no selection
  const applicableCVs = selectedCV
    ? conceptValues.filter((cv) => cv.parent_cv_id === selectedCV.cv_id)
    : conceptValues.filter((cv) => cv.parent_cv_id == null);

  // For derived index: auto-infer parent from CV tree
  const parentSnorkelJobs = (() => {
    if (!selectedCV || !selectedCV.parent_cv_id || !allEntities) return [];
    // Find all indexes belonging to the parent CV (SQL or derived)
    const parentIndexes = allEntities.indexes.filter(
      (i) => i.cv_id === selectedCV.parent_cv_id
    );
    const parentIndexIds = new Set(parentIndexes.map((i) => i.index_id));
    return allEntities.snorkelJobs.filter(
      (sj) => parentIndexIds.has(sj.index_id) && sj.status === 'COMPLETED'
    );
  })();

  const { register, handleSubmit } = useForm<Record<string, string>>();
  const [outputType, setOutputType] = useState<'softmax' | 'argmax' | 'passthrough'>('softmax');
  const [threshold, setThreshold] = useState(0.5);

  const mutation = useMutation({
    mutationFn: async (values: Record<string, string>) => {
      if (!cId) throw new Error('No concept selected');
      switch (entityType) {
        case 'index':
          if (isDerivedMode && selectedCV) {
            return createDerivedIndex(cId, {
              name: values.name,
              cv_id: selectedCV.cv_id,
              parent_snorkel_job_id: parentSnorkelJobs.length === 1
                ? parentSnorkelJobs[0].job_id
                : (values.parent_snorkel_job_id ? Number(values.parent_snorkel_job_id) : undefined),
              label_filter: outputType === 'softmax'
                ? { labels: { [String(selectedCV.cv_id)]: { min_confidence: threshold } } }
                : outputType === 'argmax'
                ? { labels: { [String(selectedCV.cv_id)]: {} } }
                : undefined,
              output_type: outputType,
            });
          }
          return createIndex(cId, {
            name: values.name,
            conn_id: Number(values.conn_id),
            key_column: values.key_column,
            sql_query: values.sql_query,
            ...(isRootCVSelected && selectedCV ? { cv_id: selectedCV.cv_id } : {}),
          });
        case 'rule': {
          // Auto-infer index: use the single index if only one, otherwise from form
          const indexId = canvasIndexes.length === 1
            ? canvasIndexes[0].index_id
            : Number(values.index_id);
          return createRule(cId, { name: values.name, index_id: indexId, sql_query: values.sql_query });
        }
        case 'lf': {
          // Rule is NOT set here — user connects Rule→LF via edge on canvas
          const cvIds = applicableCVs.map((cv) => cv.cv_id);
          return createLF(cId, { name: values.name, applicable_cv_ids: cvIds, code: values.code || undefined });
        }
        case 'cv': {
          // Auto-infer parent and level from tree context
          const parentCvId = selectedCV ? selectedCV.cv_id : undefined;
          const level = selectedCV ? selectedCV.level + 1 : 1;
          return createConceptValue(cId, {
            name: values.name,
            description: values.description || undefined,
            level,
            display_order: values.display_order ? Number(values.display_order) : undefined,
            parent_cv_id: parentCvId,
          });
        }
        default:
          throw new Error('Unknown type');
      }
    },
    onSuccess: () => {
      const keyMap: Record<string, string> = { index: 'indexes', rule: 'rules', lf: 'lfs', snorkel: 'snorkelJobs', cv: 'conceptValues' };
      const key = keyMap[entityType];
      queryClient.invalidateQueries({ queryKey: [key, cId] });
      if (entityType === 'cv') queryClient.invalidateQueries({ queryKey: ['cvTree', cId] });
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

      {/* ── Index at root: SQL only ── */}
      {isSqlMode && (
        <>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Connection</label>
            <select {...register('conn_id', { required: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md">
              <option value="">Select...</option>
              {connections?.map((c) => <option key={c.conn_id} value={c.conn_id}>{c.name}</option>)}
            </select>
          </div>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Key Column</label>
            <input {...register('key_column', { required: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md" placeholder='e.g. "user"' />
            <p className="text-[10px] text-gray-400 mt-0.5">Column containing unique entity identifiers</p>
          </div>
          <SqlEditor label="SQL Query" {...register('sql_query', { required: true })} />
        </>
      )}

      {/* ── Index inside a child CV: derived mode ── */}
      {isDerivedMode && selectedCV && (
        <>
          <ReadOnlyField label="Concept Value" value={selectedCV.name} />

          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Parent Snorkel Job</label>
            {parentSnorkelJobs.length === 1 ? (
              <ReadOnlyField value={`Snorkel #${parentSnorkelJobs[0].job_id}`} />
            ) : parentSnorkelJobs.length > 1 ? (
              <select {...register('parent_snorkel_job_id', { required: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md">
                <option value="">Select snorkel job...</option>
                {parentSnorkelJobs.map((sj) => (
                  <option key={sj.job_id} value={sj.job_id}>Snorkel #{sj.job_id}</option>
                ))}
              </select>
            ) : (
              <p className="text-xs text-amber-600">No completed snorkel job on parent CV. Run snorkel on the parent first.</p>
            )}
          </div>

          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Output Type</label>
            <select
              value={outputType}
              onChange={(e) => setOutputType(e.target.value as 'softmax' | 'argmax' | 'passthrough')}
              className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md"
            >
              <option value="softmax">Softmax (probability threshold)</option>
              <option value="argmax">Argmax (top prediction)</option>
              <option value="passthrough">Pass-through (all entities)</option>
            </select>
            <p className="text-[10px] text-gray-400 mt-0.5">How predictions from the parent Snorkel job are interpreted</p>
          </div>

          {outputType === 'softmax' && (
            <div>
              <label className="block text-xs font-medium text-gray-700 mb-1">Min Confidence</label>
              <input
                type="number"
                step="0.05"
                min="0"
                max="1"
                value={threshold}
                onChange={(e) => setThreshold(Number(e.target.value))}
                className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
              />
              <p className="text-[10px] text-gray-400 mt-0.5">Entities with probability &ge; this value are included</p>
            </div>
          )}
        </>
      )}

      {/* ── Rule: auto-infer index if only one ── */}
      {entityType === 'rule' && (
        <>
          {canvasIndexes.length === 1 ? (
            <ReadOnlyField label="Index" value={canvasIndexes[0].name} />
          ) : canvasIndexes.length > 1 ? (
            <div>
              <label className="block text-xs font-medium text-gray-700 mb-1">Index</label>
              <select {...register('index_id', { required: true })} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md">
                <option value="">Select...</option>
                {canvasIndexes.map((idx) => <option key={idx.index_id} value={idx.index_id}>{idx.name}</option>)}
              </select>
            </div>
          ) : (
            <p className="text-xs text-amber-600">No indexes found. Create one first.</p>
          )}
          <SqlEditor label="SQL Query" {...register('sql_query', { required: true })} />
        </>
      )}

      {/* ── LF: rule set via edge on canvas, labels auto-inferred ── */}
      {entityType === 'lf' && (
        <>
          {applicableCVs.length > 0 && (
            <ReadOnlyField
              label="Applicable Labels"
              value={applicableCVs.map((cv) => cv.name).join(', ')}
              hint={`Auto-assigned — all children of ${selectedCV?.name ?? 'root'}`}
            />
          )}
          <CodeEditor label="Python Code (optional, auto-generates if empty)" {...register('code')} />
        </>
      )}

      {/* ── CV: auto-infer parent + level from tree ── */}
      {entityType === 'cv' && (
        <>
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Description</label>
            <input {...register('description')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500" placeholder="Optional" />
          </div>
          {selectedCV ? (
            <ReadOnlyField label="Parent" value={selectedCV.name} hint={`Level ${selectedCV.level + 1}`} />
          ) : (
            <ReadOnlyField label="Parent" value="None (root)" hint="Level 1" />
          )}
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Display Order</label>
            <input type="number" {...register('display_order')} className="w-full px-3 py-2 text-sm border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500" placeholder="Optional" />
          </div>
        </>
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

function ReadOnlyField({ label, value, hint }: { label?: string; value: string; hint?: string }) {
  return (
    <div>
      {label && <label className="block text-xs font-medium text-gray-700 mb-1">{label}</label>}
      <input
        value={value}
        readOnly
        className="w-full px-3 py-2 text-sm border border-gray-200 rounded-md bg-gray-50 text-gray-500 cursor-not-allowed"
      />
      {hint && <p className="text-[10px] text-gray-400 mt-0.5">{hint}</p>}
    </div>
  );
}
