import { useCallback, useMemo, useRef } from 'react';
import {
  ReactFlow,
  Background,
  BackgroundVariant,
  useReactFlow,
  type OnNodesChange,
  type OnEdgesChange,
  type EdgeChange,
  type Connection,
  applyNodeChanges,
  applyEdgeChanges,
} from '@xyflow/react';
import toast from 'react-hot-toast';
import { useQueryClient } from '@tanstack/react-query';
import { ChevronLeft, GitBranch } from 'lucide-react';
import { usePipelineStore } from '../../stores/pipelineStore';
import { useConceptStore } from '../../stores/conceptStore';
import type { AllEntities } from '../../stores/conceptStore';
import type { PipelineNodeData } from '../../types/nodes';
import { isValidConnection, parseNodeId } from '../../utils/connectionRules';
import { updateLF } from '../../api/labelingFunctions';
import { updateRule } from '../../api/rules';
import { updateIndex } from '../../api/indexes';
import { updateSnorkelJob } from '../../api/snorkel';
import { updateConceptValue } from '../../api/concepts';
import { createSingleNode } from '../../utils/nodeFactory';
import type { LabelingFunction, ConceptValue, Rule, SnorkelJob, Index } from '../../types/entities';
import IndexNode from '../nodes/IndexNode';
import RuleNode from '../nodes/RuleNode';
import LabelingFunctionNode from '../nodes/LabelingFunctionNode';
import SnorkelRunNode from '../nodes/SnorkelRunNode';
import ConceptValueNode from '../nodes/ConceptValueNode';
import CVTreeNode from '../nodes/CVTreeNode';
import PlaceholderNode from '../nodes/PlaceholderNode';
import PipelineEdge from '../edges/PipelineEdge';
import ConnectionLine from '../edges/ConnectionLine';
import CanvasControls from './CanvasControls';

const nodeTypes = {
  indexNode: IndexNode,
  ruleNode: RuleNode,
  lfNode: LabelingFunctionNode,
  snorkelNode: SnorkelRunNode,
  cvNode: ConceptValueNode,
  cvTreeNode: CVTreeNode,
  placeholderNode: PlaceholderNode,
};

const edgeTypes = {
  pipelineEdge: PipelineEdge,
};

interface PipelineCanvasProps {
  allEntities: AllEntities | null;
}

export default function PipelineCanvas({ allEntities }: PipelineCanvasProps) {
  const { screenToFlowPosition } = useReactFlow();
  const { nodes, edges, setNodes, setEdges, selectNode, addNode, dismissNode } = usePipelineStore();
  const cId = useConceptStore((s) => s.activeConcept?.c_id);
  const canvasView = useConceptStore((s) => s.canvasView);
  const selectedCV = useConceptStore((s) => s.selectedCV);
  const setCanvasView = useConceptStore((s) => s.setCanvasView);
  const setSelectedCV = useConceptStore((s) => s.setSelectedCV);
  const queryClient = useQueryClient();

  // Both tree and pipeline views now use store nodes directly (built by PipelinePage)

  const getLFEntity = useCallback(
    (lfNodeId: string): LabelingFunction | null => {
      const node = usePipelineStore.getState().nodes.find((n) => n.id === lfNodeId);
      if (!node || node.data.entityType !== 'lf') return null;
      return node.data.entity as LabelingFunction;
    },
    [],
  );

  const getRuleEntity = useCallback(
    (ruleNodeId: string): Rule | null => {
      const node = usePipelineStore.getState().nodes.find((n) => n.id === ruleNodeId);
      if (!node || node.data.entityType !== 'rule') return null;
      return node.data.entity as Rule;
    },
    [],
  );

  const getIndexEntity = useCallback(
    (indexNodeId: string): Index | null => {
      const node = usePipelineStore.getState().nodes.find((n) => n.id === indexNodeId);
      if (!node || node.data.entityType !== 'index') return null;
      return node.data.entity as Index;
    },
    [],
  );

  const getSnorkelEntity = useCallback(
    (snorkelNodeId: string): SnorkelJob | null => {
      const node = usePipelineStore.getState().nodes.find((n) => n.id === snorkelNodeId);
      if (!node || node.data.entityType !== 'snorkel') return null;
      return node.data.entity as SnorkelJob;
    },
    [],
  );

  const getCVEntity = useCallback(
    (nodeId: string): ConceptValue | null => {
      const node = usePipelineStore.getState().nodes.find((n) => n.id === nodeId);
      if (!node) return null;
      if (node.data.entityType !== 'cv' && node.data.entityType !== 'cvTree') return null;
      return node.data.entity as ConceptValue;
    },
    [],
  );

  // Native HTML5 drag-over: allow drop on canvas
  const onDragOver = useCallback((event: React.DragEvent) => {
    event.preventDefault();
    event.dataTransfer.dropEffect = 'move';
  }, []);

  // Native HTML5 drop: create node at drop position
  const onDrop = useCallback(
    (event: React.DragEvent) => {
      event.preventDefault();
      const nodeId = event.dataTransfer.getData('application/pipeline-node-id');
      const label = event.dataTransfer.getData('application/pipeline-node-label');
      if (!nodeId || !allEntities) return;

      const position = screenToFlowPosition({ x: event.clientX, y: event.clientY });
      const node = createSingleNode(nodeId, allEntities, position);
      if (!node) return;

      addNode(node);
      toast.success(`Added "${label}" to canvas`);
    },
    [screenToFlowPosition, addNode, allEntities],
  );

  // Guard: when true, edge removals from onEdgesChange are dismiss-triggered (skip backend sync)
  const dismissingRef = useRef(false);

  // Always read latest state from store to avoid stale closure overwrites
  const onNodesChange: OnNodesChange = useCallback(
    (changes) => {
      // In tree view, only allow position/dimension/select changes (no delete)
      const filtered = canvasView === 'tree'
        ? changes.filter((c) => c.type === 'position' || c.type === 'dimensions' || c.type === 'select')
        : changes;
      if (filtered.length === 0) return;

      // Intercept remove changes: dismiss to sidebar instead of hard-deleting
      const removes = filtered.filter((c) => c.type === 'remove');
      const rest = filtered.filter((c) => c.type !== 'remove');

      if (removes.length > 0) {
        dismissingRef.current = true;
        for (const r of removes) {
          dismissNode(r.id);
        }
        // Reset after microtask so the onEdgesChange triggered by React Flow sees the flag
        Promise.resolve().then(() => { dismissingRef.current = false; });
      }

      if (rest.length > 0) {
        const currentNodes = usePipelineStore.getState().nodes;
        setNodes(applyNodeChanges(rest, currentNodes) as typeof nodes);
      }
    },
    [setNodes, dismissNode, canvasView],
  );

  const onEdgesChange: OnEdgesChange = useCallback(
    (changes: EdgeChange[]) => {
      const currentEdges = usePipelineStore.getState().edges;
      // Intercept edge removals to update backend (skip if triggered by node dismiss)
      const removals = changes.filter((c) => c.type === 'remove');
      if (!dismissingRef.current) for (const removal of removals) {
        const edge = currentEdges.find((e) => e.id === removal.id);
        if (!edge || !cId) continue;
        const source = parseNodeId(edge.source);
        const target = parseNodeId(edge.target);

        // LF->Snorkel: remove LF from snorkel job's lf_ids
        if (source.type === 'lf' && target.type === 'snorkel') {
          const job = getSnorkelEntity(edge.target);
          if (job) {
            const newLfIds = job.lf_ids.filter((id) => id !== source.id);
            updateSnorkelJob(cId, job.job_id, { lf_ids: newLfIds })
              .then(() => {
                queryClient.invalidateQueries({ queryKey: ['snorkelJobs', cId] });
                toast.success(`Removed LF from Snorkel #${job.job_id}`);
              })
              .catch(() => toast.error('Failed to update snorkel job'));
          }
        }

        // CV->LF: remove from applicable_cv_ids
        if (source.type === 'cv' && target.type === 'lf') {
          const lf = getLFEntity(edge.target);
          if (lf) {
            const newCvIds = lf.applicable_cv_ids.filter((id) => id !== source.id);
            updateLF(cId, lf.lf_id, { applicable_cv_ids: newCvIds })
              .then(() => {
                queryClient.invalidateQueries({ queryKey: ['lfs', cId] });
                toast.success(`Removed label from ${lf.name}`);
              })
              .catch(() => toast.error('Failed to update labels'));
          }
        }

        // CV->CV or cvTree->cvTree: clear parent_cv_id on the child
        const isCVtoCV = (source.type === 'cv' && target.type === 'cv') ||
                          (source.type === 'cvTree' && target.type === 'cvTree');
        if (isCVtoCV) {
          const childCV = getCVEntity(edge.target);
          if (childCV) {
            updateConceptValue(cId, childCV.cv_id, {
              parent_cv_id: null,
              level: 1,
            })
              .then(() => {
                queryClient.invalidateQueries({ queryKey: ['conceptValues', cId] });
                queryClient.invalidateQueries({ queryKey: ['cvTree', cId] });
                toast.success(`"${childCV.name}" is now a root concept value`);
              })
              .catch(() => toast.error('Failed to update parent'));
          }
        }
      }
      setEdges(applyEdgeChanges(changes, currentEdges) as typeof edges);
    },
    [setEdges, cId, getLFEntity, getSnorkelEntity, getCVEntity, queryClient, canvasView],
  );

  const onConnect = useCallback(
    (connection: Connection) => {
      if (!connection.source || !connection.target) return;
      const source = parseNodeId(connection.source);
      const target = parseNodeId(connection.target);

      if (!isValidConnection(source.type, target.type)) {
        toast.error(`Cannot connect ${source.type} to ${target.type}`);
        return;
      }

      // Index->Rule: update index_id on the rule
      if (source.type === 'index' && target.type === 'rule' && cId) {
        const rule = getRuleEntity(connection.target);
        if (rule) {
          if (rule.index_id === source.id) {
            toast.error('Rule already uses this index');
            return;
          }
          updateRule(cId, rule.r_id, { index_id: source.id })
            .then(() => {
              queryClient.invalidateQueries({ queryKey: ['rules', cId] });
              toast.success(`Linked "${rule.name}" to new index`);
            })
            .catch(() => toast.error('Failed to update rule index'));
        }
      }

      // Rule->LF: update rule_id on the LF
      if (source.type === 'rule' && target.type === 'lf' && cId) {
        const lf = getLFEntity(connection.target);
        if (lf) {
          if (lf.rule_id === source.id) {
            toast.error('LF already uses this rule');
            return;
          }
          updateLF(cId, lf.lf_id, { rule_id: source.id })
            .then(() => {
              queryClient.invalidateQueries({ queryKey: ['lfs', cId] });
              toast.success(`Linked "${lf.name}" to new rule`);
            })
            .catch(() => toast.error('Failed to update LF rule'));
        }
      }

      // LF->Snorkel: add LF to snorkel job's lf_ids
      if (source.type === 'lf' && target.type === 'snorkel' && cId) {
        const job = getSnorkelEntity(connection.target);
        if (job) {
          if (job.lf_ids.includes(source.id)) {
            toast.error('LF already in this snorkel run');
            return;
          }
          const newLfIds = [...job.lf_ids, source.id];
          updateSnorkelJob(cId, job.job_id, { lf_ids: newLfIds })
            .then(() => {
              queryClient.invalidateQueries({ queryKey: ['snorkelJobs', cId] });
              toast.success(`Added LF to Snorkel #${job.job_id}`);
            })
            .catch(() => toast.error('Failed to update snorkel job'));
        }
      }

      // Snorkel->Index: update parent_snorkel_job_id on the derived index
      if (source.type === 'snorkel' && target.type === 'index' && cId) {
        const idx = getIndexEntity(connection.target);
        if (idx) {
          if (idx.parent_snorkel_job_id === source.id) {
            toast.error('Index already uses this snorkel job');
            return;
          }
          updateIndex(cId, idx.index_id, { parent_snorkel_job_id: source.id })
            .then(() => {
              queryClient.invalidateQueries({ queryKey: ['indexes', cId] });
              toast.success(`Linked "${idx.name}" to Snorkel #${source.id}`);
            })
            .catch(() => toast.error('Failed to update derived index'));
        }
      }

      // CV->LF: update applicable_cv_ids on the backend
      if (source.type === 'cv' && target.type === 'lf' && cId) {
        const lf = getLFEntity(connection.target);
        if (lf) {
          if (lf.applicable_cv_ids.includes(source.id)) {
            toast.error('Label already assigned');
            return;
          }
          const newCvIds = [...lf.applicable_cv_ids, source.id];
          updateLF(cId, lf.lf_id, { applicable_cv_ids: newCvIds })
            .then(() => {
              queryClient.invalidateQueries({ queryKey: ['lfs', cId] });
              toast.success(`Added label to ${lf.name}`);
            })
            .catch(() => toast.error('Failed to update labels'));
        }
      }

      // CV->CV or cvTree->cvTree: set parent_cv_id on the target CV
      const isCVtoCV = (source.type === 'cv' && target.type === 'cv') ||
                        (source.type === 'cvTree' && target.type === 'cvTree');
      if (isCVtoCV && cId) {
        const parentCV = getCVEntity(connection.source);
        const childCV = getCVEntity(connection.target);
        if (parentCV && childCV) {
          if (childCV.parent_cv_id === parentCV.cv_id) {
            toast.error('Already a child of this concept value');
            return;
          }
          updateConceptValue(cId, childCV.cv_id, {
            parent_cv_id: parentCV.cv_id,
            level: parentCV.level + 1,
          })
            .then(() => {
              queryClient.invalidateQueries({ queryKey: ['conceptValues', cId] });
              queryClient.invalidateQueries({ queryKey: ['cvTree', cId] });
              toast.success(`Set "${childCV.name}" as child of "${parentCV.name}"`);
            })
            .catch(() => toast.error('Failed to update parent'));
        }
      }

      const currentEdges = usePipelineStore.getState().edges;
      const newEdge = {
        id: `e-${connection.source}-${connection.target}`,
        source: connection.source,
        target: connection.target,
        type: 'pipelineEdge',
      };
      setEdges([...currentEdges, newEdge]);
    },
    [setEdges, cId, getIndexEntity, getLFEntity, getRuleEntity, getSnorkelEntity, getCVEntity, queryClient, canvasView],
  );

  // Delay single-click panel open so double-click (enter CV pipeline) isn't intercepted
  const clickTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const onNodeClick = useCallback(
    (_: React.MouseEvent, node: { id: string; data?: Record<string, unknown> }) => {
      const data = node.data as PipelineNodeData | undefined;
      if (data?.entityType === 'placeholder') return;
      if (clickTimer.current) clearTimeout(clickTimer.current);
      clickTimer.current = setTimeout(() => {
        selectNode(node.id, data);
        clickTimer.current = null;
      }, 250);
    },
    [selectNode],
  );

  const onNodeDoubleClick = useCallback(
    (_: React.MouseEvent, node: { id: string; data?: Record<string, unknown> }) => {
      // Cancel the pending single-click panel open
      if (clickTimer.current) {
        clearTimeout(clickTimer.current);
        clickTimer.current = null;
      }
      // CV tree node: navigate into its pipeline
      const data = node.data as PipelineNodeData | undefined;
      if (data?.entityType === 'cvTree') {
        setSelectedCV(data.entity as ConceptValue);
        setCanvasView('pipeline');
      }
    },
    [setSelectedCV, setCanvasView],
  );

  const handleBackToTree = useCallback(() => {
    setSelectedCV(null);
    setCanvasView('tree');
  }, [setSelectedCV, setCanvasView]);

  const defaultEdgeOptions = useMemo(() => ({
    type: 'pipelineEdge',
    selectable: true,
    deletable: canvasView !== 'tree',
  }), [canvasView]);

  return (
    <div className="h-full w-full relative" onDragOver={onDragOver} onDrop={onDrop}>
      {/* Canvas header bar */}
      {canvasView === 'tree' && (
        <div className="absolute top-0 left-0 right-0 z-10 px-4 py-2 bg-white/90 backdrop-blur-sm border-b border-gray-200 flex items-center gap-2">
          <GitBranch className="w-4 h-4 text-indigo-500" />
          <span className="text-sm font-medium text-gray-700">Concept Values</span>
          <span className="text-xs text-gray-400 ml-1">Double-click to view pipeline</span>
        </div>
      )}
      {canvasView === 'pipeline' && (
        <div className="absolute top-0 left-0 right-0 z-10 px-4 py-2 bg-white/90 backdrop-blur-sm border-b border-gray-200 flex items-center gap-2">
          <button
            onClick={handleBackToTree}
            className="flex items-center gap-1 text-sm text-indigo-600 hover:text-indigo-800 font-medium"
          >
            <ChevronLeft className="w-4 h-4" />
            Back to CV Tree
          </button>
          {selectedCV && (
            <>
              <span className="text-gray-300 mx-1">|</span>
              <span className="text-sm text-gray-600">
                Level: <span className="font-medium text-gray-900">{selectedCV.name}</span>
              </span>
            </>
          )}
        </div>
      )}

      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeClick={onNodeClick}
        onNodeDoubleClick={onNodeDoubleClick}
        connectionLineComponent={ConnectionLine}
        defaultEdgeOptions={defaultEdgeOptions}
        nodesDraggable
        nodesConnectable
        edgesReconnectable
        deleteKeyCode={['Backspace', 'Delete']}
        fitView
        fitViewOptions={{ padding: 0.2 }}
        proOptions={{ hideAttribution: true }}
      >
        <Background variant={BackgroundVariant.Dots} gap={16} size={1} color="#e5e7eb" />
        <CanvasControls />
      </ReactFlow>
    </div>
  );
}
