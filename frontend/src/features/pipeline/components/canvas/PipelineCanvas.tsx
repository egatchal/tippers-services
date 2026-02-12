import { useCallback, useMemo } from 'react';
import {
  ReactFlow,
  Background,
  BackgroundVariant,
  type OnNodesChange,
  type OnEdgesChange,
  type EdgeChange,
  type Connection,
  applyNodeChanges,
  applyEdgeChanges,
} from '@xyflow/react';
import toast from 'react-hot-toast';
import { useQueryClient } from '@tanstack/react-query';
import { useDroppable } from '@dnd-kit/core';
import { usePipelineStore } from '../../stores/pipelineStore';
import { useConceptStore } from '../../stores/conceptStore';
import { isValidConnection, parseNodeId } from '../../utils/connectionRules';
import { updateLF } from '../../api/labelingFunctions';
import type { LabelingFunction } from '../../types/entities';
import IndexNode from '../nodes/IndexNode';
import RuleNode from '../nodes/RuleNode';
import FeatureNode from '../nodes/FeatureNode';
import LabelingFunctionNode from '../nodes/LabelingFunctionNode';
import SnorkelRunNode from '../nodes/SnorkelRunNode';
import ClassifierRunNode from '../nodes/ClassifierRunNode';
import ConceptValueNode from '../nodes/ConceptValueNode';
import PipelineEdge from '../edges/PipelineEdge';
import ConnectionLine from '../edges/ConnectionLine';
import CanvasControls from './CanvasControls';

const nodeTypes = {
  indexNode: IndexNode,
  ruleNode: RuleNode,
  featureNode: FeatureNode,
  lfNode: LabelingFunctionNode,
  snorkelNode: SnorkelRunNode,
  classifierNode: ClassifierRunNode,
  cvNode: ConceptValueNode,
};

const edgeTypes = {
  pipelineEdge: PipelineEdge,
};

export default function PipelineCanvas() {
  const { nodes, edges, setNodes, setEdges, selectNode } = usePipelineStore();
  const cId = useConceptStore((s) => s.activeConcept?.c_id);
  const queryClient = useQueryClient();
  const { setNodeRef } = useDroppable({ id: 'pipeline-canvas' });

  const getLFEntity = useCallback(
    (lfNodeId: string): LabelingFunction | null => {
      const node = nodes.find((n) => n.id === lfNodeId);
      if (!node || node.data.entityType !== 'lf') return null;
      return node.data.entity as LabelingFunction;
    },
    [nodes],
  );

  const onNodesChange: OnNodesChange = useCallback(
    (changes) => setNodes(applyNodeChanges(changes, nodes) as typeof nodes),
    [nodes, setNodes],
  );

  const onEdgesChange: OnEdgesChange = useCallback(
    (changes: EdgeChange[]) => {
      // Intercept CV→LF edge removals to update backend
      const removals = changes.filter((c) => c.type === 'remove');
      for (const removal of removals) {
        const edge = edges.find((e) => e.id === removal.id);
        if (!edge || !cId) continue;
        const source = parseNodeId(edge.source);
        const target = parseNodeId(edge.target);
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
      }
      setEdges(applyEdgeChanges(changes, edges) as typeof edges);
    },
    [edges, setEdges, cId, getLFEntity, queryClient],
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

      // CV→LF: update applicable_cv_ids on the backend
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

      const newEdge = {
        id: `e-${connection.source}-${connection.target}`,
        source: connection.source,
        target: connection.target,
        type: 'pipelineEdge',
      };
      setEdges([...edges, newEdge]);
    },
    [edges, setEdges, cId, getLFEntity, queryClient],
  );

  const onNodeClick = useCallback(
    (_: React.MouseEvent, node: { id: string }) => selectNode(node.id),
    [selectNode],
  );

  const defaultEdgeOptions = useMemo(() => ({ type: 'pipelineEdge' }), []);

  return (
    <div ref={setNodeRef} className="h-full w-full relative">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={onConnect}
        onNodeClick={onNodeClick}
        connectionLineComponent={ConnectionLine}
        defaultEdgeOptions={defaultEdgeOptions}
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
