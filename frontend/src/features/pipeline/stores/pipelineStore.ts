import { create } from 'zustand';
import type { PipelineNode, PipelineNodeData } from '../types/nodes';
import type { PipelineEdge } from '../types/edges';
import type { Index, Rule, LabelingFunction, SnorkelJob, ConceptValue } from '../types/entities';
import { buildNodes } from '../utils/nodeFactory';
import { buildEdges } from '../utils/edgeFactory';
import { applyDagreLayout } from '../utils/layoutEngine';
import { computeStaleness, overlayStaleStatus, type StaleStatus } from '../utils/staleness';

type BuildPipelineInput = {
  indexes: Index[];
  rules: Rule[];
  lfs: LabelingFunction[];
  snorkelJobs: SnorkelJob[];
  conceptValues: ConceptValue[];
  selectedCV?: ConceptValue | null;
  allIndexes?: Index[];
  allSnorkelJobs?: SnorkelJob[];
};

interface PipelineStore {
  nodes: PipelineNode[];
  edges: PipelineEdge[];
  selectedNodeId: string | null;
  selectedNodeData: PipelineNodeData | null;
  panelOpen: boolean;
  stalenessMap: Map<string, StaleStatus>;
  manualNodeIds: Set<string>;
  dismissedNodeIds: Set<string>;
  lastBuildCVId: number | null;

  setNodes: (nodes: PipelineNode[]) => void;
  setEdges: (edges: PipelineEdge[]) => void;
  selectNode: (nodeId: string | null, nodeData?: PipelineNodeData) => void;
  closePanel: () => void;
  addNode: (node: PipelineNode) => void;
  removeNode: (nodeId: string) => void;
  dismissNode: (nodeId: string) => void;

  buildPipeline: (data: BuildPipelineInput) => void;
}

export const usePipelineStore = create<PipelineStore>((set, get) => ({
  nodes: [],
  edges: [],
  selectedNodeId: null,
  selectedNodeData: null,
  panelOpen: false,
  stalenessMap: new Map(),
  manualNodeIds: new Set(),
  dismissedNodeIds: new Set(),
  lastBuildCVId: null,

  setNodes: (nodes) => set({ nodes }),
  setEdges: (edges) => set({ edges }),
  selectNode: (nodeId, nodeData) => set({
    selectedNodeId: nodeId,
    selectedNodeData: nodeData ?? null,
    panelOpen: !!nodeId,
  }),
  closePanel: () => set({ selectedNodeId: null, selectedNodeData: null, panelOpen: false }),

  addNode: (node) => {
    const current = get().nodes;
    if (current.some((n) => n.id === node.id)) return;
    const newManual = new Set(get().manualNodeIds);
    newManual.add(node.id);
    const newDismissed = new Set(get().dismissedNodeIds);
    newDismissed.delete(node.id);
    set({ nodes: [...current, node], manualNodeIds: newManual, dismissedNodeIds: newDismissed });
  },

  removeNode: (nodeId) => {
    const newManual = new Set(get().manualNodeIds);
    newManual.delete(nodeId);
    set({
      nodes: get().nodes.filter((n) => n.id !== nodeId),
      edges: get().edges.filter((e) => e.source !== nodeId && e.target !== nodeId),
      manualNodeIds: newManual,
    });
  },

  dismissNode: (nodeId) => {
    const newDismissed = new Set(get().dismissedNodeIds);
    newDismissed.add(nodeId);
    const newManual = new Set(get().manualNodeIds);
    newManual.delete(nodeId);
    set({
      nodes: get().nodes.filter((n) => n.id !== nodeId),
      edges: get().edges.filter((e) => e.source !== nodeId && e.target !== nodeId),
      manualNodeIds: newManual,
      dismissedNodeIds: newDismissed,
    });
  },

  buildPipeline: (data) => {
    const allNodes = buildNodes(data);
    const allEdges = buildEdges(data);
    const { dismissedNodeIds } = get();

    // Filter out dismissed nodes and their edges before computing layout
    const edges = allEdges.filter(
      (e) => !dismissedNodeIds.has(e.source) && !dismissedNodeIds.has(e.target),
    );

    // Only place nodes with at least one edge on canvas; edgeless → sidebar
    const connectedIds = new Set<string>();
    for (const e of edges) {
      connectedIds.add(e.source);
      connectedIds.add(e.target);
    }
    const canonicalNodes = allNodes.filter(
      (n) => !dismissedNodeIds.has(n.id) && (n.id.startsWith('placeholder-') || connectedIds.has(n.id)),
    );

    const stalenessMap = computeStaleness(canonicalNodes, data.allIndexes, data.allSnorkelJobs);
    const nodesWithStaleness = canonicalNodes.map((n) => {
      const staleStatus = stalenessMap.get(n.id);
      const overlaid = overlayStaleStatus(n.data.status, staleStatus);
      if (overlaid !== n.data.status) {
        return { ...n, data: { ...n.data, status: overlaid } };
      }
      return n;
    });
    const laid = applyDagreLayout(nodesWithStaleness, edges);

    const incomingCVId = data.selectedCV?.cv_id ?? null;
    const { lastBuildCVId, manualNodeIds, nodes: currentNodes } = get();
    const sameCV = incomingCVId === lastBuildCVId;

    if (sameCV) {
      // Same CV — preserve positions of existing nodes so layout doesn't jump
      const positionMap = new Map(currentNodes.map((n) => [n.id, n.position]));
      const mergedNodes = laid.map((n) => {
        const existingPos = positionMap.get(n.id);
        return existingPos ? { ...n, position: existingPos } : n;
      });
      const canonicalIds = new Set(laid.map((n) => n.id));
      const manualToKeep = currentNodes.filter(
        (n) => manualNodeIds.has(n.id) && !canonicalIds.has(n.id),
      );
      set({ nodes: [...mergedNodes, ...manualToKeep], edges, stalenessMap, lastBuildCVId: incomingCVId });
    } else {
      // Different CV — full reset with fresh layout
      set({ nodes: laid, edges, stalenessMap, manualNodeIds: new Set(), dismissedNodeIds: new Set(), lastBuildCVId: incomingCVId });
    }
  },
}));

export function getSelectedEntity(store: PipelineStore): PipelineNodeData | null {
  if (!store.selectedNodeId) return null;
  // Prefer directly-stored node data (works for both pipeline and tree view nodes)
  if (store.selectedNodeData) return store.selectedNodeData;
  // Fallback: look up by ID in pipeline nodes
  const node = store.nodes.find((n) => n.id === store.selectedNodeId);
  return (node?.data as PipelineNodeData) ?? null;
}
