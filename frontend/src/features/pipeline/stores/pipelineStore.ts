import { create } from 'zustand';
import type { PipelineNode, PipelineNodeData } from '../types/nodes';
import type { PipelineEdge } from '../types/edges';
import type { Index, Rule, Feature, LabelingFunction, SnorkelJob, ClassifierJob, ConceptValue } from '../types/entities';
import { buildNodes } from '../utils/nodeFactory';
import { buildEdges } from '../utils/edgeFactory';
import { applyDagreLayout } from '../utils/layoutEngine';
import { computeStaleness, overlayStaleStatus, type StaleStatus } from '../utils/staleness';

interface PipelineStore {
  nodes: PipelineNode[];
  edges: PipelineEdge[];
  selectedNodeId: string | null;
  panelOpen: boolean;
  stalenessMap: Map<string, StaleStatus>;

  setNodes: (nodes: PipelineNode[]) => void;
  setEdges: (edges: PipelineEdge[]) => void;
  selectNode: (nodeId: string | null) => void;
  closePanel: () => void;

  buildPipeline: (data: {
    indexes: Index[];
    rules: Rule[];
    features: Feature[];
    lfs: LabelingFunction[];
    snorkelJobs: SnorkelJob[];
    classifierJobs: ClassifierJob[];
    conceptValues: ConceptValue[];
  }) => void;
}

export const usePipelineStore = create<PipelineStore>((set) => ({
  nodes: [],
  edges: [],
  selectedNodeId: null,
  panelOpen: false,
  stalenessMap: new Map(),

  setNodes: (nodes) => set({ nodes }),
  setEdges: (edges) => set({ edges }),
  selectNode: (nodeId) => set({ selectedNodeId: nodeId, panelOpen: !!nodeId }),
  closePanel: () => set({ selectedNodeId: null, panelOpen: false }),

  buildPipeline: (data) => {
    const nodes = buildNodes(data);
    const edges = buildEdges(data);

    // Compute staleness and overlay onto node statuses
    const stalenessMap = computeStaleness(nodes);
    const nodesWithStaleness = nodes.map((n) => {
      const staleStatus = stalenessMap.get(n.id);
      const overlaid = overlayStaleStatus(n.data.status, staleStatus);
      if (overlaid !== n.data.status) {
        return { ...n, data: { ...n.data, status: overlaid } };
      }
      return n;
    });

    const laid = applyDagreLayout(nodesWithStaleness, edges);
    set({ nodes: laid, edges, stalenessMap });
  },
}));

export function getSelectedEntity(store: PipelineStore): PipelineNodeData | null {
  if (!store.selectedNodeId) return null;
  const node = store.nodes.find((n) => n.id === store.selectedNodeId);
  return (node?.data as PipelineNodeData) ?? null;
}
