import Dagre from '@dagrejs/dagre';
import type { PipelineNode } from '../types/nodes';
import type { PipelineEdge } from '../types/edges';
import { NODE_WIDTH, NODE_HEIGHT } from '../../../shared/utils/constants';

// Fixed vertical ordering for pipeline-view placeholder nodes
const pipelinePlaceholderOrder = [
  'placeholder-index',
  'placeholder-rule',
  'placeholder-lf',
  'placeholder-snorkel',
];

// Tree-view placeholder IDs (positioned as a mini tree, not a column)
const treePlaceholderIds = new Set([
  'placeholder-cv-root',
  'placeholder-cv-child1',
  'placeholder-cv-child2',
]);

export function applyDagreLayout(
  nodes: PipelineNode[],
  edges: PipelineEdge[],
): PipelineNode[] {
  const realNodes = nodes.filter((n) => !n.id.startsWith('placeholder-'));
  const placeholders = nodes.filter((n) => n.id.startsWith('placeholder-'));

  // Split placeholders into pipeline column vs tree-shaped groups
  const pipelinePH = placeholders.filter((n) => !treePlaceholderIds.has(n.id));
  const treePH = placeholders.filter((n) => treePlaceholderIds.has(n.id));

  // Run dagre only on real nodes and their edges
  const realIds = new Set(realNodes.map((n) => n.id));
  const realEdges = edges.filter((e) => realIds.has(e.source) && realIds.has(e.target));

  let laidReal = realNodes;
  if (realNodes.length > 0) {
    const g = new Dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));
    g.setGraph({ rankdir: 'TB', nodesep: 60, ranksep: 80 });

    for (const node of realNodes) {
      g.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
    }
    for (const edge of realEdges) {
      g.setEdge(edge.source, edge.target);
    }

    Dagre.layout(g);

    laidReal = realNodes.map((node) => {
      const pos = g.node(node.id);
      return {
        ...node,
        position: {
          x: pos.x - NODE_WIDTH / 2,
          y: pos.y - NODE_HEIGHT / 2,
        },
      };
    });
  }

  const result = [...laidReal];

  // --- Pipeline placeholders: fixed vertical column to the right ---
  if (pipelinePH.length > 0) {
    const fixedX = laidReal.length > 0
      ? Math.max(...laidReal.map((n) => n.position.x)) + NODE_WIDTH + 120
      : 400;
    const spacing = NODE_HEIGHT + 40;

    pipelinePH.sort((a, b) => {
      const ai = pipelinePlaceholderOrder.indexOf(a.id);
      const bi = pipelinePlaceholderOrder.indexOf(b.id);
      return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi);
    });

    for (let i = 0; i < pipelinePH.length; i++) {
      result.push({ ...pipelinePH[i], position: { x: fixedX, y: i * spacing } });
    }
  }

  // --- Tree placeholders: fixed mini-tree to the right ---
  if (treePH.length > 0) {
    // Anchor: to the right of all real nodes (or all pipeline placeholders)
    const allXSoFar = result.map((n) => n.position.x);
    const baseX = allXSoFar.length > 0
      ? Math.max(...allXSoFar) + NODE_WIDTH + 120
      : 400;
    const rowGap = NODE_HEIGHT + 50;
    const childGap = NODE_WIDTH + 40; // full node width + breathing room

    // Root centered above two children
    const posMap: Record<string, { x: number; y: number }> = {
      'placeholder-cv-root':   { x: baseX + childGap / 2,  y: 0 },
      'placeholder-cv-child1': { x: baseX,                 y: rowGap },
      'placeholder-cv-child2': { x: baseX + childGap,      y: rowGap },
    };

    for (const ph of treePH) {
      result.push({ ...ph, position: posMap[ph.id] ?? { x: baseX, y: 0 } });
    }
  }

  return result;
}
