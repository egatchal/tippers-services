import Dagre from '@dagrejs/dagre';
import type { PipelineNode } from '../types/nodes';
import type { PipelineEdge } from '../types/edges';
import { NODE_WIDTH, NODE_HEIGHT } from '../../../shared/utils/constants';

export function applyDagreLayout(
  nodes: PipelineNode[],
  edges: PipelineEdge[],
): PipelineNode[] {
  const g = new Dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: 'TB', nodesep: 60, ranksep: 80 });

  for (const node of nodes) {
    g.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
  }
  for (const edge of edges) {
    g.setEdge(edge.source, edge.target);
  }

  Dagre.layout(g);

  return nodes.map((node) => {
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
