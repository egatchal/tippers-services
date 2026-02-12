import type { Edge } from '@xyflow/react';

export interface PipelineEdgeData {
  animated?: boolean;
  [key: string]: unknown;
}

export type PipelineEdge = Edge<PipelineEdgeData>;
