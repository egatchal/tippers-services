import type { PipelineNodeType } from '../types/nodes';

// Valid source → target pairs for edge drawing
const validConnections: Record<string, PipelineNodeType[]> = {
  index: ['rule', 'feature'],
  rule: ['lf'],
  cv: ['lf'],
  lf: ['snorkel'],
  feature: ['classifier'],
  snorkel: ['classifier'],
};

export function isValidConnection(sourceType: PipelineNodeType, targetType: PipelineNodeType): boolean {
  return validConnections[sourceType]?.includes(targetType) ?? false;
}

export function parseNodeId(nodeId: string): { type: PipelineNodeType; id: number } {
  const [type, ...rest] = nodeId.split('-');
  return { type: type as PipelineNodeType, id: parseInt(rest.join('-'), 10) };
}
