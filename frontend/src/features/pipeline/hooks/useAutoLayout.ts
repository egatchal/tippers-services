import { useCallback } from 'react';
import { usePipelineStore } from '../stores/pipelineStore';
import { applyDagreLayout } from '../utils/layoutEngine';

export function useAutoLayout() {
  const nodes = usePipelineStore((s) => s.nodes);
  const edges = usePipelineStore((s) => s.edges);
  const setNodes = usePipelineStore((s) => s.setNodes);

  return useCallback(() => {
    const laid = applyDagreLayout(nodes, edges);
    setNodes(laid);
  }, [nodes, edges, setNodes]);
}
