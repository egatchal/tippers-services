import { useCallback } from 'react';
import type { DragEndEvent } from '@dnd-kit/core';
import { usePipelineStore } from '../stores/pipelineStore';

export function useDragToCanvas() {
  const selectNode = usePipelineStore((s) => s.selectNode);

  return useCallback((event: DragEndEvent) => {
    const { active, over } = event;
    if (!over || over.id !== 'pipeline-canvas') return;

    // When a sidebar item is dropped on the canvas, select that node
    const nodeId = active.id as string;
    selectNode(nodeId);
  }, [selectNode]);
}
