import { memo } from 'react';
import { BaseEdge, getSmoothStepPath, type EdgeProps } from '@xyflow/react';

function PipelineEdge(props: EdgeProps) {
  const { sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data, selected } = props;
  const [edgePath] = getSmoothStepPath({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition });
  const animated = (data as { animated?: boolean })?.animated;
  const isPlaceholder = (data as { isPlaceholder?: boolean })?.isPlaceholder;

  return (
    <BaseEdge
      {...props}
      path={edgePath}
      interactionWidth={isPlaceholder ? 0 : 20}
      style={{
        stroke: isPlaceholder ? '#d1d5db' : selected ? '#3b82f6' : animated ? '#f59e0b' : '#94a3b8',
        strokeWidth: isPlaceholder ? 1.5 : selected ? 3 : 2,
        strokeDasharray: isPlaceholder ? '6 4' : animated ? '5 5' : undefined,
        pointerEvents: isPlaceholder ? 'none' : undefined,
      }}
    />
  );
}

export default memo(PipelineEdge);
