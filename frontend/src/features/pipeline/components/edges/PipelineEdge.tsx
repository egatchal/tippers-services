import { memo } from 'react';
import { BaseEdge, getSmoothStepPath, type EdgeProps } from '@xyflow/react';

function PipelineEdge(props: EdgeProps) {
  const { sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, data } = props;
  const [edgePath] = getSmoothStepPath({ sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition });
  const animated = (data as { animated?: boolean })?.animated;

  return (
    <BaseEdge
      {...props}
      path={edgePath}
      style={{
        stroke: animated ? '#f59e0b' : '#94a3b8',
        strokeWidth: 2,
        strokeDasharray: animated ? '5 5' : undefined,
      }}
    />
  );
}

export default memo(PipelineEdge);
