import type { ConnectionLineComponentProps } from '@xyflow/react';

export default function ConnectionLine({ fromX, fromY, toX, toY }: ConnectionLineComponentProps) {
  return (
    <g>
      <path
        fill="none"
        stroke="#3b82f6"
        strokeWidth={2}
        strokeDasharray="6 3"
        d={`M${fromX},${fromY} C ${fromX},${(fromY + toY) / 2} ${toX},${(fromY + toY) / 2} ${toX},${toY}`}
      />
      <circle cx={toX} cy={toY} r={4} fill="#3b82f6" />
    </g>
  );
}
