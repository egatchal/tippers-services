import type { StepStatus } from '../types/workflow';
import ServiceStatusBadge from '../../../shared/components/ServiceStatusBadge';

interface Props {
  stepStatuses: Record<string, StepStatus>;
}

export default function StepStatusBreakdown({ stepStatuses }: Props) {
  const entries = Object.entries(stepStatuses);

  if (entries.length === 0) {
    return <p className="text-xs text-gray-500">No steps</p>;
  }

  return (
    <div className="space-y-2">
      {entries.map(([key, step]) => (
        <div key={key} className="flex items-center gap-3 text-xs">
          <div className="w-2 h-2 rounded-full bg-gray-300 shrink-0" />
          <span className="font-mono font-medium text-gray-700 min-w-[160px]">{key}</span>
          <ServiceStatusBadge status={step.status} />
          {step.dagster_run_id && (
            <span className="font-mono text-gray-400 truncate max-w-[120px]">{step.dagster_run_id}</span>
          )}
          {step.error && (
            <span className="text-red-500 truncate max-w-[200px]">{step.error}</span>
          )}
        </div>
      ))}
    </div>
  );
}
