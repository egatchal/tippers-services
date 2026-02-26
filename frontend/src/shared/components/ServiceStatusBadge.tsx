import { type ServiceStatus, serviceStatusBgClass } from '../utils/statusColors';

interface Props {
  status: string;
}

export default function ServiceStatusBadge({ status }: Props) {
  const upper = status.toUpperCase() as ServiceStatus;
  const bg = serviceStatusBgClass[upper] ?? 'bg-gray-100 text-gray-600';

  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${bg}`}>
      {upper}
    </span>
  );
}
