import { Inbox } from 'lucide-react';

interface Props {
  title?: string;
  message?: string;
  action?: React.ReactNode;
}

export default function EmptyState({ title = 'No data', message = 'Nothing here yet.', action }: Props) {
  return (
    <div className="flex flex-col items-center justify-center p-12 text-center">
      <Inbox className="w-10 h-10 text-gray-300 mb-3" />
      <h3 className="text-sm font-medium text-gray-700">{title}</h3>
      <p className="text-xs text-gray-500 mt-1">{message}</p>
      {action && <div className="mt-4">{action}</div>}
    </div>
  );
}
