import { useQuery } from '@tanstack/react-query';
import { getDagsterRunStatus } from '../api/dagster';
import { POLL_INTERVAL } from '../utils/constants';

export function usePollDagsterRun(runId: string | null | undefined) {
  return useQuery({
    queryKey: ['dagster-run', runId],
    queryFn: () => getDagsterRunStatus(runId!),
    enabled: !!runId,
    refetchInterval: (query) => {
      const status = query.state.data?.status?.toUpperCase();
      if (status === 'COMPLETED' || status === 'FAILED' || status === 'CANCELLED') return false;
      return POLL_INTERVAL;
    },
  });
}
