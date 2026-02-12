import { useEffect } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { usePipelineStore } from '../stores/pipelineStore';
import { POLL_INTERVAL } from '../../../shared/utils/constants';

export function usePollJobs(cId: number | null) {
  const queryClient = useQueryClient();
  const nodes = usePipelineStore((s) => s.nodes);

  const hasRunning = nodes.some(
    (n) => n.data.status === 'running' || n.data.status === 'pending'
  );

  useEffect(() => {
    if (!hasRunning || !cId) return;
    const interval = setInterval(() => {
      queryClient.invalidateQueries({ queryKey: ['indexes', cId] });
      queryClient.invalidateQueries({ queryKey: ['rules', cId] });
      queryClient.invalidateQueries({ queryKey: ['features', cId] });
      queryClient.invalidateQueries({ queryKey: ['snorkelJobs', cId] });
      queryClient.invalidateQueries({ queryKey: ['classifierJobs', cId] });
    }, POLL_INTERVAL);
    return () => clearInterval(interval);
  }, [hasRunning, cId, queryClient]);
}
