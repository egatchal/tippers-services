import { useEffect } from 'react';
import { useQuery } from '@tanstack/react-query';
import { listIndexes } from '../api/indexes';
import { listRules } from '../api/rules';
import { listFeatures } from '../api/features';
import { listLFs } from '../api/labelingFunctions';
import { listSnorkelJobs } from '../api/snorkel';
import { listClassifierJobs } from '../api/classifiers';
import { listConceptValues } from '../api/concepts';
import { usePipelineStore } from '../stores/pipelineStore';
import { useConceptStore } from '../stores/conceptStore';

export function useConceptData(cId: number | null) {
  const buildPipeline = usePipelineStore((s) => s.buildPipeline);
  const setConceptValues = useConceptStore((s) => s.setConceptValues);

  const enabled = cId != null;

  const indexesQ = useQuery({ queryKey: ['indexes', cId], queryFn: () => listIndexes(cId!), enabled });
  const rulesQ = useQuery({ queryKey: ['rules', cId], queryFn: () => listRules(cId!), enabled });
  const featuresQ = useQuery({ queryKey: ['features', cId], queryFn: () => listFeatures(cId!), enabled });
  const lfsQ = useQuery({ queryKey: ['lfs', cId], queryFn: () => listLFs(cId!), enabled });
  const snorkelQ = useQuery({ queryKey: ['snorkelJobs', cId], queryFn: () => listSnorkelJobs(cId!), enabled });
  const classifierQ = useQuery({ queryKey: ['classifierJobs', cId], queryFn: () => listClassifierJobs(cId!), enabled });
  const cvQ = useQuery({ queryKey: ['conceptValues', cId], queryFn: () => listConceptValues(cId!), enabled });

  const allLoaded = enabled &&
    indexesQ.isSuccess && rulesQ.isSuccess && featuresQ.isSuccess &&
    lfsQ.isSuccess && snorkelQ.isSuccess && classifierQ.isSuccess && cvQ.isSuccess;

  const isLoading = indexesQ.isLoading || rulesQ.isLoading || featuresQ.isLoading ||
    lfsQ.isLoading || snorkelQ.isLoading || classifierQ.isLoading || cvQ.isLoading;

  useEffect(() => {
    if (!allLoaded) return;
    buildPipeline({
      indexes: indexesQ.data!,
      rules: rulesQ.data!,
      features: featuresQ.data!,
      lfs: lfsQ.data!,
      snorkelJobs: snorkelQ.data!,
      classifierJobs: classifierQ.data!,
      conceptValues: cvQ.data!,
    });
    setConceptValues(cvQ.data!);
  }, [allLoaded, cId, indexesQ.data, rulesQ.data, featuresQ.data, lfsQ.data, snorkelQ.data, classifierQ.data, cvQ.data]);

  return { isLoading };
}
