import { useEffect, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { listIndexes } from '../api/indexes';
import { listRules } from '../api/rules';
import { listLFs } from '../api/labelingFunctions';
import { listSnorkelJobs } from '../api/snorkel';
import { listConceptValues, getConceptValueTree } from '../api/concepts';
import { useConceptStore } from '../stores/conceptStore';
import type { AllEntities } from '../stores/conceptStore';

export function useConceptData(cId: number | null) {
  const setConceptValues = useConceptStore((s) => s.setConceptValues);
  const setCVTree = useConceptStore((s) => s.setCVTree);
  const setAllEntities = useConceptStore((s) => s.setAllEntities);

  const enabled = cId != null;

  const indexesQ = useQuery({ queryKey: ['indexes', cId], queryFn: () => listIndexes(cId!), enabled });
  const rulesQ = useQuery({ queryKey: ['rules', cId], queryFn: () => listRules(cId!), enabled });
  const lfsQ = useQuery({ queryKey: ['lfs', cId], queryFn: () => listLFs(cId!), enabled });
  const snorkelQ = useQuery({ queryKey: ['snorkelJobs', cId], queryFn: () => listSnorkelJobs(cId!), enabled });
  const cvQ = useQuery({ queryKey: ['conceptValues', cId], queryFn: () => listConceptValues(cId!), enabled });
  const cvTreeQ = useQuery({ queryKey: ['cvTree', cId], queryFn: () => getConceptValueTree(cId!), enabled });

  const allLoaded = enabled &&
    indexesQ.isSuccess && rulesQ.isSuccess &&
    lfsQ.isSuccess && snorkelQ.isSuccess && cvQ.isSuccess;

  const isLoading = indexesQ.isLoading || rulesQ.isLoading ||
    lfsQ.isLoading || snorkelQ.isLoading || cvQ.isLoading;

  // Determine if concept has hierarchy (any CV has parent_cv_id set)
  const hasHierarchy = useMemo(() => {
    if (!cvQ.data) return false;
    return cvQ.data.some((cv) => cv.parent_cv_id != null);
  }, [cvQ.data]);

  useEffect(() => {
    if (cvTreeQ.isSuccess && cvTreeQ.data) {
      setCVTree(cvTreeQ.data);
    }
  }, [cvTreeQ.isSuccess, cvTreeQ.data]);

  useEffect(() => {
    if (!allLoaded) return;
    setConceptValues(cvQ.data!);

    const all: AllEntities = {
      indexes: indexesQ.data!,
      rules: rulesQ.data!,
      lfs: lfsQ.data!,
      snorkelJobs: snorkelQ.data!,
      conceptValues: cvQ.data!,
    };
    setAllEntities(all);
  }, [allLoaded, cId, indexesQ.data, rulesQ.data, lfsQ.data, snorkelQ.data, cvQ.data]);

  return { isLoading, hasHierarchy };
}
