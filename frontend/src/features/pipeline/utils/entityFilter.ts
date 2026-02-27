import type { AllEntities } from '../stores/conceptStore';
import type { Index, Rule, LabelingFunction, SnorkelJob, ConceptValue } from '../types/entities';

interface FilteredEntities {
  indexes: Index[];
  rules: Rule[];
  lfs: LabelingFunction[];
  snorkelJobs: SnorkelJob[];
  conceptValues: ConceptValue[];
}

export function filterEntitiesForCV(
  selectedCV: ConceptValue | null,
  allCVs: ConceptValue[],
  indexes: Index[],
  rules: Rule[],
  lfs: LabelingFunction[],
  snorkelJobs: SnorkelJob[],
): FilteredEntities {
  if (!selectedCV) {
    // Root view: SQL indexes + root CVs only
    const sqlIndexes = indexes.filter((idx) => idx.source_type !== 'derived');
    const rootCVs = allCVs.filter((cv) => cv.parent_cv_id == null);
    return {
      indexes: sqlIndexes,
      rules: [],
      lfs: [],
      snorkelJobs: [],
      conceptValues: rootCVs,
    };
  }

  // CV pipeline: scope to indexes owned by this CV (SQL for root CVs, derived for child CVs)
  const cvIndexes = indexes.filter((idx) => idx.cv_id === selectedCV.cv_id);
  const cvIndexIds = new Set(cvIndexes.map((i) => i.index_id));

  const filteredRules = rules.filter((r) => cvIndexIds.has(r.index_id));
  const filteredRuleIds = new Set(filteredRules.map((r) => r.r_id));
  // Include LFs connected to a CV rule, OR unassigned LFs whose applicable_cv_ids
  // reference this CV or its children (rule set later via canvas edge)
  const childCVIds = new Set(
    allCVs.filter((cv) => cv.parent_cv_id === selectedCV.cv_id).map((cv) => cv.cv_id),
  );
  const filteredLFs = lfs.filter((lf) =>
    filteredRuleIds.has(lf.rule_id) ||
    (lf.rule_id == null && lf.applicable_cv_ids.some((id) => childCVIds.has(id) || id === selectedCV.cv_id)),
  );
  // Include snorkel jobs connected to a CV index, OR draft jobs (index_id null) for this concept
  const filteredSnorkel = snorkelJobs.filter((sj) => cvIndexIds.has(sj.index_id) || sj.index_id == null);
  // No CV nodes on pipeline canvas — LFs already store applicable_cv_ids
  return {
    indexes: cvIndexes,
    rules: filteredRules,
    lfs: filteredLFs,
    snorkelJobs: filteredSnorkel,
    conceptValues: [],
  };
}

export function filterEntitiesForSelectedCV(
  selectedCV: ConceptValue | null,
  all: AllEntities,
): FilteredEntities {
  return filterEntitiesForCV(
    selectedCV,
    all.conceptValues,
    all.indexes,
    all.rules,
    all.lfs,
    all.snorkelJobs,
  );
}
