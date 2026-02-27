import { create } from 'zustand';
import type { Concept, ConceptValue, ConceptValueTreeNode, Index, Rule, LabelingFunction, SnorkelJob } from '../types/entities';
import type { PipelineNodeType } from '../types/nodes';

export interface AllEntities {
  indexes: Index[];
  rules: Rule[];
  lfs: LabelingFunction[];
  snorkelJobs: SnorkelJob[];
  conceptValues: ConceptValue[];
}

interface ConceptStore {
  concepts: Concept[];
  activeConcept: Concept | null;
  conceptValues: ConceptValue[];
  activeLevel: number;
  selectedCV: ConceptValue | null;
  cvTree: ConceptValueTreeNode[];
  canvasView: 'tree' | 'pipeline';
  pendingCreateType: PipelineNodeType | null;
  allEntities: AllEntities | null;
  setConcepts: (concepts: Concept[]) => void;
  setActiveConcept: (concept: Concept | null) => void;
  setConceptValues: (values: ConceptValue[]) => void;
  setActiveLevel: (level: number) => void;
  setSelectedCV: (cv: ConceptValue | null) => void;
  setCVTree: (tree: ConceptValueTreeNode[]) => void;
  setCanvasView: (view: 'tree' | 'pipeline') => void;
  setPendingCreateType: (type: PipelineNodeType | null) => void;
  setAllEntities: (entities: AllEntities) => void;
}

export const useConceptStore = create<ConceptStore>((set) => ({
  concepts: [],
  activeConcept: null,
  conceptValues: [],
  activeLevel: 1,
  selectedCV: null,
  cvTree: [],
  canvasView: 'tree',
  pendingCreateType: null,
  allEntities: null,
  setConcepts: (concepts) => set({ concepts }),
  setActiveConcept: (concept) => set({ activeConcept: concept, selectedCV: null, canvasView: 'tree', allEntities: null }),
  setConceptValues: (values) => set({ conceptValues: values }),
  setActiveLevel: (level) => set({ activeLevel: level }),
  setSelectedCV: (cv) => set({ selectedCV: cv }),
  setCVTree: (tree) => set({ cvTree: tree }),
  setCanvasView: (view) => set({ canvasView: view }),
  setPendingCreateType: (type) => set({ pendingCreateType: type }),
  setAllEntities: (entities) => set({ allEntities: entities }),
}));
