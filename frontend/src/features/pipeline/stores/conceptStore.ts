import { create } from 'zustand';
import type { Concept, ConceptValue } from '../types/entities';

interface ConceptStore {
  concepts: Concept[];
  activeConcept: Concept | null;
  conceptValues: ConceptValue[];
  activeLevel: number;
  setConcepts: (concepts: Concept[]) => void;
  setActiveConcept: (concept: Concept | null) => void;
  setConceptValues: (values: ConceptValue[]) => void;
  setActiveLevel: (level: number) => void;
}

export const useConceptStore = create<ConceptStore>((set) => ({
  concepts: [],
  activeConcept: null,
  conceptValues: [],
  activeLevel: 1,
  setConcepts: (concepts) => set({ concepts }),
  setActiveConcept: (concept) => set({ activeConcept: concept }),
  setConceptValues: (values) => set({ conceptValues: values }),
  setActiveLevel: (level) => set({ activeLevel: level }),
}));
