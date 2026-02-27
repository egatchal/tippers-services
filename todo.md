# Pipeline Canvas Fix — Todo

## Problem
Nodes vanish from canvas because `useConceptData` calls `buildPipeline` on every TanStack Query poll refresh (7 `.data` deps), rebuilding the entire node array and wiping user-added nodes.

## Goal
1. Entities load into sidebar pool
2. Canvas shows existing entities as nodes (built once on load)
3. User controls canvas — add/remove nodes freely, they persist through polls
4. Polling updates node STATUS only (running → completed), never adds/removes
5. "Run Pipeline" button executes based on canvas nodes

## Files to modify
- `frontend/src/features/pipeline/hooks/useConceptData.ts`
- `frontend/src/features/pipeline/pages/PipelinePage.tsx`
- `frontend/src/features/pipeline/components/canvas/PipelineCanvas.tsx`

---

## Phase 1: Stop auto-rebuild in `useConceptData.ts`

**What:** Remove the `buildPipeline` calls from the poll-triggered `useEffect`. The hook becomes a pure data loader — fetches entities, stores them in `allEntities`/`conceptValues`, nothing more.

**Current code (lines 53-76):**
```ts
useEffect(() => {
  if (!allLoaded) return;
  setConceptValues(cvQ.data!);
  const all: AllEntities = { indexes: indexesQ.data!, rules: rulesQ.data!, ... };
  setAllEntities(all);
  if (canvasView === 'tree') {
    buildPipeline(all);          // ← REMOVE
    return;                       // ← REMOVE
  }
  const filtered = filterEntitiesForSelectedCV(selectedCV, all);
  buildPipeline({ ...filtered, selectedCV });  // ← REMOVE
}, [allLoaded, cId, canvasView, selectedCV, indexesQ.data, ...]);
```

**After:**
```ts
useEffect(() => {
  if (!allLoaded) return;
  setConceptValues(cvQ.data!);
  const all: AllEntities = { indexes: indexesQ.data!, rules: rulesQ.data!, ... };
  setAllEntities(all);
}, [allLoaded, cId, indexesQ.data, rulesQ.data, featuresQ.data, lfsQ.data, snorkelQ.data, classifierQ.data, cvQ.data]);
```

**Also clean up:**
- Remove `buildPipeline` selector (line 16)
- Remove `selectedCV` selector (line 20)
- Remove `canvasView` selector (line 21)
- Remove `filterEntitiesForSelectedCV` import (line 13)
- Remove `canvasView`, `selectedCV` from dep array

**Status:** [ ] Not started

---

## Phase 2: Build once in `PipelinePage.tsx`

**What:** Add a ref-guarded effect that calls `buildPipeline` once when entities first load. Resets only on concept/CV/view changes (intentional rebuild). Polls do NOT re-trigger.

**Add these imports:**
```ts
import { useRef } from 'react';  // add useRef to existing import
import { usePipelineStore } from '../stores/pipelineStore';
import { filterEntitiesForSelectedCV } from '../utils/entityFilter';
```

**Add after existing hooks:**
```ts
const selectedCV = useConceptStore((s) => s.selectedCV);
const buildPipeline = usePipelineStore((s) => s.buildPipeline);
const hasBuilt = useRef(false);

// Reset build guard on intentional view context changes
useEffect(() => {
  hasBuilt.current = false;
}, [cId, canvasView, selectedCV?.cv_id]);

// Build pipeline ONCE per view context
useEffect(() => {
  if (!allEntities || hasBuilt.current) return;
  if (canvasView === 'tree') {
    buildPipeline(allEntities);
  } else {
    const filtered = filterEntitiesForSelectedCV(selectedCV, allEntities);
    buildPipeline({ ...filtered, selectedCV });
  }
  hasBuilt.current = true;
}, [allEntities, canvasView, selectedCV, buildPipeline]);
```

**Status:** [ ] Not started

---

## Phase 3: Status-update effect in `PipelineCanvas.tsx`

**What:** When `allEntities` changes from polling, update the data (status, metrics, summary) of existing canvas nodes without changing positions or the node list.

**Add this effect after existing hooks:**
```ts
// Update existing node status/data when entities refresh (polling)
useEffect(() => {
  if (!allEntities || canvasView === 'tree') return;
  const currentNodes = usePipelineStore.getState().nodes;
  if (currentNodes.length === 0) return;

  const updatedNodes = currentNodes.map((node) => {
    const fresh = createSingleNode(node.id, allEntities, node.position);
    if (fresh) return { ...node, data: fresh.data };  // keep position, update data
    return node;
  });

  const changed = updatedNodes.some((n, i) => n !== currentNodes[i]);
  if (changed) setNodes(updatedNodes);
}, [allEntities, canvasView, setNodes]);
```

**How it works:**
- `createSingleNode` rebuilds node data from fresh entity (new status, metrics)
- `{ ...node, data: fresh.data }` preserves user's position, updates everything else
- `changed` guard prevents unnecessary re-renders

**Status:** [ ] Not started

---

## Phase 4: Rename button in `PipelinePage.tsx`

**What:** Rename "Re-run Pipeline" to "Run Pipeline" in the header button.

**Change (line ~72):**
```
Before: Re-run Pipeline
After:  Run Pipeline
```

The existing `RunPipelineModal` already reads canvas nodes and builds execution plans from them — no modal changes needed.

**Status:** [ ] Not started

---

## Phase 5: Verify

- [ ] `cd frontend && npx tsc --noEmit` passes
- [ ] Page load → entities fetch → nodes appear on canvas (built once)
- [ ] Wait for poll → nodes stay in place, status badges update
- [ ] Click sidebar item → node added, stays through poll cycles
- [ ] Drag sidebar item → node at drop position, stays
- [ ] Drag node to new position → position persists through polls
- [ ] Switch CV → canvas rebuilds for new CV
- [ ] Click "Run Pipeline" → modal opens with correct execution plan

**Status:** [ ] Not started
