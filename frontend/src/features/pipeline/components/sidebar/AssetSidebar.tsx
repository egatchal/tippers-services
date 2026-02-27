import { useMemo, useCallback } from 'react';
import toast from 'react-hot-toast';
import { usePipelineStore } from '../../stores/pipelineStore';
import { useConceptStore } from '../../stores/conceptStore';
import { createSingleNode } from '../../utils/nodeFactory';
import { filterEntitiesForSelectedCV } from '../../utils/entityFilter';
import type { PipelineNodeData, PipelineNodeType } from '../../types/nodes';
import { getStatusFromEntity } from '../../../../shared/utils/statusColors';
import SidebarSection from './SidebarSection';
import SidebarAssetItem from './SidebarAssetItem';

interface SectionConfig {
  type: PipelineNodeType;
  title: string;
}

const allSections: SectionConfig[] = [
  { type: 'index', title: 'Indexes' },
  { type: 'rule', title: 'Rules' },
  { type: 'cv', title: 'Concept Values' },
  { type: 'lf', title: 'Labeling Functions' },
  { type: 'snorkel', title: 'Snorkel Runs' },
];

const treeSectionTypes = new Set<PipelineNodeType>(['cv']);

interface SidebarItem {
  nodeId: string;
  label: string;
  status: string;
}

export default function AssetSidebar() {
  const nodes = usePipelineStore((s) => s.nodes);
  const addNode = usePipelineStore((s) => s.addNode);
  const canvasView = useConceptStore((s) => s.canvasView);
  const allEntities = useConceptStore((s) => s.allEntities);
  const selectedCV = useConceptStore((s) => s.selectedCV);
  const setPendingCreateType = useConceptStore((s) => s.setPendingCreateType);

  const handleAddToCanvas = useCallback((nodeId: string, label: string) => {
    if (!allEntities) return;
    // Place to the right of rightmost node
    const currentNodes = usePipelineStore.getState().nodes;
    const maxX = currentNodes.length > 0
      ? Math.max(...currentNodes.map((n) => n.position.x)) + 350
      : 100;
    const y = currentNodes.length > 0 ? currentNodes[currentNodes.length - 1].position.y : 100;

    const node = createSingleNode(nodeId, allEntities, { x: maxX, y });
    if (!node) return;
    addNode(node);
    toast.success(`Added "${label}" to canvas`);
  }, [allEntities, addNode]);

  // Compute the set of entity node IDs currently visible on the canvas
  const onCanvasIds = useMemo(() => {
    const ids = new Set<string>();
    // Pipeline view: entity nodes live in the pipeline store
    for (const n of nodes) {
      if ((n.data as PipelineNodeData).entityType !== 'placeholder') {
        ids.add(n.id);
      }
    }
    // Tree view: CVs are represented as cvTree nodes
    if (canvasView === 'tree' && allEntities) {
      for (const cv of allEntities.conceptValues) {
        ids.add(`cv-${cv.cv_id}`);
      }
    }
    return ids;
  }, [nodes, canvasView, allEntities]);

  // Scope entities: tree view → SQL indexes + root CVs only;
  // pipeline view → entities scoped to the selected CV
  const levelEntities = useMemo(() => {
    if (!allEntities) return null;
    return filterEntitiesForSelectedCV(selectedCV, allEntities);
  }, [allEntities, selectedCV]);

  // Compute sidebar items = level-scoped entities NOT on canvas
  const poolSections = useMemo(() => {
    if (!levelEntities) return new Map<PipelineNodeType, SidebarItem[]>();

    const map = new Map<PipelineNodeType, SidebarItem[]>();

    map.set('index', levelEntities.indexes
      .filter((i) => !onCanvasIds.has(`index-${i.index_id}`))
      .map((i) => ({
        nodeId: `index-${i.index_id}`,
        label: i.source_type === 'derived' ? `${i.name} (derived)` : i.name,
        status: getStatusFromEntity(i),
      })));

    map.set('rule', levelEntities.rules
      .filter((r) => !onCanvasIds.has(`rule-${r.r_id}`))
      .map((r) => ({
        nodeId: `rule-${r.r_id}`,
        label: r.name,
        status: getStatusFromEntity(r),
      })));

    map.set('cv', levelEntities.conceptValues
      .filter((cv) => !onCanvasIds.has(`cv-${cv.cv_id}`))
      .map((cv) => ({
        nodeId: `cv-${cv.cv_id}`,
        label: cv.name,
        status: 'materialized',
      })));

    map.set('lf', levelEntities.lfs
      .filter((lf) => !onCanvasIds.has(`lf-${lf.lf_id}`))
      .map((lf) => ({
        nodeId: `lf-${lf.lf_id}`,
        label: lf.name,
        status: lf.is_active ? 'materialized' : 'default',
      })));

    map.set('snorkel', levelEntities.snorkelJobs
      .filter((sj) => !onCanvasIds.has(`snorkel-${sj.job_id}`))
      .map((sj) => ({
        nodeId: `snorkel-${sj.job_id}`,
        label: `Snorkel #${sj.job_id}`,
        status: getStatusFromEntity(sj),
      })));

    return map;
  }, [levelEntities, onCanvasIds]);

  // Tree view: indexes + CVs only
  // Pipeline view with selectedCV: no CV section (you're inside one)
  // Pipeline view without selectedCV: all sections
  const sections = useMemo(() => {
    if (canvasView === 'tree') {
      return allSections.filter((s) => treeSectionTypes.has(s.type));
    }
    if (selectedCV) {
      return allSections.filter((s) => s.type !== 'cv');
    }
    return allSections;
  }, [canvasView, selectedCV]);

  return (
    <div className="w-[220px] border-r border-gray-200 bg-white overflow-y-auto shrink-0">
      <div className="px-3 py-2 border-b border-gray-200">
        <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Assets</h3>
      </div>
      {sections.map(({ type, title }) => {
        const items = poolSections.get(type) ?? [];
        return (
          <SidebarSection
            key={type}
            title={title}
            count={items.length}
            onCreateClick={() => setPendingCreateType(type)}
          >
            {items.length === 0 ? (
              <p className="text-[11px] text-gray-400 italic px-2 py-1">All on canvas</p>
            ) : (
              items.map((item) => (
                <SidebarAssetItem
                  key={item.nodeId}
                  nodeId={item.nodeId}
                  label={item.label}
                  status={item.status as import('../../../../shared/utils/statusColors').NodeStatus}
                  onClick={() => handleAddToCanvas(item.nodeId, item.label)}
                />
              ))
            )}
          </SidebarSection>
        );
      })}
    </div>
  );
}
