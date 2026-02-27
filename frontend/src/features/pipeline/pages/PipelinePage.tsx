import { useState, useEffect } from 'react';
import { ReactFlowProvider } from '@xyflow/react';
import { RefreshCw } from 'lucide-react';
import { useConceptStore } from '../stores/conceptStore';
import { usePipelineStore } from '../stores/pipelineStore';
import { useConceptData } from '../hooks/useConceptData';
import { usePollJobs } from '../hooks/usePollJobs';
import { filterEntitiesForSelectedCV } from '../utils/entityFilter';
import { buildCVTreeNodes } from '../utils/nodeFactory';
import type { PipelineNodeType } from '../types/nodes';
import ConceptSelector from '../components/ConceptSelector';
import AssetSidebar from '../components/sidebar/AssetSidebar';
import PipelineCanvas from '../components/canvas/PipelineCanvas';
import FloatingPanel from '../components/panel/FloatingPanel';
import CreateEntityModal from '../components/modals/CreateEntityModal';
import RunPipelineModal from '../components/modals/RunPipelineModal';
import LoadingSpinner from '../../../shared/components/LoadingSpinner';
import EmptyState from '../../../shared/components/EmptyState';

export default function PipelinePage() {
  const activeConcept = useConceptStore((s) => s.activeConcept);
  const allEntities = useConceptStore((s) => s.allEntities);
  const canvasView = useConceptStore((s) => s.canvasView);
  const selectedCV = useConceptStore((s) => s.selectedCV);
  const conceptValues = useConceptStore((s) => s.conceptValues);
  const buildPipeline = usePipelineStore((s) => s.buildPipeline);
  const setNodes = usePipelineStore((s) => s.setNodes);
  const setEdges = usePipelineStore((s) => s.setEdges);
  const cId = activeConcept?.c_id ?? null;
  const { isLoading } = useConceptData(cId);
  usePollJobs(cId);

  // Build nodes for current view — rebuilds whenever data changes,
  // but positions are preserved for existing nodes (see buildPipeline / merge below)
  useEffect(() => {
    if (!allEntities) return;

    if (canvasView === 'tree') {
      const treeData = buildCVTreeNodes(conceptValues, allEntities);
      // Preserve positions of existing tree nodes so layout doesn't jump on data refresh
      const currentNodes = usePipelineStore.getState().nodes;
      const posMap = new Map(currentNodes.map((n) => [n.id, n.position]));
      const merged = treeData.nodes.map((n) => {
        const pos = posMap.get(n.id);
        return pos ? { ...n, position: pos } : n;
      });
      setNodes(merged);
      setEdges(treeData.edges);
    } else {
      const filtered = filterEntitiesForSelectedCV(selectedCV, allEntities);
      buildPipeline({
        ...filtered,
        selectedCV,
        allIndexes: allEntities.indexes,
        allSnorkelJobs: allEntities.snorkelJobs,
      });
    }
  }, [allEntities, canvasView, selectedCV, conceptValues, buildPipeline, setNodes, setEdges]);

  const [createModal, setCreateModal] = useState<PipelineNodeType | null>(null);
  const [showRerunPipeline, setShowRerunPipeline] = useState(false);
  const pendingCreateType = useConceptStore((s) => s.pendingCreateType);
  const setPendingCreateType = useConceptStore((s) => s.setPendingCreateType);

  // When a placeholder is clicked on the canvas, open the create modal
  useEffect(() => {
    if (pendingCreateType) {
      setCreateModal(pendingCreateType);
      setPendingCreateType(null);
    }
  }, [pendingCreateType, setPendingCreateType]);

  return (
    <ReactFlowProvider>
      <div className="h-full flex flex-col">
        <div className="px-4 py-2 border-b border-gray-200 bg-white flex items-center gap-3 shrink-0">
          <span className="text-xs font-medium text-gray-500 uppercase">Concept:</span>
          <ConceptSelector />
          {activeConcept && (
            <button
              onClick={() => setShowRerunPipeline(true)}
              className="ml-auto flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-emerald-600 text-white rounded-md hover:bg-emerald-700"
            >
              <RefreshCw className="w-3.5 h-3.5" />
              Re-run Pipeline
            </button>
          )}
        </div>

        {!activeConcept ? (
          <EmptyState title="No concept selected" message="Create a concept to get started." />
        ) : isLoading ? (
          <LoadingSpinner className="flex-1" />
        ) : (
          <div className="flex flex-1 overflow-hidden relative">
            <AssetSidebar />
            <div className="flex-1 relative">
              <PipelineCanvas allEntities={allEntities} />
              <FloatingPanel />
            </div>
          </div>
        )}
      </div>

      {createModal && (
        <CreateEntityModal entityType={createModal} onClose={() => setCreateModal(null)} />
      )}

      {showRerunPipeline && (
        <RunPipelineModal target={{ type: 'all' }} onClose={() => setShowRerunPipeline(false)} />
      )}
    </ReactFlowProvider>
  );
}
