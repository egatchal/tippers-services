import { useState } from 'react';
import { ReactFlowProvider } from '@xyflow/react';
import { DndContext } from '@dnd-kit/core';
import { RefreshCw } from 'lucide-react';
import { useConceptStore } from '../stores/conceptStore';
import { useConceptData } from '../hooks/useConceptData';
import { usePollJobs } from '../hooks/usePollJobs';
import { useDragToCanvas } from '../hooks/useDragToCanvas';
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
  const cId = activeConcept?.c_id ?? null;
  const { isLoading } = useConceptData(cId);
  usePollJobs(cId);
  const handleDragEnd = useDragToCanvas();

  const [createModal, setCreateModal] = useState<PipelineNodeType | null>(null);
  const [showRerunPipeline, setShowRerunPipeline] = useState(false);

  return (
    <ReactFlowProvider>
      <DndContext onDragEnd={handleDragEnd}>
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
              <AssetSidebar onCreateEntity={(type) => setCreateModal(type)} />
              <div className="flex-1 relative">
                <PipelineCanvas />
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
      </DndContext>
    </ReactFlowProvider>
  );
}
