import { useState, useRef, useCallback, useEffect } from 'react';
import { usePipelineStore, getSelectedEntity } from '../../stores/pipelineStore';
import type { PipelineNodeData } from '../../types/nodes';
import PanelHeader from './PanelHeader';
import IndexPanel from './IndexPanel';
import RulePanel from './RulePanel';
import FeaturePanel from './FeaturePanel';
import LabelingFunctionPanel from './LabelingFunctionPanel';
import SnorkelRunPanel from './SnorkelRunPanel';
import ClassifierRunPanel from './ClassifierRunPanel';
import ConceptValuePanel from './ConceptValuePanel';

export default function FloatingPanel() {
  const store = usePipelineStore();
  const entity = getSelectedEntity(store);
  const closePanel = store.closePanel;

  const [pos, setPos] = useState({ x: 20, y: 20 });
  const dragRef = useRef<{ startX: number; startY: number; origX: number; origY: number } | null>(null);
  const panelRef = useRef<HTMLDivElement>(null);

  const onMouseDown = useCallback((e: React.MouseEvent) => {
    dragRef.current = { startX: e.clientX, startY: e.clientY, origX: pos.x, origY: pos.y };
    e.preventDefault();
  }, [pos]);

  useEffect(() => {
    const onMouseMove = (e: MouseEvent) => {
      if (!dragRef.current) return;
      setPos({
        x: dragRef.current.origX + (e.clientX - dragRef.current.startX),
        y: dragRef.current.origY + (e.clientY - dragRef.current.startY),
      });
    };
    const onMouseUp = () => { dragRef.current = null; };
    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
    return () => { window.removeEventListener('mousemove', onMouseMove); window.removeEventListener('mouseup', onMouseUp); };
  }, []);

  if (!store.panelOpen || !entity) return null;

  return (
    <div
      ref={panelRef}
      className="absolute z-20 w-[480px] max-h-[calc(100%-40px)] bg-white border border-gray-200 rounded-lg shadow-lg overflow-y-auto"
      style={{ left: pos.x, top: pos.y }}
    >
      <PanelHeader
        entityType={entity.entityType}
        label={entity.label}
        onClose={closePanel}
        dragHandleProps={{ onMouseDown }}
      />
      <PanelContent entity={entity} />
    </div>
  );
}

function PanelContent({ entity }: { entity: PipelineNodeData }) {
  switch (entity.entityType) {
    case 'index': return <IndexPanel entity={entity.entity} />;
    case 'rule': return <RulePanel entity={entity.entity} />;
    case 'feature': return <FeaturePanel entity={entity.entity} />;
    case 'lf': return <LabelingFunctionPanel entity={entity.entity} />;
    case 'snorkel': return <SnorkelRunPanel entity={entity.entity} />;
    case 'classifier': return <ClassifierRunPanel entity={entity.entity} />;
    case 'cv': return <ConceptValuePanel entity={entity.entity} />;
  }
}
