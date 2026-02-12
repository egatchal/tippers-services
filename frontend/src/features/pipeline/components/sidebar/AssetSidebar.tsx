import { usePipelineStore } from '../../stores/pipelineStore';
import type { PipelineNodeData, PipelineNodeType } from '../../types/nodes';
import SidebarSection from './SidebarSection';
import SidebarAssetItem from './SidebarAssetItem';

interface Props {
  onCreateEntity: (type: PipelineNodeType) => void;
}

const sections: { type: PipelineNodeType; title: string }[] = [
  { type: 'index', title: 'Indexes' },
  { type: 'rule', title: 'Rules' },
  { type: 'feature', title: 'Features' },
  { type: 'cv', title: 'Concept Values' },
  { type: 'lf', title: 'Labeling Functions' },
  { type: 'snorkel', title: 'Snorkel Runs' },
  { type: 'classifier', title: 'Classifiers' },
];

export default function AssetSidebar({ onCreateEntity }: Props) {
  const { nodes, selectNode } = usePipelineStore();

  return (
    <div className="w-[220px] border-r border-gray-200 bg-white overflow-y-auto shrink-0">
      <div className="px-3 py-2 border-b border-gray-200">
        <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Assets</h3>
      </div>
      {sections.map(({ type, title }) => {
        const items = nodes.filter((n) => (n.data as PipelineNodeData).entityType === type);
        return (
          <SidebarSection
            key={type}
            title={title}
            count={items.length}
            onAdd={() => onCreateEntity(type)}
          >
            {items.length === 0 ? (
              <p className="text-[11px] text-gray-400 px-2 py-1">None yet</p>
            ) : (
              items.map((node) => {
                const data = node.data as PipelineNodeData;
                return (
                  <SidebarAssetItem
                    key={node.id}
                    nodeId={node.id}
                    label={data.label}
                    status={data.status}
                    onClick={() => selectNode(node.id)}
                  />
                );
              })
            )}
          </SidebarSection>
        );
      })}
    </div>
  );
}
