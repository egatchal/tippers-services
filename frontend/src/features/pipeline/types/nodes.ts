import type { Node } from '@xyflow/react';
import type { Index, Rule, LabelingFunction, SnorkelJob, ConceptValue } from './entities';
import type { NodeStatus } from '../../../shared/utils/statusColors';

export type PipelineNodeType = 'index' | 'rule' | 'lf' | 'snorkel' | 'cv' | 'cvTree' | 'placeholder';

interface BaseNodeData {
  entityType: PipelineNodeType;
  label: string;
  status: NodeStatus;
  summary?: string;
  metrics?: Array<{ label: string; value: string | number }>;
  [key: string]: unknown;
}

export interface IndexNodeData extends BaseNodeData {
  entityType: 'index';
  entity: Index;
}

export interface RuleNodeData extends BaseNodeData {
  entityType: 'rule';
  entity: Rule;
}

export interface LFNodeData extends BaseNodeData {
  entityType: 'lf';
  entity: LabelingFunction;
}

export interface SnorkelNodeData extends BaseNodeData {
  entityType: 'snorkel';
  entity: SnorkelJob;
}

export interface CVNodeData extends BaseNodeData {
  entityType: 'cv';
  entity: ConceptValue;
}

export interface CVTreeNodeData extends BaseNodeData {
  entityType: 'cvTree';
  entity: ConceptValue;
  hasCompletedSnorkel: boolean;
  ruleCount: number;
  lfCount: number;
  snorkelStatus: 'completed' | 'running' | 'none';
}

export interface PlaceholderNodeData extends BaseNodeData {
  entityType: 'placeholder';
  targetType: PipelineNodeType;
  hideTopHandle?: boolean;
  hideBottomHandle?: boolean;
}

export type PipelineNodeData =
  | IndexNodeData
  | RuleNodeData
  | LFNodeData
  | SnorkelNodeData
  | CVNodeData
  | CVTreeNodeData
  | PlaceholderNodeData;

export type PipelineNode = Node<PipelineNodeData>;
