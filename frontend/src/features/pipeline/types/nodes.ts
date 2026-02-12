import type { Node } from '@xyflow/react';
import type { Index, Rule, Feature, LabelingFunction, SnorkelJob, ClassifierJob, ConceptValue } from './entities';
import type { NodeStatus } from '../../../shared/utils/statusColors';

export type PipelineNodeType = 'index' | 'rule' | 'feature' | 'lf' | 'snorkel' | 'classifier' | 'cv';

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

export interface FeatureNodeData extends BaseNodeData {
  entityType: 'feature';
  entity: Feature;
}

export interface LFNodeData extends BaseNodeData {
  entityType: 'lf';
  entity: LabelingFunction;
}

export interface SnorkelNodeData extends BaseNodeData {
  entityType: 'snorkel';
  entity: SnorkelJob;
}

export interface ClassifierNodeData extends BaseNodeData {
  entityType: 'classifier';
  entity: ClassifierJob;
}

export interface CVNodeData extends BaseNodeData {
  entityType: 'cv';
  entity: ConceptValue;
}

export type PipelineNodeData =
  | IndexNodeData
  | RuleNodeData
  | FeatureNodeData
  | LFNodeData
  | SnorkelNodeData
  | ClassifierNodeData
  | CVNodeData;

export type PipelineNode = Node<PipelineNodeData>;
