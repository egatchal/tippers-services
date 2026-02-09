# Dagster Migration - Complete Implementation Guide

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Database Schema](#database-schema)
3. [React Flow UI](#react-flow-ui)
4. [FastAPI Endpoints](#fastapi-endpoints)
5. [Dagster Implementation](#dagster-implementation)
6. [Migration Steps](#migration-steps)

---

## Architecture Overview

### The Flow

```
┌─────────────────────────────────────────────────────┐
│              REACT FLOW UI (User Interface)          │
│                                                      │
│  Users visually build pipelines:                    │
│  - Drag/drop indexes, rules, features               │
│  - Connect nodes to define flow                     │
│  - Configure labeling functions                     │
│  - Choose output format (softmax/hard labels)       │
│                                                      │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│              FASTAPI (REST API Layer)                │
│                                                      │
│  - Validates definitions (SQL, Python code)          │
│  - Saves to PostgreSQL                               │
│  - Triggers Dagster materialization                  │
│  - Returns status updates                            │
│                                                      │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│              POSTGRESQL (Storage)                    │
│                                                      │
│  - Database connections                              │
│  - Index definitions + SQL queries                   │
│  - Rule/feature definitions + SQL queries            │
│  - Labeling function definitions                     │
│  - Materialization status                            │
│                                                      │
└──────────────────┬──────────────────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────────────────┐
│              DAGSTER (Execution Engine)              │
│                                                      │
│  - Reads definitions from run_config                 │
│  - Executes SQL queries (indexes, rules, features)   │
│  - Materializes data to S3                           │
│  - Creates and applies labeling functions            │
│  - Trains Snorkel models                             │
│  - Logs to MLflow                                    │
│  - Returns results (hard labels or softmax)          │
│                                                      │
└─────────────────────────────────────────────────────┘
```

### Key Principles

1. **React Flow** - Visual pipeline builder (not Dagster UI)
2. **FastAPI** - Creation, validation, persistence
3. **PostgreSQL** - Single source of truth for definitions
4. **Dagster** - Execution only (reads definitions, runs pipelines)
5. **Assets** - Indexes, Rules/Features are materialized and reusable
6. **LFs** - Managed through UI but not tracked as separate materialized assets
7. **Softmax** - Optional probabilistic output from Snorkel

---

## Database Schema

### Core Tables

```sql
-- Database connections
CREATE TABLE database_connections (
    conn_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    connection_type VARCHAR(50) NOT NULL, -- postgresql, mysql, etc.
    host VARCHAR(255) NOT NULL,
    port INTEGER NOT NULL,
    database VARCHAR(255) NOT NULL,
    user VARCHAR(255) NOT NULL,
    encrypted_password TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name)
);

-- Indexes (materialized queries)
CREATE TABLE concept_indexes (
    index_id SERIAL PRIMARY KEY,
    c_id INTEGER REFERENCES concepts(c_id) ON DELETE CASCADE,
    conn_id INTEGER REFERENCES database_connections(conn_id),
    name VARCHAR(255) NOT NULL,
    sql_query TEXT NOT NULL,
    query_template_params JSONB,
    partition_type VARCHAR(50),           -- 'time', 'id_range', 'categorical', null
    partition_config JSONB,               -- Config for partitioning
    storage_path VARCHAR(500),
    is_materialized BOOLEAN DEFAULT FALSE,
    materialized_at TIMESTAMP,
    row_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_index_name UNIQUE(c_id, name)
);

-- Rules/Features (materialized queries)
CREATE TABLE concept_rules (
    r_id SERIAL PRIMARY KEY,
    c_id INTEGER REFERENCES concepts(c_id) ON DELETE CASCADE,
    conn_id INTEGER REFERENCES database_connections(conn_id),
    name VARCHAR(255) NOT NULL,
    sql_query TEXT NOT NULL,
    query_template_params JSONB,
    partition_type VARCHAR(50),
    partition_config JSONB,
    storage_path VARCHAR(500),
    is_materialized BOOLEAN DEFAULT FALSE,
    materialized_at TIMESTAMP,
    row_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_rule_name UNIQUE(c_id, name)
);

-- Labeling functions (NOT materialized assets, just definitions)
CREATE TABLE labeling_functions (
    lf_id SERIAL PRIMARY KEY,
    c_id INTEGER REFERENCES concepts(c_id) ON DELETE CASCADE,
    cv_id INTEGER REFERENCES concept_values(cv_id),
    name VARCHAR(255) NOT NULL,
    lf_type VARCHAR(50) NOT NULL,         -- 'template', 'regex', 'custom'
    lf_config JSONB NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    requires_approval BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_lf_name UNIQUE(c_id, name)
);

-- Snorkel training jobs
CREATE TABLE snorkel_jobs (
    job_id SERIAL PRIMARY KEY,
    c_id INTEGER REFERENCES concepts(c_id),
    index_id INTEGER REFERENCES concept_indexes(index_id),
    rule_ids INTEGER[],
    lf_ids INTEGER[],
    config JSONB,
    output_type VARCHAR(50) DEFAULT 'hard_labels', -- 'hard_labels' or 'softmax'
    dagster_run_id VARCHAR(255),
    status VARCHAR(50) DEFAULT 'PENDING',
    result_path VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP
);

-- Partition tracking
CREATE TABLE partition_executions (
    execution_id SERIAL PRIMARY KEY,
    index_id INTEGER REFERENCES concept_indexes(index_id) ON DELETE CASCADE,
    r_id INTEGER REFERENCES concept_rules(r_id) ON DELETE CASCADE,
    partition_key VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    row_count INTEGER,
    storage_path VARCHAR(500),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    CONSTRAINT unique_index_partition UNIQUE(index_id, partition_key),
    CONSTRAINT unique_rule_partition UNIQUE(r_id, partition_key)
);

CREATE INDEX idx_index_materialized ON concept_indexes(is_materialized);
CREATE INDEX idx_rule_materialized ON concept_rules(is_materialized);
CREATE INDEX idx_lf_active ON labeling_functions(is_active);
CREATE INDEX idx_snorkel_job_status ON snorkel_jobs(status);
```

---

## React Flow UI

### Asset Catalog + Visual Pipeline Builder

```tsx
// components/SnorkelPipelineBuilder.tsx
import React, { useState, useCallback } from 'react';
import ReactFlow, {
  Node,
  Edge,
  addEdge,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  Connection,
} from 'reactflow';
import 'reactflow/dist/style.css';

// Custom node types
import IndexNode from './nodes/IndexNode';
import RuleNode from './nodes/RuleNode';
import LFConfigNode from './nodes/LFConfigNode';
import TrainNode from './nodes/TrainNode';

const nodeTypes = {
  index: IndexNode,
  rule: RuleNode,
  lfConfig: LFConfigNode,
  train: TrainNode,
};

interface Asset {
  id: number;
  name: string;
  is_materialized: boolean;
  row_count?: number;
}

export const SnorkelPipelineBuilder: React.FC = () => {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  // Asset catalog data
  const [indexes, setIndexes] = useState<Asset[]>([]);
  const [rules, setRules] = useState<Asset[]>([]);
  const [outputType, setOutputType] = useState<'hard_labels' | 'softmax'>('softmax');

  // Load catalog on mount
  useEffect(() => {
    loadCatalog();
    const interval = setInterval(loadCatalog, 5000);
    return () => clearInterval(interval);
  }, []);

  const loadCatalog = async () => {
    const response = await fetch('/concepts/1/catalog');
    const data = await response.json();
    setIndexes(data.indexes);
    setRules(data.rules);
  };

  // Add node from catalog
  const addIndexNode = (index: Asset) => {
    const newNode: Node = {
      id: `index-${index.id}`,
      type: 'index',
      position: { x: 100, y: 100 },
      data: {
        label: index.name,
        assetId: index.id,
        isMaterialized: index.is_materialized,
        rowCount: index.row_count,
        onMaterialize: () => materializeAsset('index', index.id),
      },
    };
    setNodes((nds) => [...nds, newNode]);
  };

  const addRuleNode = (rule: Asset) => {
    const newNode: Node = {
      id: `rule-${rule.id}`,
      type: 'rule',
      position: { x: 400, y: 100 },
      data: {
        label: rule.name,
        assetId: rule.id,
        isMaterialized: rule.is_materialized,
        rowCount: rule.row_count,
        onMaterialize: () => materializeAsset('rule', rule.id),
      },
    };
    setNodes((nds) => [...nds, newNode]);
  };

  const addLFConfigNode = () => {
    const newNode: Node = {
      id: `lf-config-${Date.now()}`,
      type: 'lfConfig',
      position: { x: 700, y: 100 },
      data: {
        labelingFunctions: [],
        onAddLF: handleAddLF,
        onRemoveLF: handleRemoveLF,
      },
    };
    setNodes((nds) => [...nds, newNode]);
  };

  const addTrainNode = () => {
    const newNode: Node = {
      id: 'train',
      type: 'train',
      position: { x: 1000, y: 100 },
      data: {
        outputType,
        onOutputTypeChange: setOutputType,
        onTrain: handleTrain,
      },
    };
    setNodes((nds) => [...nds, newNode]);
  };

  const onConnect = useCallback(
    (params: Connection) => setEdges((eds) => addEdge(params, eds)),
    [setEdges]
  );

  const materializeAsset = async (type: 'index' | 'rule', assetId: number) => {
    const endpoint = type === 'index'
      ? `/concepts/1/indexes/${assetId}/materialize`
      : `/concepts/1/rules/${assetId}/materialize`;

    const response = await fetch(endpoint, { method: 'POST' });
    const result = await response.json();

    console.log('Materialization started:', result.dagster_run_id);

    // Poll for completion
    pollMaterializationStatus(result.dagster_run_id);
  };

  const pollMaterializationStatus = async (runId: string) => {
    const interval = setInterval(async () => {
      const response = await fetch(`/dagster/runs/${runId}`);
      const status = await response.json();

      if (status.status === 'SUCCESS') {
        clearInterval(interval);
        loadCatalog(); // Refresh catalog
      } else if (status.status === 'FAILURE') {
        clearInterval(interval);
        alert('Materialization failed');
      }
    }, 3000);
  };

  const handleAddLF = async (lfConfig: any) => {
    // Create LF via API
    const response = await fetch(`/concepts/1/labeling-functions/${lfConfig.type}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(lfConfig),
    });

    const lf = await response.json();
    return lf;
  };

  const handleRemoveLF = (lfId: number) => {
    // Remove LF from configuration
  };

  const handleTrain = async () => {
    // Extract pipeline configuration from React Flow graph
    const indexNode = nodes.find(n => n.type === 'index');
    const ruleNodes = nodes.filter(n => n.type === 'rule');
    const lfConfigNode = nodes.find(n => n.type === 'lfConfig');

    if (!indexNode || ruleNodes.length === 0 || !lfConfigNode) {
      alert('Please configure complete pipeline: Index → Rules → LFs → Train');
      return;
    }

    // Check if all assets are materialized
    if (!indexNode.data.isMaterialized) {
      alert('Index is not materialized. Click "Materialize" on the index node.');
      return;
    }

    for (const ruleNode of ruleNodes) {
      if (!ruleNode.data.isMaterialized) {
        alert(`Rule "${ruleNode.data.label}" is not materialized.`);
        return;
      }
    }

    // Build training request
    const trainingRequest = {
      selectedIndex: indexNode.data.assetId,
      selectedRules: ruleNodes.map(n => n.data.assetId),
      selectedLFs: lfConfigNode.data.labelingFunctions.map((lf: any) => lf.lf_id),
      snorkel: {
        epochs: 100,
        lr: 0.01,
        sample_size: 10000,
        output_type: outputType,
      },
    };

    // Submit training job
    const response = await fetch('/concepts/1/snorkel/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(trainingRequest),
    });

    const result = await response.json();
    console.log('Training started:', result.job_id, result.dagster_run_id);

    // Navigate to results page
    window.location.href = `/concepts/1/snorkel/jobs/${result.job_id}`;
  };

  return (
    <div className="pipeline-builder" style={{ height: '100vh' }}>
      {/* Asset Catalog Sidebar */}
      <div className="catalog-sidebar">
        <h3>Asset Catalog</h3>

        <div className="catalog-section">
          <h4>Indexes</h4>
          {indexes.map(index => (
            <div key={index.id} className="catalog-item">
              <span className={index.is_materialized ? 'ready' : 'not-ready'}>
                {index.is_materialized ? '✓' : '○'}
              </span>
              <span>{index.name}</span>
              <button onClick={() => addIndexNode(index)}>Add</button>
            </div>
          ))}
          <button onClick={() => openCreateDialog('index')}>+ Create Index</button>
        </div>

        <div className="catalog-section">
          <h4>Rules / Features</h4>
          {rules.map(rule => (
            <div key={rule.id} className="catalog-item">
              <span className={rule.is_materialized ? 'ready' : 'not-ready'}>
                {rule.is_materialized ? '✓' : '○'}
              </span>
              <span>{rule.name}</span>
              <button onClick={() => addRuleNode(rule)}>Add</button>
            </div>
          ))}
          <button onClick={() => openCreateDialog('rule')}>+ Create Rule</button>
        </div>

        <div className="catalog-section">
          <h4>Pipeline Nodes</h4>
          <button onClick={addLFConfigNode}>+ LF Configuration</button>
          <button onClick={addTrainNode}>+ Train Node</button>
        </div>
      </div>

      {/* React Flow Canvas */}
      <div style={{ flexGrow: 1 }}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          nodeTypes={nodeTypes}
          fitView
        >
          <Background />
          <Controls />
          <MiniMap />
        </ReactFlow>
      </div>
    </div>
  );
};
```

### Custom Node Components

```tsx
// components/nodes/IndexNode.tsx
import React from 'react';
import { Handle, Position } from 'reactflow';

export const IndexNode: React.FC<{ data: any }> = ({ data }) => {
  return (
    <div className="custom-node index-node">
      <div className="node-header">
        <span className="node-icon">📊</span>
        <span className="node-title">Index</span>
      </div>

      <div className="node-content">
        <strong>{data.label}</strong>
        {data.isMaterialized ? (
          <div className="node-status ready">
            ✓ Ready ({data.rowCount?.toLocaleString()} rows)
          </div>
        ) : (
          <div className="node-status not-ready">
            ○ Not Materialized
            <button onClick={data.onMaterialize}>Materialize</button>
          </div>
        )}
      </div>

      <Handle type="source" position={Position.Right} />
    </div>
  );
};

// components/nodes/RuleNode.tsx
export const RuleNode: React.FC<{ data: any }> = ({ data }) => {
  return (
    <div className="custom-node rule-node">
      <Handle type="target" position={Position.Left} />

      <div className="node-header">
        <span className="node-icon">🔧</span>
        <span className="node-title">Rule</span>
      </div>

      <div className="node-content">
        <strong>{data.label}</strong>
        {data.isMaterialized ? (
          <div className="node-status ready">
            ✓ Ready ({data.rowCount?.toLocaleString()} rows)
          </div>
        ) : (
          <div className="node-status not-ready">
            ○ Not Materialized
            <button onClick={data.onMaterialize}>Materialize</button>
          </div>
        )}
      </div>

      <Handle type="source" position={Position.Right} />
    </div>
  );
};

// components/nodes/LFConfigNode.tsx
export const LFConfigNode: React.FC<{ data: any }> = ({ data }) => {
  const [showDialog, setShowDialog] = useState(false);

  return (
    <div className="custom-node lf-config-node">
      <Handle type="target" position={Position.Left} />

      <div className="node-header">
        <span className="node-icon">🏷️</span>
        <span className="node-title">Labeling Functions</span>
      </div>

      <div className="node-content">
        <div className="lf-list">
          {data.labelingFunctions.map((lf: any) => (
            <div key={lf.lf_id} className="lf-item">
              <span>{lf.name}</span>
              <button onClick={() => data.onRemoveLF(lf.lf_id)}>✕</button>
            </div>
          ))}
        </div>

        <button onClick={() => setShowDialog(true)}>+ Add LF</button>

        {data.labelingFunctions.length === 0 && (
          <div className="node-warning">No LFs configured</div>
        )}
      </div>

      <Handle type="source" position={Position.Right} />
    </div>
  );
};

// components/nodes/TrainNode.tsx
export const TrainNode: React.FC<{ data: any }> = ({ data }) => {
  return (
    <div className="custom-node train-node">
      <Handle type="target" position={Position.Left} />

      <div className="node-header">
        <span className="node-icon">🚀</span>
        <span className="node-title">Train Snorkel</span>
      </div>

      <div className="node-content">
        <div className="output-type-selector">
          <label>
            <input
              type="radio"
              name="output"
              value="softmax"
              checked={data.outputType === 'softmax'}
              onChange={(e) => data.onOutputTypeChange(e.target.value)}
            />
            Softmax (Probabilities)
          </label>
          <label>
            <input
              type="radio"
              name="output"
              value="hard_labels"
              checked={data.outputType === 'hard_labels'}
              onChange={(e) => data.onOutputTypeChange(e.target.value)}
            />
            Hard Labels
          </label>
        </div>

        <button className="train-button" onClick={data.onTrain}>
          Train Model
        </button>
      </div>
    </div>
  );
};
```

---

## FastAPI Endpoints

### Asset Catalog

```python
# backend/routers/catalog.py
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from backend.db.session import get_db

router = APIRouter()

@router.get("/concepts/{c_id}/catalog")
async def get_asset_catalog(c_id: int, db: Session = Depends(get_db)):
    """Get all assets for concept (indexes, rules)."""
    indexes = db.query(ConceptIndexes).filter(ConceptIndexes.c_id == c_id).all()
    rules = db.query(ConceptRules).filter(ConceptRules.c_id == c_id).all()

    return {
        "indexes": [
            {
                "id": idx.index_id,
                "name": idx.name,
                "is_materialized": idx.is_materialized,
                "row_count": idx.row_count,
                "storage_path": idx.storage_path,
            }
            for idx in indexes
        ],
        "rules": [
            {
                "id": rule.r_id,
                "name": rule.name,
                "is_materialized": rule.is_materialized,
                "row_count": rule.row_count,
                "storage_path": rule.storage_path,
            }
            for rule in rules
        ]
    }
```

### Create and Materialize Assets

```python
# backend/routers/indexes.py

@router.post("/concepts/{c_id}/indexes")
async def create_index(c_id: int, request: IndexCreate, db: Session = Depends(get_db)):
    """Create new index definition."""
    validate_sql_syntax(request.sql_query)

    index = ConceptIndexes(
        c_id=c_id,
        name=request.name,
        conn_id=request.conn_id,
        sql_query=request.sql_query,
        query_template_params=request.query_template_params,
        partition_type=request.partition_type,
        partition_config=request.partition_config,
        is_materialized=False
    )

    db.add(index)
    db.commit()
    db.refresh(index)

    return {"index_id": index.index_id, "name": index.name}

@router.post("/concepts/{c_id}/indexes/{index_id}/materialize")
async def materialize_index(index_id: int, db: Session = Depends(get_db)):
    """Trigger Dagster to materialize index."""
    index = db.get(ConceptIndexes, index_id)
    if not index:
        raise HTTPException(404, "Index not found")

    conn = db.get(DatabaseConnection, index.conn_id)

    # Build Dagster run config
    run_config = {
        "resources": {
            "database_connection": {
                "config": {
                    "host": conn.host,
                    "port": conn.port,
                    "database": conn.database,
                    "user": conn.user,
                    "password": decrypt_password(conn.encrypted_password),
                }
            }
        },
        "ops": {
            "materialized_index": {
                "config": {
                    "index_id": index_id,
                    "sql_query": index.sql_query,
                    "query_template_params": index.query_template_params or {},
                }
            }
        }
    }

    # Launch Dagster
    dagster_client = get_dagster_client()
    result = dagster_client.submit_job_execution(
        job_name="materialize_index",
        run_config=run_config
    )

    index.dagster_run_id = result.run_id
    db.commit()

    return {"dagster_run_id": result.run_id, "status": "STARTED"}

# Similar endpoints for rules:
# POST /concepts/{c_id}/rules
# POST /concepts/{c_id}/rules/{r_id}/materialize
```

### Labeling Functions

```python
# backend/routers/labeling_functions.py

@router.post("/concepts/{c_id}/labeling-functions/template")
async def create_template_lf(c_id: int, request: TemplateLFCreate, db: Session = Depends(get_db)):
    """Create template-based LF."""
    lf = LabelingFunction(
        c_id=c_id,
        cv_id=request.cv_id,
        name=request.name,
        lf_type="template",
        lf_config={
            "template": request.template,
            "field": request.field,
            "operator": request.operator,
            "value": request.value,
            "label": request.label
        },
        is_active=True
    )

    db.add(lf)
    db.commit()
    db.refresh(lf)

    return {"lf_id": lf.lf_id, "name": lf.name, "is_active": True}

@router.post("/concepts/{c_id}/labeling-functions/regex")
async def create_regex_lf(c_id: int, request: RegexLFCreate, db: Session = Depends(get_db)):
    """Create regex-based LF."""
    lf = LabelingFunction(
        c_id=c_id,
        cv_id=request.cv_id,
        name=request.name,
        lf_type="regex",
        lf_config={
            "pattern": request.pattern,
            "field": request.field,
            "label": request.label
        },
        is_active=True
    )

    db.add(lf)
    db.commit()
    db.refresh(lf)

    return {"lf_id": lf.lf_id, "name": lf.name, "is_active": True}

@router.post("/concepts/{c_id}/labeling-functions/custom")
async def create_custom_lf(c_id: int, request: CustomLFCreate, db: Session = Depends(get_db)):
    """Create custom Python LF (requires approval)."""
    validate_custom_python(request.code, set(request.allowed_imports))

    lf = LabelingFunction(
        c_id=c_id,
        cv_id=request.cv_id,
        name=request.name,
        lf_type="custom",
        lf_config={
            "code": request.code,
            "allowed_imports": request.allowed_imports,
            "label": request.label
        },
        is_active=False,  # Requires approval
        requires_approval=True
    )

    db.add(lf)
    db.commit()
    db.refresh(lf)

    return {"lf_id": lf.lf_id, "name": lf.name, "is_active": False, "requires_approval": True}
```

### Snorkel Training

```python
# backend/routers/snorkel.py

@router.post("/concepts/{c_id}/snorkel/run")
async def run_snorkel_training(c_id: int, request: SnorkelRequest, db: Session = Depends(get_db)):
    """Trigger Snorkel training with configured pipeline."""
    # Validate assets are materialized
    index = db.get(ConceptIndexes, request.selectedIndex)
    if not index.is_materialized:
        raise HTTPException(400, "Index not materialized")

    rules = db.query(ConceptRules).filter(
        ConceptRules.r_id.in_(request.selectedRules)
    ).all()

    for rule in rules:
        if not rule.is_materialized:
            raise HTTPException(400, f"Rule {rule.name} not materialized")

    # Get active labeling functions
    lfs = db.query(LabelingFunction).filter(
        LabelingFunction.lf_id.in_(request.selectedLFs),
        LabelingFunction.is_active == True
    ).all()

    # Build run config
    run_config = {
        "resources": {...},
        "ops": {
            "materialized_index": {
                "config": {"storage_path": index.storage_path}
            },
            "labeling_functions": {
                "config": {
                    "lf_definitions": [
                        {"lf_id": lf.lf_id, "lf_type": lf.lf_type, "lf_config": lf.lf_config}
                        for lf in lfs
                    ]
                }
            },
            "trained_snorkel_model": {
                "config": {
                    "epochs": request.snorkel.epochs,
                    "lr": request.snorkel.lr,
                    "output_type": request.snorkel.output_type,
                }
            }
        }
    }

    # Create job
    job = SnorkelJob(
        c_id=c_id,
        index_id=request.selectedIndex,
        rule_ids=request.selectedRules,
        lf_ids=request.selectedLFs,
        config=run_config,
        output_type=request.snorkel.output_type,
        status="RUNNING"
    )
    db.add(job)
    db.commit()

    # Launch Dagster
    dagster_client = get_dagster_client()
    result = dagster_client.submit_job_execution(
        job_name="snorkel_training_pipeline",
        run_config=run_config
    )

    job.dagster_run_id = result.run_id
    db.commit()

    return {"job_id": job.job_id, "dagster_run_id": result.run_id}

@router.get("/concepts/{c_id}/snorkel/jobs/{job_id}")
async def get_snorkel_results(c_id: int, job_id: int, db: Session = Depends(get_db)):
    """Get Snorkel training results."""
    job = db.get(SnorkelJob, job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    results = load_results_from_s3(job.result_path)

    if job.output_type == 'softmax':
        predictions = [
            {
                "sample_id": idx,
                "probs": probs.tolist(),
                "predicted_class": int(np.argmax(probs)),
                "confidence": float(np.max(probs))
            }
            for idx, probs in enumerate(results['probabilities'])
        ]

        return {
            "job_id": job_id,
            "output_type": "softmax",
            "predictions": predictions,
            "summary": {
                "total_samples": len(predictions),
                "avg_confidence": float(np.mean([p['confidence'] for p in predictions]))
            }
        }
    else:
        return {
            "job_id": job_id,
            "output_type": "hard_labels",
            "predictions": results['labels'].tolist()
        }
```

---

## Dagster Implementation

### Assets

```python
# backend/dagster/assets/indexes.py
from dagster import asset, AssetExecutionContext, MetadataValue
import pandas as pd

@asset(group_name="data_preparation", compute_kind="sql")
def materialized_index(context: AssetExecutionContext, database_connection, s3_storage) -> pd.DataFrame:
    """Execute SQL query and materialize to S3."""
    config = context.op_config
    index_id = config["index_id"]
    sql_query = config["sql_query"]
    query_params = config.get("query_template_params", {})

    # Render SQL template
    from jinja2 import Template
    rendered_query = Template(sql_query).render(**query_params)

    # Execute
    df = execute_query(database_connection, rendered_query)

    # Upload to S3
    storage_path = f"indexes/index_{index_id}/data.parquet"
    upload_dataframe(s3_storage, df, storage_path)

    # Update database
    db = get_db_session()
    index = db.get(ConceptIndexes, index_id)
    index.is_materialized = True
    index.materialized_at = datetime.now()
    index.row_count = len(df)
    index.storage_path = storage_path
    db.commit()

    context.add_output_metadata({
        "num_rows": MetadataValue.int(len(df)),
        "storage_path": MetadataValue.text(storage_path)
    })

    return df

# Similar for rules
@asset(group_name="data_preparation", compute_kind="sql")
def materialized_rule(context: AssetExecutionContext, database_connection, s3_storage) -> pd.DataFrame:
    """Execute rule SQL query and materialize to S3."""
    # Similar implementation
    pass
```

### Labeling Functions

```python
# backend/dagster/assets/snorkel.py

@asset(group_name="snorkel", compute_kind="python")
def labeling_functions(
    context: AssetExecutionContext,
    materialized_index: pd.DataFrame
) -> List[Any]:
    """Create labeling functions from definitions."""
    lf_definitions = context.op_config.get("lf_definitions", [])

    lfs = []
    for lf_def in lf_definitions:
        lf = create_lf_from_definition(lf_def, materialized_index)
        lfs.append(lf)

    context.add_output_metadata({
        "num_lfs": MetadataValue.int(len(lfs))
    })

    return lfs

def create_lf_from_definition(lf_def: dict, df: pd.DataFrame) -> Any:
    """Factory to create LF based on type."""
    from snorkel.labeling import labeling_function

    if lf_def['lf_type'] == 'template':
        config = lf_def['lf_config']

        @labeling_function(name=lf_def['name'])
        def lf(x):
            field_value = getattr(x, config['field'])
            operator = config['operator']
            value = config['value']

            if operator == '>':
                return config['label'] if field_value > value else -1
            elif operator == '<':
                return config['label'] if field_value < value else -1
            elif operator == '==':
                return config['label'] if field_value == value else -1
            return -1

        return lf

    elif lf_def['lf_type'] == 'regex':
        import re
        config = lf_def['lf_config']
        pattern = re.compile(config['pattern'])

        @labeling_function(name=lf_def['name'])
        def lf(x):
            field_value = str(getattr(x, config['field']))
            return config['label'] if pattern.search(field_value) else -1

        return lf

    elif lf_def['lf_type'] == 'custom':
        # Execute custom Python in sandbox
        config = lf_def['lf_config']
        lf_func = execute_custom_lf_safely(config['code'], config['allowed_imports'])
        return lf_func

    return None
```

### Snorkel Training with Softmax

```python
# backend/dagster/assets/snorkel.py

@asset(group_name="snorkel", compute_kind="ml")
def snorkel_label_matrix(
    context: AssetExecutionContext,
    materialized_index: pd.DataFrame,
    labeling_functions: List[Any]
) -> Dict[str, Any]:
    """Apply labeling functions to create label matrix."""
    from snorkel.labeling import PandasLFApplier

    applier = PandasLFApplier(lfs=labeling_functions)
    L = applier.apply(df=materialized_index)

    context.add_output_metadata({
        "num_samples": MetadataValue.int(len(L)),
        "num_lfs": MetadataValue.int(L.shape[1]),
        "coverage": MetadataValue.float((L != -1).any(axis=1).mean())
    })

    return {"L": L, "df": materialized_index}

@asset(group_name="snorkel", compute_kind="ml")
def trained_snorkel_model(
    context: AssetExecutionContext,
    snorkel_label_matrix: Dict[str, Any]
) -> Dict[str, Any]:
    """Train Snorkel model and generate predictions."""
    from snorkel.labeling.model import LabelModel

    config = context.op_config
    output_type = config.get("output_type", "hard_labels")
    epochs = config.get("epochs", 100)
    lr = config.get("lr", 0.01)

    L = snorkel_label_matrix["L"]
    df = snorkel_label_matrix["df"]

    # Train
    label_model = LabelModel(cardinality=2, verbose=True)
    label_model.fit(L_train=L, n_epochs=epochs, lr=lr)

    # Generate output based on type
    if output_type == "softmax":
        probabilities = label_model.predict_proba(L)
        predictions = np.argmax(probabilities, axis=1)
        confidences = np.max(probabilities, axis=1)

        context.add_output_metadata({
            "output_type": MetadataValue.text("softmax"),
            "avg_confidence": MetadataValue.float(float(confidences.mean()))
        })

        return {
            "predictions": predictions,
            "probabilities": probabilities,
            "confidences": confidences,
            "df": df,
            "output_type": "softmax"
        }
    else:
        predictions = label_model.predict(L)

        context.add_output_metadata({
            "output_type": MetadataValue.text("hard_labels")
        })

        return {
            "predictions": predictions,
            "df": df,
            "output_type": "hard_labels"
        }

@asset(group_name="snorkel", compute_kind="storage")
def snorkel_model_results(
    context: AssetExecutionContext,
    trained_snorkel_model: Dict[str, Any],
    s3_storage
) -> int:
    """Save results to S3 and update database."""
    job_id = context.op_config.get("job_id")

    results_df = trained_snorkel_model["df"].copy()
    results_df['predicted_label'] = trained_snorkel_model["predictions"]

    if trained_snorkel_model["output_type"] == "softmax":
        probs = trained_snorkel_model["probabilities"]
        for i in range(probs.shape[1]):
            results_df[f'prob_class_{i}'] = probs[:, i]
        results_df['confidence'] = trained_snorkel_model["confidences"]

    # Upload to S3
    storage_path = f"snorkel_results/job_{job_id}/results.parquet"
    upload_dataframe(s3_storage, results_df, storage_path)

    # Update database
    db = get_db_session()
    job = db.get(SnorkelJob, job_id)
    job.status = "SUCCESS"
    job.result_path = storage_path
    job.completed_at = datetime.now()
    db.commit()

    return job_id
```

### Jobs

```python
# backend/dagster/jobs.py
from dagster import define_asset_job, AssetSelection

materialize_index_job = define_asset_job(
    name="materialize_index",
    selection=AssetSelection.assets(materialized_index)
)

materialize_rule_job = define_asset_job(
    name="materialize_rule",
    selection=AssetSelection.assets(materialized_rule)
)

snorkel_training_pipeline = define_asset_job(
    name="snorkel_training_pipeline",
    selection=AssetSelection.groups("data_preparation", "snorkel")
)
```

---

## Migration Steps

### Phase 1: Database Setup (Week 1)

```sql
-- Run migrations
-- 1. Add new columns to existing tables
ALTER TABLE concept_indexes ADD COLUMN partition_type VARCHAR(50);
ALTER TABLE concept_indexes ADD COLUMN partition_config JSONB;
ALTER TABLE concept_indexes ADD COLUMN is_materialized BOOLEAN DEFAULT FALSE;
ALTER TABLE concept_indexes ADD COLUMN storage_path VARCHAR(500);

ALTER TABLE concept_rules ADD COLUMN partition_type VARCHAR(50);
ALTER TABLE concept_rules ADD COLUMN partition_config JSONB;
ALTER TABLE concept_rules ADD COLUMN is_materialized BOOLEAN DEFAULT FALSE;
ALTER TABLE concept_rules ADD COLUMN storage_path VARCHAR(500);

-- 2. Create new tables
CREATE TABLE database_connections (...);
CREATE TABLE labeling_functions (...);
CREATE TABLE snorkel_jobs (...);
CREATE TABLE partition_executions (...);

-- 3. Set up MLflow
docker run -d -p 5000:5000 \
  -v mlflow_data:/mlflow \
  --name mlflow \
  ghcr.io/mlflow/mlflow:latest \
  mlflow server --host 0.0.0.0 --port 5000
```

### Phase 2: Dagster Setup (Week 1-2)

```bash
# Install dependencies
pip install dagster dagster-webserver dagster-postgres dagster-aws dagster-mlflow

# Create Dagster code location
mkdir -p backend/dagster
touch backend/dagster/__init__.py
touch backend/dagster/definitions.py

# Start Dagster daemon
dagster dev -f backend/dagster/definitions.py
```

### Phase 3: Implement Core Assets (Week 2-3)

1. Create `backend/dagster/assets/indexes.py` - Index materialization
2. Create `backend/dagster/assets/rules.py` - Rule materialization
3. Create `backend/dagster/assets/snorkel.py` - Snorkel pipeline
4. Test materialization with sample data

### Phase 4: FastAPI Endpoints (Week 3-4)

1. Create `backend/routers/catalog.py` - Asset catalog
2. Create `backend/routers/indexes.py` - Index CRUD + materialize
3. Create `backend/routers/rules.py` - Rule CRUD + materialize
4. Create `backend/routers/labeling_functions.py` - LF creation
5. Create `backend/routers/snorkel.py` - Training trigger + results

### Phase 5: React Flow UI (Week 4-5)

1. Install dependencies: `npm install reactflow`
2. Create `components/SnorkelPipelineBuilder.tsx`
3. Create custom node components (IndexNode, RuleNode, etc.)
4. Implement asset catalog sidebar
5. Implement drag/drop from catalog to canvas
6. Connect to FastAPI endpoints

### Phase 6: Testing (Week 5-6)

1. Test POC (`dagster_poc_simple.py`)
2. Test full pipeline end-to-end
3. Test partitioned execution
4. Test softmax output
5. Performance testing with large datasets

### Phase 7: Gradual Migration (Week 6+)

1. Keep existing Celery system running
2. Add feature flag to switch between Celery/Dagster
3. Migrate one concept at a time
4. Monitor performance and errors
5. Full cutover after validation

---

## Summary

### Key Components

1. **React Flow UI** - Visual pipeline builder (not Dagster UI)
2. **Asset Catalog** - Indexes and Rules as materialized, reusable assets
3. **FastAPI** - Validation, persistence, Dagster triggering
4. **PostgreSQL** - Single source of truth
5. **Dagster** - Execution engine only
6. **S3** - Data storage (parquet files)
7. **MLflow** - Model registry
8. **Softmax** - Optional probabilistic output

### Benefits

✅ **Visual Interface** - Intuitive React Flow pipeline builder
✅ **Reusable Assets** - Materialize once, use many times
✅ **Partitioning** - Parallel execution for large datasets (6x speedup)
✅ **Softmax Output** - Confidence scores for predictions
✅ **Lineage Tracking** - Automatic in Dagster
✅ **No Launchpad** - Users never interact with Dagster UI directly
✅ **Extensible** - Easy to add new pipeline types
