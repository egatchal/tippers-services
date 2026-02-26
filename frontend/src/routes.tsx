import { Navigate, type RouteObject } from 'react-router-dom';
import { lazy, Suspense } from 'react';
import LoadingSpinner from './shared/components/LoadingSpinner';

const PipelinePage = lazy(() => import('./features/pipeline/pages/PipelinePage'));
const ConnectionsPage = lazy(() => import('./features/connections/pages/ConnectionsPage'));
const DatasetsPage = lazy(() => import('./features/datasets/pages/DatasetsPage'));
const JobsPage = lazy(() => import('./features/jobs/pages/JobsPage'));
const ModelsPage = lazy(() => import('./features/models/pages/ModelsPage'));
const WorkflowsPage = lazy(() => import('./features/workflows/pages/WorkflowsPage'));
const OccupancyPage = lazy(() => import('./features/occupancy/pages/OccupancyPage'));

function Lazy({ children }: { children: React.ReactNode }) {
  return <Suspense fallback={<LoadingSpinner />}>{children}</Suspense>;
}

export const routes: RouteObject[] = [
  { index: true, element: <Navigate to="/pipeline" replace /> },
  { path: 'pipeline', element: <Lazy><PipelinePage /></Lazy> },
  { path: 'occupancy', element: <Lazy><OccupancyPage /></Lazy> },
  { path: 'datasets', element: <Lazy><DatasetsPage /></Lazy> },
  { path: 'jobs', element: <Lazy><JobsPage /></Lazy> },
  { path: 'models', element: <Lazy><ModelsPage /></Lazy> },
  { path: 'workflows', element: <Lazy><WorkflowsPage /></Lazy> },
  { path: 'connections', element: <Lazy><ConnectionsPage /></Lazy> },
];
