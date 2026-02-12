import { Navigate, type RouteObject } from 'react-router-dom';
import { lazy, Suspense } from 'react';
import LoadingSpinner from './shared/components/LoadingSpinner';

const PipelinePage = lazy(() => import('./features/pipeline/pages/PipelinePage'));
const ConnectionsPage = lazy(() => import('./features/connections/pages/ConnectionsPage'));

function Lazy({ children }: { children: React.ReactNode }) {
  return <Suspense fallback={<LoadingSpinner />}>{children}</Suspense>;
}

export const routes: RouteObject[] = [
  { index: true, element: <Navigate to="/pipeline" replace /> },
  { path: 'pipeline', element: <Lazy><PipelinePage /></Lazy> },
  { path: 'connections', element: <Lazy><ConnectionsPage /></Lazy> },
];
