import { Outlet } from 'react-router-dom';
import AppSidebar from './shared/components/AppSidebar';
import AppHeader from './shared/components/AppHeader';

export default function App() {
  return (
    <div className="flex h-screen overflow-hidden">
      <AppSidebar />
      <div className="flex flex-col flex-1 overflow-hidden">
        <AppHeader />
        <main className="flex-1 overflow-hidden">
          <Outlet />
        </main>
      </div>
    </div>
  );
}
