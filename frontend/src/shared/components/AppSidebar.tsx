import { useState } from 'react';
import { NavLink } from 'react-router-dom';
import { Workflow, Database, PanelLeftClose, PanelLeftOpen } from 'lucide-react';

const navItems = [
  { label: 'Snorkel Pipeline Builder', path: '/pipeline', icon: Workflow },
  { label: 'Connections', path: '/connections', icon: Database },
];

export default function AppSidebar() {
  const [collapsed, setCollapsed] = useState(false);

  return (
    <aside
      className={`flex flex-col h-screen bg-gray-900 text-gray-300 transition-all duration-200 ${
        collapsed ? 'w-16' : 'w-60'
      }`}
    >
      <div className="flex items-center gap-2 px-4 h-14 border-b border-gray-800">
        <Workflow className="w-6 h-6 text-blue-400 shrink-0" />
        {!collapsed && <span className="font-semibold text-white text-sm truncate">Tippers Services</span>}
      </div>

      <nav className="flex-1 py-2">
        {navItems.map(({ label, path, icon: Icon }) => (
          <NavLink
            key={path}
            to={path}
            className={({ isActive }) =>
              `flex items-center gap-3 px-4 py-2.5 text-sm transition-colors ${
                isActive ? 'bg-gray-800 text-white border-r-2 border-blue-400' : 'hover:bg-gray-800/50'
              }`
            }
          >
            <Icon className="w-5 h-5 shrink-0" />
            {!collapsed && <span className="truncate">{label}</span>}
          </NavLink>
        ))}
      </nav>

      <div className="border-t border-gray-800 py-2">
        <button
          onClick={() => setCollapsed(!collapsed)}
          className="flex items-center gap-3 px-4 py-2.5 text-sm w-full hover:bg-gray-800/50 transition-colors"
        >
          {collapsed ? <PanelLeftOpen className="w-5 h-5" /> : <PanelLeftClose className="w-5 h-5" />}
          {!collapsed && <span>Collapse</span>}
        </button>
      </div>
    </aside>
  );
}
