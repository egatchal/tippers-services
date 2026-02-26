const services = ['snorkel', 'occupancy', 'hvac', 'general'] as const;

interface Props {
  active: string | null;
  onChange: (service: string | null) => void;
}

export default function ServiceTabs({ active, onChange }: Props) {
  return (
    <div className="flex gap-1 border-b border-gray-200 mb-4">
      <TabButton label="All" isActive={active === null} onClick={() => onChange(null)} />
      {services.map((s) => (
        <TabButton key={s} label={s.charAt(0).toUpperCase() + s.slice(1)} isActive={active === s} onClick={() => onChange(s)} />
      ))}
    </div>
  );
}

function TabButton({ label, isActive, onClick }: { label: string; isActive: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className={`px-4 py-2 text-sm font-medium transition-colors ${
        isActive
          ? 'text-blue-600 border-b-2 border-blue-600'
          : 'text-gray-500 hover:text-gray-700'
      }`}
    >
      {label}
    </button>
  );
}
