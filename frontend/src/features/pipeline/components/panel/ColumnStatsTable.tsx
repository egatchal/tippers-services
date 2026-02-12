import type { ColumnStat } from '../../types/entities';

interface Props {
  stats?: Record<string, ColumnStat>;
}

export default function ColumnStatsTable({ stats }: Props) {
  if (!stats || Object.keys(stats).length === 0) return null;

  return (
    <div>
      <h4 className="text-xs font-semibold text-gray-600 uppercase mb-2">Column Statistics</h4>
      <div className="overflow-x-auto">
        <table className="min-w-full text-xs">
          <thead>
            <tr className="border-b border-gray-200">
              <th className="text-left px-2 py-1.5 font-semibold text-gray-600">Column</th>
              <th className="text-left px-2 py-1.5 font-semibold text-gray-600">Type</th>
              <th className="text-left px-2 py-1.5 font-semibold text-gray-600">Nulls</th>
              <th className="text-left px-2 py-1.5 font-semibold text-gray-600">Stats</th>
            </tr>
          </thead>
          <tbody>
            {Object.entries(stats).map(([col, stat]) => (
              <tr key={col} className="border-b border-gray-50">
                <td className="px-2 py-1.5 font-medium text-gray-900">{col}</td>
                <td className="px-2 py-1.5 text-gray-500">{stat.dtype}</td>
                <td className="px-2 py-1.5 text-gray-500">{stat.null_count}</td>
                <td className="px-2 py-1.5">
                  {stat.min != null ? (
                    <span className="text-gray-600">
                      {stat.min.toFixed(2)}..{stat.max?.toFixed(2)} (avg {stat.mean?.toFixed(2)})
                    </span>
                  ) : stat.unique_count != null ? (
                    <span className="text-gray-600">
                      {stat.unique_count} unique
                      {stat.top_values && Object.keys(stat.top_values).length > 0 && (
                        <span className="text-gray-400 ml-1">
                          top: {Object.entries(stat.top_values).slice(0, 3).map(([k, v]) => `${k}(${v})`).join(', ')}
                        </span>
                      )}
                    </span>
                  ) : (
                    <span className="text-gray-400">-</span>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
