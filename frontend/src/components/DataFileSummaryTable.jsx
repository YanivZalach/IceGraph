import { PanelSectionTitle } from "./PanelContent";

const formatLabel = (label) => label.replaceAll("_", " ");

const formatValue = (value) => {
  if (value == null || value === "") return "-";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
};

const DataFileSummaryTable = ({ summary }) => {
  const columns = Object.entries(summary);
  if (columns.length === 0) return null;

  const metricNames = [
    ...new Set(
      columns.flatMap(([, metrics]) =>
        metrics && typeof metrics === "object" ? Object.keys(metrics) : [],
      ),
    ),
  ];

  return (
    <div>
      <PanelSectionTitle>Summary</PanelSectionTitle>
      <div className="overflow-x-auto rounded-lg border border-edge">
        <table className="w-full text-left font-mono text-xs text-slate-200">
          <thead className="bg-surface text-slate-400">
            <tr>
              <th className="px-3 py-2 font-semibold">column</th>
              {metricNames.map((metricName) => (
                <th key={metricName} className="px-3 py-2 font-semibold">
                  {formatLabel(metricName)}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-edge">
            {columns.map(([columnName, metrics]) => (
              <tr key={columnName} className="bg-canvas align-top">
                <th className="px-3 py-2 font-semibold text-accent">
                  {columnName}
                </th>
                {metricNames.map((metricName) => (
                  <td key={metricName} className="px-3 py-2 whitespace-nowrap">
                    {formatValue(metrics?.[metricName])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default DataFileSummaryTable;
