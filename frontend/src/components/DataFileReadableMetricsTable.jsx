import { PanelSectionTitle } from "./PanelContent";
import { formatLocaleDateTime, parseUtcDate } from "../utils/dateUtils";

const formatLabel = (label) =>
  label === "column_size_mib"
    ? "column size (MiB)"
    : label.replaceAll("_", " ");

const formatValue = (value, metricName) => {
  if (value == null || value === "") return "-";
  if (metricName === "column_size_mib") {
    const numericValue = Number(value);
    if (Number.isFinite(numericValue)) {
      return numericValue.toLocaleString("en-US", {
        maximumFractionDigits: 20,
        useGrouping: false,
      });
    }
  }
  if (
    typeof value === "string" &&
    ["lower_bound", "upper_bound"].includes(metricName)
  ) {
    const date = parseUtcDate(value);
    if (date) return formatLocaleDateTime(date);
  }
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
};

const DataFileReadableMetricsTable = ({ readableMetrics }) => {
  const columns = Object.entries(readableMetrics);
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
      <PanelSectionTitle>Readable Metrics</PanelSectionTitle>
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
                    {formatValue(metrics?.[metricName], metricName)}
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

export default DataFileReadableMetricsTable;
