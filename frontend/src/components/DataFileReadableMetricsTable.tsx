import { PanelSectionTitle } from "./PanelContent";
import {
  formatReadableMetricValue,
  type ReadableMetrics,
} from "../utils/readableMetrics";
import { stripByteUnitFromFieldName } from "../shared/lib/formatBytes";

interface DataFileReadableMetricsTableProps {
  readableMetrics: ReadableMetrics;
}

const formatMetricLabel = (metricName: string): string =>
  stripByteUnitFromFieldName(metricName).replaceAll("_", " ");

const DataFileReadableMetricsTable = ({
  readableMetrics,
}: DataFileReadableMetricsTableProps) => {
  const columns = Object.entries(readableMetrics);
  if (columns.length === 0) return null;

  const metricNames = [
    ...new Set(columns.flatMap(([, metrics]) => Object.keys(metrics))),
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
                  {formatMetricLabel(metricName)}
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
                    {formatReadableMetricValue(
                      metrics[metricName],
                      metricName,
                      metrics.field_type,
                    )}
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
