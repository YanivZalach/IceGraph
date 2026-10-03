import type { ReadableMetrics } from "../utils/readableMetrics";
import { buildReadableMetricRows } from "../utils/readableMetricsStatistics";
import StatisticsTable from "../shared/components/StatisticsTable";
import { getReadableMetricsColumns } from "./readableMetricsTableColumns";

interface DataFileReadableMetricsTableProps {
  readableMetrics: ReadableMetrics;
  sizeScope: "file" | "files";
  title?: string;
  totalFileSizeBytes: number | null;
}

const DataFileReadableMetricsTable = ({
  readableMetrics,
  sizeScope,
  title = "Readable Metrics",
  totalFileSizeBytes,
}: DataFileReadableMetricsTableProps) => {
  const rows = buildReadableMetricRows(readableMetrics, totalFileSizeBytes);
  return (
    <StatisticsTable
      columns={getReadableMetricsColumns(sizeScope)}
      getRowId={(row) => row.columnName}
      rowHeaderColumnId="columnName"
      rows={rows}
      sortDescFirst={false}
      title={title}
    />
  );
};

export default DataFileReadableMetricsTable;
