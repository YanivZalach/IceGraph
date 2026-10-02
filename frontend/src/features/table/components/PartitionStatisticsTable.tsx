import { createColumnHelper } from "@tanstack/react-table";
import type { z } from "zod";
import { UI_HELPER_TEXT_CLASS } from "../../../uiTypography";
import StatisticsTable from "../../../shared/components/StatisticsTable";
import {
  formatValue,
  parseSignedInteger,
  STATISTICS_TABLE_FEATURES,
} from "../../../shared/components/statisticsTableBase";
import { compareBigInts } from "../../../utils/readableMetricsStatistics";
import { formatLocaleDateTime } from "../../../utils/dateUtils";
import type { partitionStatisticsRowSchema } from "../api/graphSchemas";

type PartitionStatisticsRow = z.infer<typeof partitionStatisticsRowSchema>;

interface PartitionStatisticsTableProps {
  partitionsCount: number | string | null;
  sampledPartitions: PartitionStatisticsRow[];
}

const PARTITION_COLUMN = "partition";
const LAST_UPDATED_AT_COLUMN = "last_updated_at";

const columnHelper = createColumnHelper<
  typeof STATISTICS_TABLE_FEATURES,
  PartitionStatisticsRow
>();

const comparePartitionValues = (first: unknown, second: unknown): number => {
  const firstNumber = parseSignedInteger(first);
  const secondNumber = parseSignedInteger(second);
  if (firstNumber !== undefined && secondNumber !== undefined) {
    return compareBigInts(firstNumber, secondNumber);
  }
  return formatValue(first).localeCompare(formatValue(second));
};

const formatPartitionValue = (columnId: string, value: unknown): string => {
  if (columnId === LAST_UPDATED_AT_COLUMN && typeof value === "number") {
    return formatLocaleDateTime(new Date(value));
  }
  return formatValue(value);
};

const buildColumns = (sampledPartitions: PartitionStatisticsRow[]) =>
  [...new Set(sampledPartitions.flatMap((row) => Object.keys(row)))].map(
    (columnId) =>
      columnHelper.accessor((row) => row[columnId], {
        cell: ({ getValue }) => formatPartitionValue(columnId, getValue()),
        header: columnId,
        id: columnId,
        sortFn: (first, second) =>
          comparePartitionValues(
            first.original[columnId],
            second.original[columnId],
          ),
        sortUndefined: "last",
      }),
  );

const PartitionStatisticsTable = ({
  partitionsCount,
  sampledPartitions,
}: PartitionStatisticsTableProps) => {
  const isSampledByLastUpdate = sampledPartitions.some(
    (row) => LAST_UPDATED_AT_COLUMN in row,
  );

  return (
    <StatisticsTable
      columns={buildColumns(sampledPartitions)}
      description={
        <p className={`${UI_HELPER_TEXT_CLASS} mb-2`}>
          Showing {sampledPartitions.length} of{" "}
          {partitionsCount ?? sampledPartitions.length} partitions
          {isSampledByLastUpdate ? ", sampled by most recent update" : ""}
        </p>
      }
      getRowId={(_row, index) => String(index)}
      rowHeaderColumnId={PARTITION_COLUMN}
      rows={sampledPartitions}
      title="Partitions"
    />
  );
};

export default PartitionStatisticsTable;
