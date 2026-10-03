import { createColumnHelper } from "@tanstack/react-table";
import type { z } from "zod";
import { UI_HELPER_TEXT_CLASS } from "../../../uiTypography";
import {
  formatBytesAsMebibytes,
  isByteFieldName,
  stripByteUnitFromFieldName,
} from "../../../shared/lib/formatBytes";
import StatisticsTable from "../../../shared/components/StatisticsTable";
import {
  formatValue,
  STATISTICS_TABLE_FEATURES,
} from "../../../shared/components/statisticsTableBase";
import type { partitionDistributionSchema } from "../api/metadataSchemas";

type PartitionDistribution = z.infer<typeof partitionDistributionSchema>;
type MinAvgMax = NonNullable<PartitionDistribution["data_record_count"]>;
type StatisticValue = MinAvgMax["min"];

interface DistributionRow extends MinAvgMax {
  statistic: string;
}

interface PartitionDistributionTableProps {
  partitionDistribution: PartitionDistribution;
}

const STATISTIC_COLUMN = "statistics";
const AVERAGE_FRACTION_DIGITS = 2;
const MIN_AVG_MAX_KEYS = ["min", "avg", "max"] as const;

const columnHelper = createColumnHelper<
  typeof STATISTICS_TABLE_FEATURES,
  DistributionRow
>();

const formatStatisticValue = (
  statistic: string,
  value: StatisticValue,
): string => {
  if (value === null) return "";
  if (isByteFieldName(statistic)) return formatBytesAsMebibytes(value);
  return typeof value === "number" && !Number.isInteger(value)
    ? value.toFixed(AVERAGE_FRACTION_DIGITS)
    : formatValue(value);
};

const columns = columnHelper.columns([
  columnHelper.accessor((row) => row.statistic, {
    cell: ({ getValue }) => {
      const statistic = getValue();
      return isByteFieldName(statistic)
        ? stripByteUnitFromFieldName(statistic)
        : statistic;
    },
    header: STATISTIC_COLUMN,
    id: STATISTIC_COLUMN,
  }),
  ...MIN_AVG_MAX_KEYS.map((key) =>
    columnHelper.accessor((row) => row[key], {
      cell: ({ getValue, row }) =>
        formatStatisticValue(row.original.statistic, getValue()),
      enableSorting: false,
      header: key,
      id: key,
    }),
  ),
]);

const buildRows = (
  partitionDistribution: PartitionDistribution,
): DistributionRow[] =>
  Object.entries(partitionDistribution).flatMap(([statistic, value]) =>
    value !== undefined && value.min !== null ? [{ statistic, ...value }] : [],
  );

const PartitionDistributionTable = ({
  partitionDistribution,
}: PartitionDistributionTableProps) => {
  return (
    <StatisticsTable
      columns={columns}
      description={
        <p className={`${UI_HELPER_TEXT_CLASS} mb-2`}>
          Per partition across all partitions
        </p>
      }
      getRowId={(row) => row.statistic}
      rowHeaderColumnId={STATISTIC_COLUMN}
      rows={buildRows(partitionDistribution)}
      title="Partition distribution"
    />
  );
};

export default PartitionDistributionTable;
