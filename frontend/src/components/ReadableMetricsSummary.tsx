import { formatBytesAsMebibytes } from "../shared/lib/formatBytes";
import type { ReadableMetrics } from "../utils/readableMetrics";
import {
  formatMetricPercentage,
  summarizeReadableMetrics,
} from "../utils/readableMetricsStatistics";
import { PanelSectionTitle } from "./PanelContent";

interface ReadableMetricsSummaryProps {
  readableMetrics: ReadableMetrics;
  sizeScope: "file" | "files";
  totalFileSizeBytes: number | null;
}

interface StatisticCardProps {
  detail?: string;
  label: string;
  value: string;
}

const StatisticCard = ({ detail, label, value }: StatisticCardProps) => (
  <div className="min-w-0 rounded-lg border border-edge bg-canvas p-3">
    <div className="text-xs font-bold uppercase tracking-wide text-slate-500">
      {label}
    </div>
    <div className="mt-1 break-words font-mono text-sm font-semibold text-ink">
      {value}
    </div>
    {detail !== undefined && (
      <div className="mt-1 font-mono text-xs text-slate-400">{detail}</div>
    )}
  </div>
);

const formatColumnNames = (columnNames: string[]): string =>
  columnNames.join(", ");

const ReadableMetricsSummary = ({
  readableMetrics,
  sizeScope,
  totalFileSizeBytes,
}: ReadableMetricsSummaryProps) => {
  const summary = summarizeReadableMetrics(readableMetrics);
  const totalFileSizeBigInt =
    totalFileSizeBytes !== null &&
    Number.isSafeInteger(totalFileSizeBytes) &&
    totalFileSizeBytes >= 0
      ? BigInt(totalFileSizeBytes)
      : null;
  const metadataSizeBytes =
    totalFileSizeBigInt !== null &&
    summary.totalMeasuredColumnSizeBytes <= totalFileSizeBigInt
      ? totalFileSizeBigInt - summary.totalMeasuredColumnSizeBytes
      : null;
  const metadataSizePercentage =
    metadataSizeBytes !== null &&
    totalFileSizeBytes !== null &&
    totalFileSizeBytes > 0
      ? (Number(metadataSizeBytes) / totalFileSizeBytes) * 100
      : null;
  const cards: StatisticCardProps[] = [
    {
      label: `Data size in ${sizeScope}`,
      value: formatBytesAsMebibytes(
        summary.totalMeasuredColumnSizeBytes.toString(),
      ),
    },
  ];

  if (metadataSizeBytes !== null) {
    cards.push({
      ...(metadataSizePercentage === null
        ? {}
        : {
            detail: `${formatMetricPercentage(metadataSizePercentage)} of total file size`,
          }),
      label: `Metadata size in ${sizeScope}`,
      value: formatBytesAsMebibytes(metadataSizeBytes.toString()),
    });
  }

  if (summary.largestColumns !== null) {
    cards.push({
      detail: formatBytesAsMebibytes(summary.largestColumns.value.toString()),
      label: "Largest column",
      value: formatColumnNames(summary.largestColumns.columnNames),
    });
  }
  if (summary.smallestColumns !== null) {
    cards.push({
      detail: formatBytesAsMebibytes(summary.smallestColumns.value.toString()),
      label: "Smallest column",
      value: formatColumnNames(summary.smallestColumns.columnNames),
    });
  }
  if (summary.highestNullPercentage !== null) {
    cards.push({
      detail: formatMetricPercentage(summary.highestNullPercentage.value),
      label: "Highest null percentage",
      value: formatColumnNames(summary.highestNullPercentage.columnNames),
    });
  }
  if (summary.lowestNullPercentage !== null) {
    cards.push({
      detail: formatMetricPercentage(summary.lowestNullPercentage.value),
      label: "Lowest null percentage",
      value: formatColumnNames(summary.lowestNullPercentage.columnNames),
    });
  }
  if (summary.highestNanPercentage !== null) {
    cards.push({
      detail: formatMetricPercentage(summary.highestNanPercentage.value),
      label: "Highest NaN percentage",
      value: formatColumnNames(summary.highestNanPercentage.columnNames),
    });
  }
  cards.push({
    label: "Number of columns",
    value: summary.measuredColumnCount.toLocaleString(),
  });

  return (
    <section>
      <PanelSectionTitle className="mb-3">Column statistics</PanelSectionTitle>
      <div className="grid grid-cols-2 gap-2">
        {cards.map((card) => (
          <StatisticCard key={card.label} {...card} />
        ))}
      </div>
    </section>
  );
};

export default ReadableMetricsSummary;
