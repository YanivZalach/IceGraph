import { formatBytesAsMebibytes } from "../shared/lib/formatBytes";
import type { ReadableMetrics } from "../utils/readableMetrics";
import {
  createMetricRatio,
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
  const metadataSizePercentage = createMetricRatio(
    metadataSizeBytes ?? undefined,
    totalFileSizeBigInt ?? undefined,
    true,
  );
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
      ...(metadataSizePercentage === undefined
        ? {}
        : {
            detail: `${formatMetricPercentage(metadataSizePercentage)} of total size`,
          }),
      label: `Metadata size in ${sizeScope}`,
      value: formatBytesAsMebibytes(metadataSizeBytes.toString()),
    });
  }

  cards.push({
    label: "Number of columns",
    value: summary.columnCount.toLocaleString(),
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
