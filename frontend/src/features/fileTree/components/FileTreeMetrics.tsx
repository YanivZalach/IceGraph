import DataFileReadableMetricsTable from "../../../components/DataFileReadableMetricsTable";
import ReadableMetricsSummary from "../../../components/ReadableMetricsSummary";
import { PanelSectionTitle } from "../../../components/PanelContent";
import { aggregateReadableMetrics } from "../model/aggregateReadableMetrics";
import { calculateFileStatistics } from "../model/partitionModel";
import type { DataFileNode } from "../types";

interface FileTreeMetricsProps {
  files: DataFileNode[];
  isAggregated: boolean;
}

const METRIC_GROUPS = [
  { type: "data", title: "Data-file column metrics" },
  { type: "position_delete", title: "Position-delete file column metrics" },
  { type: "equality_delete", title: "Equality-delete file column metrics" },
] as const;

const FileTreeMetrics = ({ files, isAggregated }: FileTreeMetricsProps) => (
  <>
    {METRIC_GROUPS.map(({ type, title }) => {
      const groupFiles = files.filter((file) => file.type === type);
      const readableMetrics = isAggregated
        ? aggregateReadableMetrics(groupFiles)
        : (groupFiles[0]?.details.readable_metrics ?? {});
      if (Object.keys(readableMetrics).length === 0) return null;
      const statistics = calculateFileStatistics(groupFiles);
      const totalFileSizeBytes = statistics.hasCompleteFileSizes
        ? statistics.totalSizeBytes
        : null;
      const sizeScope = isAggregated ? "files" : "file";

      return (
        <section key={type} className="space-y-4">
          <PanelSectionTitle>{title}</PanelSectionTitle>
          <p className="text-xs text-slate-400">
            {type === "data"
              ? "Metrics describe stored data before deletes are applied."
              : "Metrics describe delete-file records, not surviving data rows."}{" "}
            Sizes and percentages use only these{" "}
            {type === "data" ? "data" : "delete"} files.
          </p>
          <ReadableMetricsSummary
            readableMetrics={readableMetrics}
            sizeScope={sizeScope}
            totalFileSizeBytes={totalFileSizeBytes}
          />
          <DataFileReadableMetricsTable
            readableMetrics={readableMetrics}
            sizeScope={sizeScope}
            title={
              isAggregated ? "Aggregated Readable Metrics" : "Readable Metrics"
            }
            totalFileSizeBytes={totalFileSizeBytes}
          />
        </section>
      );
    })}
    {files.some((file) => file.type === "deletion_vector") && (
      <section>
        <PanelSectionTitle>Deletion vectors</PanelSectionTitle>
        <p className="mt-3 text-xs text-slate-400">
          Puffin deletion vectors store bitmaps of deleted positions, not
          columns. They contribute to delete counts and size totals, but have no
          per-column metrics. Folder size totals use the vector size, while
          Puffin file size refers to the whole containing file.
        </p>
      </section>
    )}
  </>
);

export default FileTreeMetrics;
