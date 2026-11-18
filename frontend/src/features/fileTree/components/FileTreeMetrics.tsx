import DataFileReadableMetricsTable from "../../../components/DataFileReadableMetricsTable";
import PanelIssueNotice from "../../../components/PanelIssueNotice";
import ReadableMetricsSummary from "../../../components/ReadableMetricsSummary";
import type { ReadableMetrics } from "../../../utils/readableMetrics";
import { aggregateReadableMetrics } from "../model/aggregateReadableMetrics";
import { calculateFileStatistics } from "../model/partitionModel";
import { readableMetricsSchema } from "../schemas";
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

const FILE_TYPE_SECTION_CLASS = "rounded-lg border border-edge";
const FILE_TYPE_HEADING_CLASS =
  "rounded-t-lg border-b border-edge bg-surface-deep px-4 py-3 text-base font-semibold leading-snug text-accent";

const parseFileMetrics = (files: DataFileNode[]): ReadableMetrics[] | null => {
  const fileMetrics: ReadableMetrics[] = [];
  for (const file of files) {
    const parsedMetrics = readableMetricsSchema.safeParse(
      file.details.readable_metrics ?? {},
    );
    if (!parsedMetrics.success) return null;
    fileMetrics.push(parsedMetrics.data);
  }
  return fileMetrics;
};

const FileTreeMetrics = ({ files, isAggregated }: FileTreeMetricsProps) => (
  <>
    {METRIC_GROUPS.map(({ type, title }) => {
      const groupFiles = files.filter((file) => file.type === type);
      const fileMetrics = parseFileMetrics(groupFiles);
      if (fileMetrics === null) {
        return (
          <section key={type} className={FILE_TYPE_SECTION_CLASS}>
            <h3 className={FILE_TYPE_HEADING_CLASS}>{title}</h3>
            <div className="p-4">
              <PanelIssueNotice type="error">
                Readable metrics are invalid.
              </PanelIssueNotice>
            </div>
          </section>
        );
      }
      const readableMetrics = isAggregated
        ? aggregateReadableMetrics(fileMetrics)
        : (fileMetrics[0] ?? {});
      if (Object.keys(readableMetrics).length === 0) return null;
      const statistics = calculateFileStatistics(groupFiles);
      const totalFileSizeBytes = statistics.hasCompleteFileSizes
        ? statistics.totalSizeBytes
        : null;
      const sizeScope = isAggregated ? "files" : "file";

      return (
        <section key={type} className={FILE_TYPE_SECTION_CLASS}>
          <h3 className={FILE_TYPE_HEADING_CLASS}>{title}</h3>
          <div className="space-y-4 p-4">
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
                isAggregated
                  ? "Aggregated Readable Metrics"
                  : "Readable Metrics"
              }
              totalFileSizeBytes={totalFileSizeBytes}
            />
          </div>
        </section>
      );
    })}
    {files.some((file) => file.type === "deletion_vector") && (
      <section className={FILE_TYPE_SECTION_CLASS}>
        <h3 className={FILE_TYPE_HEADING_CLASS}>Deletion vectors</h3>
        <p className="p-4 text-xs text-slate-400">
          Deletion vectors mark deleted rows and have no column metrics. Folder
          totals use each vector’s size; Puffin file size covers the whole
          container.
        </p>
      </section>
    )}
  </>
);

export default FileTreeMetrics;
