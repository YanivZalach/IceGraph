import type { MouseEvent } from "react";
import DataFileReadableMetricsTable from "../../../components/DataFileReadableMetricsTable";
import {
  PanelDetailRow,
  PanelSectionTitle,
} from "../../../components/PanelContent";
import ReadableMetricsSummary from "../../../components/ReadableMetricsSummary";
import { fileTypeLabel } from "../../../graphConstants.js";
import { formatBytesAsMebibytes } from "../../../shared/lib/formatBytes";
import { isEmptyValue } from "../../../shared/lib/isEmptyValue";
import { getFileSizeBytes } from "../model";
import type { InspectedFileTreeItem } from "../types";
import FileTreeStatistics from "./FileTreeStatistics";

interface FileTreeInspectorContentProps {
  duplicatingNodeId: string | null;
  inspectedItem: InspectedFileTreeItem;
  onViewInGraph: (event: MouseEvent<HTMLButtonElement>, fileId: string) => void;
}

const FILE_SUMMARY_KEYS = new Set([
  "file_path",
  "file_size_in_bytes",
  "format",
  "readable_metrics",
  "row_count",
  "type",
]);

const humanizeKey = (key: string): string =>
  key
    .replaceAll("_", " ")
    .replaceAll("-", " ")
    .replace(/^./, (firstCharacter) => firstCharacter.toUpperCase());

const FileTreeInspectorContent = ({
  duplicatingNodeId,
  inspectedItem,
  onViewInGraph,
}: FileTreeInspectorContentProps) => {
  const file = inspectedItem.kind === "file" ? inspectedItem.file : null;
  const statistics =
    inspectedItem.kind === "partition-path"
      ? inspectedItem.partitionPathNode.statistics
      : inspectedItem.kind === "partition"
        ? inspectedItem.partition.statistics
        : null;
  const readableMetrics =
    file?.details.readable_metrics ?? statistics?.readableMetrics ?? {};
  const hasReadableMetrics = Object.keys(readableMetrics).length > 0;
  const totalFileSizeBytes =
    file !== null
      ? getFileSizeBytes(file)
      : statistics?.hasCompleteFileSizes === true
        ? statistics.totalSizeBytes
        : null;
  const fileSummaryRows =
    file === null
      ? []
      : [
          file.details.file_size_in_bytes === undefined
            ? null
            : [
                "File size",
                formatBytesAsMebibytes(file.details.file_size_in_bytes),
              ],
          file.details.row_count === undefined
            ? null
            : ["Rows", file.details.row_count.toLocaleString()],
          file.details.format === undefined
            ? null
            : ["Format", file.details.format],
          ["File type", fileTypeLabel(file.type)],
        ].filter((row): row is [string, string] => row !== null);
  const fileDetailRows =
    file === null
      ? []
      : Object.entries(file.details).filter(
          ([key, value]) => !FILE_SUMMARY_KEYS.has(key) && !isEmptyValue(value),
        );

  return (
    <>
      {file !== null ? (
        <section>
          <PanelSectionTitle className="mb-3">File summary</PanelSectionTitle>
          <div className="flex flex-col gap-3">
            {fileSummaryRows.map(([label, value]) => (
              <PanelDetailRow key={label} label={label} value={value} />
            ))}
          </div>
        </section>
      ) : statistics !== null ? (
        <FileTreeStatistics statistics={statistics} />
      ) : null}
      {hasReadableMetrics && (
        <>
          <ReadableMetricsSummary
            readableMetrics={readableMetrics}
            sizeScope={file === null ? "files" : "file"}
            totalFileSizeBytes={totalFileSizeBytes}
          />
          <DataFileReadableMetricsTable
            readableMetrics={readableMetrics}
            title={
              file === null ? "Aggregated Readable Metrics" : "Readable Metrics"
            }
          />
        </>
      )}
      {file !== null && (
        <>
          <div className="flex justify-center border-y border-edge py-4">
            <button
              type="button"
              onClick={(event) => {
                onViewInGraph(event, file.id);
              }}
              disabled={duplicatingNodeId !== null}
              className="w-full cursor-pointer rounded-lg border border-accent px-3 py-2 text-xs font-bold uppercase tracking-wide text-accent hover:bg-accent-muted disabled:cursor-not-allowed disabled:opacity-50"
            >
              {duplicatingNodeId === file.id ? "Opening..." : "View in graph"}
            </button>
          </div>
          <section>
            <PanelSectionTitle className="mb-3">
              File information
            </PanelSectionTitle>
            <div className="flex flex-col gap-3">
              <PanelDetailRow label="File path" value={file.id} />
              {fileDetailRows.map(([key, value]) => (
                <PanelDetailRow
                  key={key}
                  label={humanizeKey(key)}
                  value={value}
                />
              ))}
            </div>
          </section>
        </>
      )}
    </>
  );
};

export default FileTreeInspectorContent;
