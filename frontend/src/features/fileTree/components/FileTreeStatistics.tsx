import HelpTerm from "../../../shared/components/HelpTerm";
import { formatBytesAsMebibytes } from "../../../shared/lib/formatBytes";
import type { FileStatistics } from "../types";

interface FileTreeStatisticsProps {
  statistics: FileStatistics;
}

const FileTreeStatistics = ({ statistics }: FileTreeStatisticsProps) => {
  const formatByteCount = (byteCount: number): string =>
    formatBytesAsMebibytes(String(byteCount));
  const hasDeletionVectors = statistics.deletionVectorCount > 0;
  const rows = [
    ["Total size", formatByteCount(statistics.totalSizeBytes)],
    ["Average file size", formatByteCount(statistics.averageSizeBytes)],
    ["Smallest file", formatByteCount(statistics.smallestSizeBytes)],
    ["Largest file", formatByteCount(statistics.largestSizeBytes)],
    [
      "Data rows (before deletes)",
      statistics.dataRowCount?.toLocaleString() ?? "Unavailable",
    ],
    [
      "Position-delete file records",
      statistics.positionDeleteRecordCount?.toLocaleString() ?? "Unavailable",
    ],
    ...(hasDeletionVectors
      ? [
          [
            "Deleted positions (vectors)",
            statistics.deletionVectorPositionCount?.toLocaleString() ??
              "Unavailable",
          ],
        ]
      : []),
    [
      "Equality delete records",
      statistics.equalityDeleteRecordCount?.toLocaleString() ?? "Unavailable",
    ],
    ["Files", statistics.fileCount.toLocaleString()],
  ];

  return (
    <div className="flex flex-col gap-4">
      <div className="grid grid-cols-2 gap-2">
        {rows.map(([label, value]) => (
          <div
            key={label}
            className="rounded-lg border border-edge bg-canvas p-3"
          >
            <div className="text-xs font-bold uppercase tracking-wide text-slate-500">
              {label}
            </div>
            <div className="mt-1 font-mono text-sm font-semibold text-ink">
              {value}
            </div>
          </div>
        ))}
      </div>
      <div>
        <div className="mb-2 text-xs font-bold uppercase tracking-wide text-slate-500">
          Files by type
        </div>
        <div className="flex flex-col gap-2 text-sm">
          <div className="flex justify-between">
            <span className="text-slate-400">Data</span>
            <span className="font-mono text-ink">
              {statistics.dataFileCount}
            </span>
          </div>
          <div className="flex justify-between">
            <span className="text-slate-400">
              <HelpTerm label="Position deletes">
                Number of traditional position-delete files. Deletion vectors
                are counted separately.
              </HelpTerm>
            </span>
            <span className="font-mono text-ink">
              {statistics.positionDeleteFileCount}
            </span>
          </div>
          {hasDeletionVectors && (
            <div className="flex justify-between">
              <span className="text-slate-400">
                <HelpTerm label="Deletion vectors">
                  Position-delete file records exclude deletion vectors. Deleted
                  positions in vectors are counted separately. Deletes are not
                  applied to the data-row count. Equality delete records are
                  matching rules, not a count of deleted rows. Sizes and file
                  counts include data files and delete entries.
                </HelpTerm>
              </span>
              <span className="font-mono text-ink">
                {statistics.deletionVectorCount}
              </span>
            </div>
          )}
          <div className="flex justify-between">
            <span className="text-slate-400">Equality deletes</span>
            <span className="font-mono text-ink">
              {statistics.equalityDeleteFileCount}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default FileTreeStatistics;
