import { formatByteSize } from "../model";
import type { FileStatistics } from "../types";

interface FileTreeStatisticsProps {
  statistics: FileStatistics;
}

const FileTreeStatistics = ({ statistics }: FileTreeStatisticsProps) => {
  const rows = [
    ["Total size", formatByteSize(statistics.totalSizeBytes)],
    ["Average size", formatByteSize(statistics.averageSizeBytes)],
    ["Smallest file", formatByteSize(statistics.smallestSizeBytes)],
    ["Largest file", formatByteSize(statistics.largestSizeBytes)],
    ["Rows", statistics.totalRowCount.toLocaleString()],
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
            <span className="text-slate-400">Position deletes</span>
            <span className="font-mono text-ink">
              {statistics.positionDeleteFileCount}
            </span>
          </div>
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
