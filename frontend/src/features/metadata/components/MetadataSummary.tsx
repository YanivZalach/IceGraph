import type { TableMetadata } from "../../table/api/metadataSchemas";
import {
  formatCount,
  formatMetadataTime,
  integerText,
} from "../metadataPresentation";
import { formatBytesAsGibibytes } from "../../../shared/lib/formatBytes";
import HelpTerm from "../../../shared/components/HelpTerm";
import CopyIconButton from "../../../components/CopyIconButton";

interface MetadataSummaryProps {
  metadata: TableMetadata;
}

const MetadataSummary = ({ metadata }: MetadataSummaryProps) => {
  const snapshotId = integerText(metadata["current-snapshot-id"]);
  const snapshot = metadata["current-snapshot"];
  const stat = (key: string): string | null => snapshot?.summary?.[key] ?? null;
  const deleteFiles = stat("total-delete-files");
  const bytes = stat("total-files-size-bytes");
  const hasSnapshot = snapshotId !== null && snapshotId !== "-1";
  return (
    <section aria-labelledby="metadata-summary-title">
      <div className="mb-3 flex flex-wrap items-baseline gap-3">
        <h2
          id="metadata-summary-title"
          className="text-base font-semibold text-ink"
        >
          At a glance
        </h2>
        <span className="text-xs text-slate-400">
          {snapshot
            ? `Snapshot committed ${formatMetadataTime(snapshot["timestamp-ms"])}`
            : "Snapshot statistics"}
        </span>
      </div>
      <div className="grid grid-cols-2 divide-x divide-y divide-edge rounded-xl border border-edge bg-surface lg:grid-cols-4 lg:divide-y-0">
        <div className="p-5">
          <span className="text-xs text-slate-400">Records in data files</span>
          <p className="mt-2 text-2xl font-semibold text-ink">
            {formatCount(stat("total-records"))}
          </p>
        </div>
        <div className="p-5">
          <span className="text-xs text-slate-400">Data files</span>
          <p className="mt-2 text-2xl font-semibold text-ink">
            {formatCount(stat("total-data-files"))}
          </p>
        </div>
        <div className="p-5">
          <span className="text-xs text-slate-400">
            <HelpTerm label="Delete files">
              Files that record deleted rows separately from the data files.
              &quot;Records in data files&quot; does not subtract them, so the
              table can hold fewer live rows than that number suggests.
            </HelpTerm>
          </span>
          <p className="mt-2 text-2xl font-semibold text-ink">
            {formatCount(deleteFiles)}
          </p>
        </div>
        <div className="p-5">
          <span className="text-xs text-slate-400">
            Total file size (data + deletes)
          </span>
          <p
            className="mt-2 text-2xl font-semibold text-ink"
            title={bytes === null ? undefined : `${formatCount(bytes)} bytes`}
          >
            {bytes === null ? "Unavailable" : formatBytesAsGibibytes(bytes)}
          </p>
          {bytes !== null && (
            <details className="mt-2 text-xs text-slate-400">
              <summary className="cursor-pointer">Exact bytes</summary>
              <div className="mt-2 flex items-center gap-2">
                <p className="min-w-0 break-all font-mono">{bytes}</p>
                <CopyIconButton text={bytes} title="Copy exact bytes" />
              </div>
            </details>
          )}
        </div>
      </div>
      {!hasSnapshot && (
        <p className="mt-2 text-sm text-slate-400">
          No current snapshot is recorded in this metadata. Snapshot statistics
          are unavailable.
        </p>
      )}
      {hasSnapshot && !snapshot && (
        <p className="mt-2 text-sm text-slate-400">
          Current snapshot {snapshotId} is not recorded in this metadata file.
          Its statistics are unavailable.
        </p>
      )}
    </section>
  );
};
export default MetadataSummary;
