import type { IcebergInteger } from "../../table/api/metadataSchemas";
import { formatMetadataTime } from "../metadataPresentation";
import CopyIconButton from "../../../components/CopyIconButton";

interface MetadataVersionProps {
  path: string | undefined;
  updatedAt: IcebergInteger | null | undefined;
}

const MetadataVersion = ({ path, updatedAt }: MetadataVersionProps) => (
  <section
    aria-labelledby="metadata-version-title"
    className="rounded-xl border border-accent/40 border-l-4 border-l-accent bg-surface p-5"
  >
    <h2
      id="metadata-version-title"
      className="text-base font-semibold text-ink"
    >
      Latest metadata in the selected range
    </h2>
    <p className="mt-2 text-xl font-semibold text-ink">
      {formatMetadataTime(updatedAt)}
    </p>
    <div className="mt-3 flex flex-wrap items-center gap-3 rounded-lg border border-edge bg-canvas p-3">
      <code className="min-w-0 flex-1 break-all text-xs leading-relaxed text-slate-300">
        {path ?? "Metadata file path unavailable"}
      </code>
      {path !== undefined && (
        <CopyIconButton text={path} title="Copy metadata path" />
      )}
    </div>
  </section>
);
export default MetadataVersion;
