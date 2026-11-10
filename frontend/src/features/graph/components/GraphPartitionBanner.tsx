import type { ManifestPartitionView } from "../hooks/useGraphSelection";

interface GraphPartitionBannerProps {
  view: ManifestPartitionView;
  onShowFullManifest: () => void;
}

const GraphPartitionBanner = ({
  view,
  onShowFullManifest,
}: GraphPartitionBannerProps) => (
  <div
    role="status"
    className="flex w-full flex-col gap-1.5 rounded-lg border-2 border-dashed border-amber-400 bg-surface px-3 py-2 text-xs text-slate-200 shadow-md"
  >
    <span className="font-bold uppercase tracking-wide text-amber-400">
      Partial view
    </span>
    <span className="break-all">
      Manifest <span className="font-mono">{view.manifest.label}</span> shows
      only the files of partition{" "}
      <span className="font-mono text-amber-300">{view.partitionLabel}</span>
    </span>
    <button
      type="button"
      onClick={onShowFullManifest}
      className="w-fit cursor-pointer rounded border border-amber-400 px-2 py-1 font-bold uppercase tracking-wide text-amber-400 transition hover:bg-amber-400/20"
    >
      Show full manifest
    </button>
  </div>
);

export default GraphPartitionBanner;
