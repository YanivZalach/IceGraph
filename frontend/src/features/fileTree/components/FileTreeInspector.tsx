import { useState } from "react";
import type { MouseEvent } from "react";
import { calculateFileStatistics } from "../fileTreeModel";
import type { InspectedFileTreeItem } from "../fileTreeTypes";
import FileTreeDetailRow from "./FileTreeDetailRow";
import FileTreeStatistics from "./FileTreeStatistics";

interface FileTreeInspectorProps {
  duplicatingNodeId: string | null;
  inspectedItem: InspectedFileTreeItem;
  onClose: () => void;
  onViewInGraph: (event: MouseEvent<HTMLButtonElement>, fileId: string) => void;
}

const humanizeKey = (key: string): string =>
  key
    .replaceAll("_", " ")
    .replaceAll("-", " ")
    .replace(/^./, (firstCharacter) => firstCharacter.toUpperCase());

const FileTreeInspector = ({
  duplicatingNodeId,
  inspectedItem,
  onClose,
  onViewInGraph,
}: FileTreeInspectorProps) => {
  const [isPathCopied, setIsPathCopied] = useState(false);
  const title =
    inspectedItem.kind === "file"
      ? "Data file"
      : inspectedItem.kind === "folder"
        ? "Folder statistics"
        : "Partition statistics";
  const subtitle =
    inspectedItem.kind === "file"
      ? inspectedItem.file.id
      : inspectedItem.kind === "folder"
        ? inspectedItem.folder.path
        : inspectedItem.partition.name;
  const statistics =
    inspectedItem.kind === "file"
      ? calculateFileStatistics([inspectedItem.file])
      : inspectedItem.kind === "folder"
        ? inspectedItem.folder.statistics
        : inspectedItem.partition.statistics;

  const handleCopyPath = async () => {
    await navigator.clipboard.writeText(subtitle);
    setIsPathCopied(true);
    window.setTimeout(() => {
      setIsPathCopied(false);
    }, 2000);
  };

  return (
    <aside
      aria-label="File tree inspector"
      className="flex w-panel-default shrink-0 flex-col border-l border-edge bg-surface"
    >
      <div className="flex items-start justify-between gap-3 border-b border-edge px-5 py-4">
        <div className="min-w-0">
          <h2 className="text-sm font-bold uppercase tracking-wide text-ink">
            {title}
          </h2>
          <p className="mt-1 break-words font-mono text-xs text-slate-400">
            {subtitle}
          </p>
        </div>
        <button
          type="button"
          aria-label="Close inspector"
          onClick={onClose}
          className="size-7 shrink-0 cursor-pointer rounded-full bg-edge text-slate-400 hover:bg-edge-hover hover:text-ink"
        >
          ×
        </button>
      </div>
      <div className="flex min-h-0 flex-1 flex-col gap-5 overflow-y-auto px-5 py-4">
        <FileTreeStatistics statistics={statistics} />
        {inspectedItem.kind === "file" && (
          <>
            <div className="flex gap-2 border-y border-edge py-4">
              <button
                type="button"
                onClick={() => void handleCopyPath()}
                className="flex-1 cursor-pointer rounded-lg border border-edge px-3 py-2 text-sm text-slate-300 hover:border-edge-hover hover:text-white"
              >
                {isPathCopied ? "Copied" : "Copy path"}
              </button>
              <button
                type="button"
                onClick={(event) => {
                  onViewInGraph(event, inspectedItem.file.id);
                }}
                disabled={duplicatingNodeId !== null}
                className="flex-1 cursor-pointer rounded-lg border border-accent px-3 py-2 text-sm text-accent hover:bg-accent-muted disabled:cursor-not-allowed disabled:opacity-50"
              >
                {duplicatingNodeId === inspectedItem.file.id
                  ? "Opening..."
                  : "View in graph"}
              </button>
            </div>
            <section className="flex flex-col gap-3">
              <h3 className="text-xs font-bold uppercase tracking-wide text-slate-400">
                File information
              </h3>
              {Object.entries(inspectedItem.file.details).map(
                ([key, value]) => (
                  <FileTreeDetailRow
                    key={key}
                    label={humanizeKey(key)}
                    value={value}
                  />
                ),
              )}
            </section>
          </>
        )}
      </div>
    </aside>
  );
};

export default FileTreeInspector;
