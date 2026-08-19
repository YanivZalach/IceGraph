import { useState } from "react";
import type { CSSProperties, MouseEvent } from "react";
import {
  PanelDetailRow,
  PanelHeader,
  PanelSectionTitle,
} from "../../../components/PanelContent";
import { isEmptyValue } from "../../../shared/lib/isEmptyValue";
import SidePanelFrame, {
  SidePanelResizeHandle,
} from "../../../components/SidePanelFrame";
import { formatByteSize, getFileSizeBytes } from "../model";
import type { InspectedFileTreeItem } from "../types";
import FileTreeStatistics from "./FileTreeStatistics";

interface FileTreeInspectorProps {
  duplicatingNodeId: string | null;
  inspectedItem: InspectedFileTreeItem;
  onClose: () => void;
  onViewInGraph: (event: MouseEvent<HTMLButtonElement>, fileId: string) => void;
}

interface FileTreeInspectorStyle extends CSSProperties {
  "--file-tree-inspector-width": string;
  "--panel-accent": string;
}

const PANEL_ACCENT = "#2e86c1";
const PANEL_MIN_WIDTH_PX = 320;

const humanizeKey = (key: string): string =>
  key
    .replaceAll("_", " ")
    .replaceAll("-", " ")
    .replace(/^./, (firstCharacter) => firstCharacter.toUpperCase());

const FILE_SUMMARY_KEYS = new Set([
  "file_path",
  "format",
  "row_count",
  "size_gb",
  "type",
]);

const FileTreeInspector = ({
  duplicatingNodeId,
  inspectedItem,
  onClose,
  onViewInGraph,
}: FileTreeInspectorProps) => {
  const [panelWidthPx, setPanelWidthPx] = useState<number | null>(null);
  const title =
    inspectedItem.kind === "file"
      ? "Data file"
      : inspectedItem.kind === "partition-path"
        ? "Partition path statistics"
        : "Partition statistics";
  const subtitle =
    inspectedItem.kind === "file"
      ? inspectedItem.file.id
      : inspectedItem.kind === "partition-path"
        ? inspectedItem.partitionPathNode.path
        : inspectedItem.partition.name;
  const statistics =
    inspectedItem.kind === "partition-path"
      ? inspectedItem.partitionPathNode.statistics
      : inspectedItem.kind === "partition"
        ? inspectedItem.partition.statistics
        : null;
  const fileSummaryRows =
    inspectedItem.kind === "file"
      ? [
          inspectedItem.file.details.size_gb === undefined
            ? null
            : [
                "File size",
                formatByteSize(getFileSizeBytes(inspectedItem.file)),
              ],
          inspectedItem.file.details.row_count === undefined
            ? null
            : ["Rows", inspectedItem.file.details.row_count.toLocaleString()],
          inspectedItem.file.details.format === undefined
            ? null
            : ["Format", inspectedItem.file.details.format],
          ["File type", humanizeKey(inspectedItem.file.type)],
        ].filter((row): row is [string, string] => row !== null)
      : [];
  const fileDetailRows =
    inspectedItem.kind === "file"
      ? Object.entries(inspectedItem.file.details).filter(
          ([key, value]) => !FILE_SUMMARY_KEYS.has(key) && !isEmptyValue(value),
        )
      : [];
  const panelStyle: FileTreeInspectorStyle = {
    "--file-tree-inspector-width":
      panelWidthPx === null ? "38%" : `${String(panelWidthPx)}px`,
    "--panel-accent": PANEL_ACCENT,
  };

  const startResize = (event: MouseEvent<HTMLDivElement>) => {
    event.preventDefault();
    const panel = event.currentTarget.parentElement;
    if (panel === null) return;
    const container = panel.parentElement;
    if (container === null) return;

    const startX = event.clientX;
    const startWidth = panel.getBoundingClientRect().width;
    const maximumWidth = Math.max(
      PANEL_MIN_WIDTH_PX,
      container.getBoundingClientRect().width * 0.7,
    );
    const handleMove = (moveEvent: globalThis.MouseEvent) => {
      const nextWidth = Math.min(
        maximumWidth,
        Math.max(PANEL_MIN_WIDTH_PX, startWidth + startX - moveEvent.clientX),
      );
      setPanelWidthPx(nextWidth);
    };
    const handleUp = () => {
      document.removeEventListener("mousemove", handleMove);
      document.removeEventListener("mouseup", handleUp);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
    };

    document.body.style.cursor = "ew-resize";
    document.body.style.userSelect = "none";
    document.addEventListener("mousemove", handleMove);
    document.addEventListener("mouseup", handleUp);
  };

  return (
    <SidePanelFrame
      variant="docked"
      ariaLabel="File tree inspector"
      className="h-[55%] min-h-0 w-full shrink-0 border-t border-edge bg-surface md:h-auto md:w-[var(--file-tree-inspector-width)] md:min-w-[20rem] md:max-w-[70%] md:border-l-0 md:border-t-0"
      contentClassName="gap-5 overscroll-contain px-4 sm:px-5"
      contentTestId="file-tree-inspector-scroll"
      header={
        <PanelHeader
          title={title}
          titleColor={PANEL_ACCENT}
          subtitle={subtitle}
          preserveSubtitleEnd
        />
      }
      leading={
        <SidePanelResizeHandle
          accentColor={PANEL_ACCENT}
          className="hidden rounded-none md:block"
          onMouseDown={startResize}
          title="Drag left or right to resize"
        />
      }
      onClose={onClose}
      style={panelStyle}
    >
      {inspectedItem.kind === "file" ? (
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
      {inspectedItem.kind === "file" && (
        <>
          <div className="flex justify-center border-y border-edge py-4">
            <button
              type="button"
              onClick={(event) => {
                onViewInGraph(event, inspectedItem.file.id);
              }}
              disabled={duplicatingNodeId !== null}
              className="w-full cursor-pointer rounded-lg border border-accent px-3 py-2 text-xs font-bold uppercase tracking-wide text-accent hover:bg-accent-muted disabled:cursor-not-allowed disabled:opacity-50"
            >
              {duplicatingNodeId === inspectedItem.file.id
                ? "Opening..."
                : "View in graph"}
            </button>
          </div>
          <section>
            <PanelSectionTitle className="mb-3">
              File information
            </PanelSectionTitle>
            <div className="flex flex-col gap-3">
              <PanelDetailRow label="File path" value={inspectedItem.file.id} />
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
    </SidePanelFrame>
  );
};

export default FileTreeInspector;
