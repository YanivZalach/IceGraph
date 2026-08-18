import type { MouseEvent } from "react";
import {
  isEmptyValue,
  PanelDetailRow,
  PanelHeader,
  PanelSectionTitle,
} from "../../../components/PanelContent";
import SidePanelFrame from "../../../components/SidePanelFrame";
import { formatByteSize, getFileSizeBytes } from "../fileTreeModel";
import type { InspectedFileTreeItem } from "../fileTreeTypes";
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
    inspectedItem.kind === "folder"
      ? inspectedItem.folder.statistics
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

  return (
    <SidePanelFrame
      variant="docked"
      ariaLabel="File tree inspector"
      className="flex h-[55%] min-h-0 w-full shrink-0 flex-col border-t border-edge bg-surface md:h-auto md:w-[38%] md:min-w-[20rem] md:max-w-[32rem] md:border-l md:border-t-0"
      contentClassName="gap-5 overscroll-contain px-4 sm:px-5"
      contentTestId="file-tree-inspector-scroll"
      header={
        <PanelHeader
          title={title}
          titleColor="#2e86c1"
          subtitle={subtitle}
          preserveSubtitleEnd
        />
      }
      onClose={onClose}
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
              className="w-full cursor-pointer rounded-lg border border-accent px-3 py-2 text-sm text-accent hover:bg-accent-muted disabled:cursor-not-allowed disabled:opacity-50"
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
