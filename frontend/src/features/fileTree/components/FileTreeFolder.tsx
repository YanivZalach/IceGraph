import type { MouseEvent } from "react";
import { cn } from "../../../shared/lib/cn";
import { getAllFolderIds, getLatestFileTimestamp } from "../model";
import type { DataFileNode, FileTreeFolder as Folder } from "../types";
import FileTreeFileRow from "./FileTreeFileRow";

interface FileTreeFolderProps {
  checkedFileIds: Set<string>;
  depth: number;
  duplicatingNodeId: string | null;
  expandedItemIds: Set<string>;
  inspectedFileId: string | null;
  inspectedFolderId: string | null;
  folder: Folder;
  onCollapseMany: (folderIds: string[]) => void;
  onExpandMany: (folderIds: string[]) => void;
  onInspectFile: (file: DataFileNode) => void;
  onInspectFolder: (folder: Folder) => void;
  onToggleChecked: (fileId: string) => void;
  onToggleExpanded: (itemId: string) => void;
  onToggleFiles: (files: DataFileNode[]) => void;
  onViewInGraph: (event: MouseEvent<HTMLButtonElement>, fileId: string) => void;
}

const FileTreeFolder = ({
  checkedFileIds,
  depth,
  duplicatingNodeId,
  expandedItemIds,
  inspectedFileId,
  inspectedFolderId,
  folder,
  onCollapseMany,
  onExpandMany,
  onInspectFile,
  onInspectFolder,
  onToggleChecked,
  onToggleExpanded,
  onToggleFiles,
  onViewInGraph,
}: FileTreeFolderProps) => {
  const isExpanded = expandedItemIds.has(folder.id);
  const isAllChecked =
    folder.allFiles.length > 0 &&
    folder.allFiles.every((file) => checkedFileIds.has(file.id));
  const isSomeChecked =
    !isAllChecked &&
    folder.allFiles.some((file) => checkedFileIds.has(file.id));
  const descendantFolderIds = getAllFolderIds(folder.children);
  const latestTimestamp = getLatestFileTimestamp(folder.allFiles);

  return (
    <div
      role="treeitem"
      aria-expanded={isExpanded}
      aria-level={depth}
      aria-selected={inspectedFolderId === folder.id}
      className={cn(
        "overflow-hidden rounded-lg border bg-surface",
        inspectedFolderId === folder.id ? "border-accent" : "border-edge",
      )}
    >
      <div
        onClick={() => {
          onInspectFolder(folder);
        }}
        className="flex cursor-pointer items-center px-4 py-2.5 transition hover:bg-surface-hover"
      >
        <div className="flex min-w-0 flex-1 items-center gap-3">
          <button
            type="button"
            aria-label={`${isExpanded ? "Collapse" : "Expand"} ${folder.label}`}
            onClick={(event) => {
              event.stopPropagation();
              onToggleExpanded(folder.id);
            }}
            className={cn(
              "flex size-8 shrink-0 cursor-pointer items-center justify-center rounded text-accent transition hover:bg-accent-muted",
              isExpanded ? "" : "-rotate-90",
            )}
          >
            <svg
              className="size-4"
              viewBox="0 0 16 16"
              fill="none"
              stroke="currentColor"
              strokeWidth="2.5"
              aria-hidden="true"
            >
              <path d="M4 6l4 4 4-4" strokeLinecap="round" />
            </svg>
          </button>
          <svg
            className="size-4 shrink-0 text-slate-400"
            viewBox="0 0 20 20"
            fill="currentColor"
            aria-hidden="true"
          >
            <path d="M2 6a2 2 0 012-2h4l2 2h6a2 2 0 012 2v6a2 2 0 01-2 2H4a2 2 0 01-2-2V6z" />
          </svg>
          <span className="truncate font-mono text-xs text-slate-200">
            {folder.label}
          </span>
        </div>
        <div className="ml-4 flex shrink-0 items-center gap-2">
          {latestTimestamp !== null && (
            <span className="hidden whitespace-nowrap font-mono text-xs text-slate-500 sm:block">
              {latestTimestamp}
            </span>
          )}
          <span className="rounded bg-edge px-2 py-0.5 text-xs font-semibold text-slate-300">
            {folder.allFiles.length}
          </span>
          {folder.children.length > 0 && (
            <>
              <button
                type="button"
                title="Expand inner folders"
                aria-label={`Expand all folders inside ${folder.label}`}
                onClick={(event) => {
                  event.stopPropagation();
                  onExpandMany([folder.id, ...descendantFolderIds]);
                }}
                className="flex size-8 cursor-pointer items-center justify-center rounded text-base text-slate-600 hover:bg-edge hover:text-slate-300"
              >
                ⇊
              </button>
              <button
                type="button"
                title="Collapse inner folders"
                aria-label={`Collapse all folders inside ${folder.label}`}
                onClick={(event) => {
                  event.stopPropagation();
                  onCollapseMany([folder.id, ...descendantFolderIds]);
                }}
                className="flex size-8 cursor-pointer items-center justify-center rounded text-base text-slate-600 hover:bg-edge hover:text-slate-300"
              >
                ⇈
              </button>
            </>
          )}
          <input
            type="checkbox"
            aria-label={`Select all files in ${folder.label}`}
            checked={isAllChecked}
            ref={(element) => {
              if (element !== null) element.indeterminate = isSomeChecked;
            }}
            onChange={() => {
              onToggleFiles(folder.allFiles);
            }}
            onClick={(event) => {
              event.stopPropagation();
            }}
            className="size-3.5 cursor-pointer rounded accent-accent"
          />
        </div>
      </div>
      {isExpanded && (
        <div
          role="group"
          className="flex flex-col gap-2 border-t border-edge px-4 py-2"
        >
          {folder.children.map((child) => (
            <FileTreeFolder
              key={child.id}
              checkedFileIds={checkedFileIds}
              depth={depth + 1}
              duplicatingNodeId={duplicatingNodeId}
              expandedItemIds={expandedItemIds}
              folder={child}
              inspectedFileId={inspectedFileId}
              inspectedFolderId={inspectedFolderId}
              onCollapseMany={onCollapseMany}
              onExpandMany={onExpandMany}
              onInspectFile={onInspectFile}
              onInspectFolder={onInspectFolder}
              onToggleChecked={onToggleChecked}
              onToggleExpanded={onToggleExpanded}
              onToggleFiles={onToggleFiles}
              onViewInGraph={onViewInGraph}
            />
          ))}
          {folder.directFiles.map((file) => (
            <FileTreeFileRow
              key={file.id}
              checkedFileIds={checkedFileIds}
              duplicatingNodeId={duplicatingNodeId}
              file={file}
              isInspected={inspectedFileId === file.id}
              isTreeItem
              onInspect={onInspectFile}
              onToggleChecked={onToggleChecked}
              onViewInGraph={onViewInGraph}
            />
          ))}
        </div>
      )}
    </div>
  );
};

export default FileTreeFolder;
