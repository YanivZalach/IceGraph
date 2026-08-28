import type { MouseEvent } from "react";
import { getInspectedRowId } from "../model/fileTreeRows";
import type { FileTreeRow } from "../model/fileTreeRows";
import type { FileTreeSelection } from "../hooks/useFileTreeState";
import type { DataFileNode } from "../types";
import FileTreeFileRow from "./FileTreeFileRow";
import FileTreeGroupRow from "./FileTreeGroupRow";
import type { GroupCheckedState } from "./FileTreeGroupRow";
import FileTreeVirtualList from "./FileTreeVirtualList";

const getGroupCheckedState = (
  files: DataFileNode[],
  checkedFileIds: Set<string>,
): GroupCheckedState => {
  if (files.length === 0) return "none";
  if (files.every((file) => checkedFileIds.has(file.id))) return "all";
  return files.some((file) => checkedFileIds.has(file.id)) ? "some" : "none";
};

interface FileTreeContentProps {
  duplicatingNodeId: string | null;
  onViewInGraph: (event: MouseEvent<HTMLButtonElement>, fileId: string) => void;
  rows: FileTreeRow[];
  search: string;
  selection: FileTreeSelection;
}

const FileTreeContent = ({
  duplicatingNodeId,
  onViewInGraph,
  rows,
  search,
  selection,
}: FileTreeContentProps) => {
  const inspectedRowId = getInspectedRowId(selection.inspectedItem);
  const renderVisibleRow = (row: FileTreeRow) => (
    <div role="none" className="flex w-full">
      {Array.from({ length: row.depth }, (_, indentIndex) => (
        <span key={indentIndex} aria-hidden="true" className="w-4 shrink-0" />
      ))}
      <div role="none" className="min-w-0 flex-1">
        {row.kind === "file" ? (
          <FileTreeFileRow
            ariaLevel={row.ariaLevel}
            duplicatingNodeId={duplicatingNodeId}
            file={row.file}
            isChecked={selection.checkedFileIds.has(row.file.id)}
            isInspected={inspectedRowId === row.id}
            onInspect={() => {
              selection.inspect({ file: row.file, kind: "file" });
            }}
            onToggleChecked={() => {
              selection.toggleChecked(row.file.id);
            }}
            onViewInGraph={onViewInGraph}
          />
        ) : (
          <FileTreeGroupRow
            checkedState={getGroupCheckedState(
              row.files,
              selection.checkedFileIds,
            )}
            isExpanded={selection.expandedItemIds.has(row.id)}
            isInspected={inspectedRowId === row.id}
            onCollapseNested={() => {
              selection.collapseItems([row.id, ...row.nestedGroupIds]);
            }}
            onExpandNested={() => {
              selection.expandItems([row.id, ...row.nestedGroupIds]);
            }}
            onInspect={() => {
              selection.inspect(row.inspectionTarget);
            }}
            onToggleChecked={() => {
              selection.toggleFiles(row.files);
            }}
            onToggleExpanded={() => {
              selection.toggleExpanded(row.id);
            }}
            row={row}
          />
        )}
      </div>
    </div>
  );

  if (rows.length === 0) {
    return (
      <div
        data-testid="file-tree-content-scroll"
        className="min-h-0 flex-1 overflow-y-auto"
      >
        <p className="mt-4 text-sm italic text-slate-500">
          {search === ""
            ? "No data files found for this snapshot and scope."
            : "No partitions match the search."}
        </p>
      </div>
    );
  }

  return <FileTreeVirtualList renderRow={renderVisibleRow} rows={rows} />;
};

export default FileTreeContent;
