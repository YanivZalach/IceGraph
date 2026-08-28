import type { MouseEvent } from "react";
import PanelIssueNotice from "../../components/PanelIssueNotice";
import { useViewInGraph } from "../../hooks/useViewInGraph";
import FileTreeContent from "./components/FileTreeContent";
import FileTreeInspector from "./components/FileTreeInspector";
import FileTreeToolbar from "./components/FileTreeToolbar";
import FileTreeViewSettings from "./components/FileTreeViewSettings";
import { buildFileTreeGraphIndex } from "./model/graphIndex";
import {
  getBranches,
  getDisplayedSnapshots,
  selectCurrentSnapshot,
} from "./model/snapshotSelection";
import { getSnapshotFileResult } from "./model/snapshotLineage";
import { buildVisibleFileTreeRows } from "./model/fileTreeRows";
import {
  buildPartitionPathTree,
  getAllPartitionPathNodeIds,
  groupFilesByPartition,
} from "./model/partitionModel";
import type { FileTreeContext } from "./schemas";
import { useFileTreeState } from "./hooks/useFileTreeState";

interface FileTreeViewProps {
  graphData: FileTreeContext;
}

const FileTreeView = ({ graphData }: FileTreeViewProps) => {
  const { duplicatingNodeId, viewInGraph } = useViewInGraph();
  const activeDuplicatingNodeId =
    typeof duplicatingNodeId === "string" ? duplicatingNodeId : null;

  const { selection, viewSettings } = useFileTreeState();

  const graphIndex = buildFileTreeGraphIndex(graphData);
  const branches = getBranches(graphData);
  const selectedBranchName = branches.some(
    (branch) => branch.name === viewSettings.requestedBranchName,
  )
    ? viewSettings.requestedBranchName
    : null;
  const displayedSnapshots = getDisplayedSnapshots(
    graphIndex,
    branches,
    selectedBranchName,
  );
  const currentSnapshotSelection = selectCurrentSnapshot(
    displayedSnapshots,
    viewSettings.requestedSnapshotId,
  );
  const currentSnapshot = currentSnapshotSelection.snapshot;
  const currentSnapshotId =
    currentSnapshot?.details.snapshot_id ?? currentSnapshot?.id ?? "";
  const snapshotFileResult = getSnapshotFileResult(
    currentSnapshot,
    graphIndex,
    viewSettings.scope,
  );
  const partitions = groupFilesByPartition(
    snapshotFileResult.files,
    viewSettings.search,
  );
  const partitionPathNodes = buildPartitionPathTree(partitions);
  const visibleFiles = partitions.flatMap((partition) => partition.files);
  const visibleRows = buildVisibleFileTreeRows(
    partitions,
    partitionPathNodes,
    selection.expandedItemIds,
    viewSettings.viewMode,
  );
  const snapshotWarnings = [
    ...new Set([
      ...currentSnapshotSelection.warnings,
      ...snapshotFileResult.warnings,
    ]),
  ];

  const handleViewInGraph = (
    event: MouseEvent<HTMLButtonElement>,
    fileId: string,
  ) => {
    void viewInGraph(event, fileId);
  };
  const handleExpandAll = () => {
    selection.expandItems(
      viewSettings.viewMode === "tree"
        ? [
            ...getAllPartitionPathNodeIds(partitionPathNodes),
            ...partitions
              .filter(({ name }) => name === "(unpartitioned)")
              .map(({ id }) => id),
          ]
        : partitions.map(({ id }) => id),
    );
  };

  return (
    <div className="h-graph flex w-full min-h-0 flex-none flex-col overflow-hidden bg-canvas">
      <FileTreeToolbar
        checkedFileIds={selection.checkedFileIds}
        fileCount={visibleFiles.length}
        onClearSelection={() => {
          selection.selectFiles([]);
        }}
        onCollapseAll={selection.collapseAll}
        onExpandAll={handleExpandAll}
        onSearchChange={viewSettings.setSearch}
        onSelectAll={() => {
          selection.selectFiles(visibleFiles);
        }}
        partitionCount={partitions.length}
        search={viewSettings.search}
        settingsMenu={
          <FileTreeViewSettings
            branches={branches}
            currentSnapshotId={currentSnapshotId}
            selectedBranchName={selectedBranchName}
            snapshots={displayedSnapshots}
            viewSettings={viewSettings}
          />
        }
      />
      <div className="flex min-h-0 flex-1 flex-col md:flex-row">
        <main
          className={`flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden px-3 py-3 sm:px-8 sm:py-4 ${
            selection.inspectedItem === null ? "" : "basis-[45%] md:basis-auto"
          }`}
        >
          {snapshotFileResult.errors.length > 0 && (
            <div className="mb-3">
              <PanelIssueNotice type="error">
                {snapshotFileResult.errors.join("\n")}
              </PanelIssueNotice>
            </div>
          )}
          {snapshotWarnings.length > 0 && (
            <div className="mb-3">
              <PanelIssueNotice type="warning">
                {snapshotWarnings.join("\n")}
              </PanelIssueNotice>
            </div>
          )}
          {currentSnapshot === undefined ? (
            <p className="flex h-full items-center justify-center text-sm italic text-slate-500">
              No snapshots available for this branch in the loaded range.
            </p>
          ) : (
            <FileTreeContent
              duplicatingNodeId={activeDuplicatingNodeId}
              onViewInGraph={handleViewInGraph}
              rows={visibleRows}
              search={viewSettings.search}
              selection={selection}
            />
          )}
        </main>
        {selection.inspectedItem !== null && (
          <FileTreeInspector
            duplicatingNodeId={activeDuplicatingNodeId}
            inspectedItem={selection.inspectedItem}
            onClose={selection.closeInspector}
            onViewInGraph={handleViewInGraph}
          />
        )}
      </div>
    </div>
  );
};

export default FileTreeView;
