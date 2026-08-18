import type { MouseEvent } from "react";
import { useOutletContext } from "react-router-dom";
import PanelIssueNotice from "../../components/PanelIssueNotice";
import { useViewInGraph } from "../../hooks/useViewInGraph";
import FileTreeContent from "./components/FileTreeContent";
import FileTreeInspector from "./components/FileTreeInspector";
import FileTreeToolbar from "./components/FileTreeToolbar";
import {
  buildFileTree,
  buildFileTreeGraphIndex,
  getAllFolderIds,
  getBranches,
  getCurrentSnapshot,
  getDisplayedSnapshots,
  getSnapshotFileErrors,
  getSnapshotFiles,
  groupFilesByPartition,
} from "./fileTreeModel";
import { fileTreeContextSchema } from "./fileTreeSchemas";
import { useFileTreePageState } from "./useFileTreePageState";

const FileTreePage = () => {
  const rawContext: unknown = useOutletContext();
  const context = fileTreeContextSchema.parse(rawContext);
  const { duplicatingNodeId, viewInGraph } = useViewInGraph();
  const activeDuplicatingNodeId =
    typeof duplicatingNodeId === "string" ? duplicatingNodeId : null;

  const pageState = useFileTreePageState();

  const graphIndex = buildFileTreeGraphIndex(context);
  const branches = getBranches(context);
  const selectedBranchName = branches.some(
    (branch) => branch.name === pageState.requestedBranchName,
  )
    ? pageState.requestedBranchName
    : null;
  const displayedSnapshots = getDisplayedSnapshots(
    graphIndex,
    branches,
    selectedBranchName,
  );
  const currentSnapshot = getCurrentSnapshot(
    displayedSnapshots,
    pageState.requestedSnapshotId,
  );
  const currentSnapshotId =
    currentSnapshot?.details.snapshot_id ?? currentSnapshot?.id ?? "";
  const snapshotFiles = getSnapshotFiles(
    currentSnapshot,
    graphIndex,
    pageState.scope,
  );
  const partitions = groupFilesByPartition(snapshotFiles, pageState.search);
  const folders = buildFileTree(partitions);
  const visibleFiles = partitions.flatMap((partition) => partition.files);
  const snapshotErrors = getSnapshotFileErrors(currentSnapshot, graphIndex);

  const handleViewInGraph = (
    event: MouseEvent<HTMLButtonElement>,
    fileId: string,
  ) => {
    void viewInGraph(event, fileId);
  };

  if (currentSnapshot === undefined) {
    return (
      <div className="flex flex-1 items-center justify-center bg-canvas">
        <p className="text-sm italic text-slate-500">No snapshots available.</p>
      </div>
    );
  }

  const inspectedFileId =
    pageState.inspectedItem?.kind === "file"
      ? pageState.inspectedItem.file.id
      : null;
  const inspectedFolderId =
    pageState.inspectedItem?.kind === "folder"
      ? pageState.inspectedItem.folder.id
      : null;
  const inspectedPartitionId =
    pageState.inspectedItem?.kind === "partition"
      ? pageState.inspectedItem.partition.id
      : null;

  return (
    <div className="h-graph flex w-full min-h-0 flex-none flex-col overflow-hidden bg-canvas">
      <FileTreeToolbar
        branches={branches}
        checkedFileIds={pageState.checkedFileIds}
        currentSnapshotId={currentSnapshotId}
        fileCount={visibleFiles.length}
        onBranchChange={pageState.setBranch}
        onClearSelection={() => {
          pageState.selectFiles([]);
        }}
        onCollapseAll={pageState.collapseAll}
        onExpandAll={() => {
          pageState.expandItems(
            pageState.viewMode === "tree"
              ? [
                  ...getAllFolderIds(folders),
                  ...partitions
                    .filter(({ name }) => name === "(unpartitioned)")
                    .map(({ id }) => id),
                ]
              : partitions.map(({ id }) => id),
          );
        }}
        onScopeChange={pageState.setScope}
        onSearchChange={pageState.setSearch}
        onSelectAll={() => {
          pageState.selectFiles(visibleFiles);
        }}
        onSnapshotChange={pageState.setSnapshot}
        onViewModeChange={pageState.setViewMode}
        partitionCount={partitions.length}
        scope={pageState.scope}
        search={pageState.search}
        selectedBranchName={selectedBranchName}
        snapshots={displayedSnapshots}
        viewMode={pageState.viewMode}
      />
      <div className="flex min-h-0 flex-1 flex-col md:flex-row">
        <main
          data-testid="file-tree-content-scroll"
          className={`min-h-0 min-w-0 flex-1 overflow-y-auto px-3 py-3 sm:px-8 sm:py-4 ${
            pageState.inspectedItem === null ? "" : "basis-[45%] md:basis-auto"
          }`}
        >
          {snapshotErrors.length > 0 && (
            <div className="mb-3">
              <PanelIssueNotice type="error">
                {snapshotErrors.join("\n")}
              </PanelIssueNotice>
            </div>
          )}
          <FileTreeContent
            checkedFileIds={pageState.checkedFileIds}
            duplicatingNodeId={activeDuplicatingNodeId}
            expandedItemIds={pageState.expandedItemIds}
            folders={folders}
            inspectedFileId={inspectedFileId}
            inspectedFolderId={inspectedFolderId}
            inspectedPartitionId={inspectedPartitionId}
            onCollapseMany={pageState.collapseMany}
            onExpandMany={pageState.expandItems}
            onInspectFile={pageState.inspectFile}
            onInspectFolder={pageState.inspectFolder}
            onInspectPartition={pageState.inspectPartition}
            onToggleChecked={pageState.toggleChecked}
            onToggleExpanded={pageState.toggleExpanded}
            onToggleFiles={pageState.toggleFiles}
            onViewInGraph={handleViewInGraph}
            partitions={partitions}
            search={pageState.search}
            viewMode={pageState.viewMode}
          />
        </main>
        {pageState.inspectedItem !== null && (
          <FileTreeInspector
            duplicatingNodeId={activeDuplicatingNodeId}
            inspectedItem={pageState.inspectedItem}
            onClose={pageState.closeInspector}
            onViewInGraph={handleViewInGraph}
          />
        )}
      </div>
    </div>
  );
};

export default FileTreePage;
