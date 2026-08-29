import type { MouseEvent } from "react";
import PanelIssueNotice from "../../components/PanelIssueNotice";
import { useViewInGraph } from "../../hooks/useViewInGraph";
import FileTreeContent from "./components/FileTreeContent";
import FileTreeInspector from "./components/FileTreeInspector";
import FileTreeToolbar from "./components/FileTreeToolbar";
import { buildFileTreeGraphIndex } from "./model/graphIndex";
import {
  getBranches,
  getDisplayedSnapshots,
  selectCurrentSnapshot,
} from "./model/snapshotSelection";
import { getSnapshotFileResult } from "./model/snapshotLineage";
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
  const { viewInGraph } = useViewInGraph();

  const pageState = useFileTreeState();

  const graphIndex = buildFileTreeGraphIndex(graphData);
  const branches = getBranches(graphData);
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
  const currentSnapshotSelection = selectCurrentSnapshot(
    displayedSnapshots,
    pageState.requestedSnapshotId,
  );
  const currentSnapshot = currentSnapshotSelection.snapshot;
  const currentSnapshotId =
    currentSnapshot?.details.snapshot_id ?? currentSnapshot?.id ?? "";
  const snapshotFileResult = getSnapshotFileResult(
    currentSnapshot,
    graphIndex,
    pageState.scope,
  );
  const snapshotFiles = snapshotFileResult.files;
  const partitions = groupFilesByPartition(snapshotFiles, pageState.search);
  const partitionPathNodes = buildPartitionPathTree(partitions);
  const visibleFiles = partitions.flatMap((partition) => partition.files);
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
    viewInGraph(event, fileId);
  };

  const inspectedFileId =
    pageState.inspectedItem?.kind === "file"
      ? pageState.inspectedItem.file.id
      : null;
  const inspectedPartitionPathNodeId =
    pageState.inspectedItem?.kind === "partition-path"
      ? pageState.inspectedItem.partitionPathNode.id
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
                  ...getAllPartitionPathNodeIds(partitionPathNodes),
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
          className={`flex min-h-0 min-w-0 flex-1 flex-col overflow-hidden px-3 py-3 sm:px-8 sm:py-4 ${
            pageState.inspectedItem === null ? "" : "basis-[45%] md:basis-auto"
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
              checkedFileIds={pageState.checkedFileIds}
              expandedItemIds={pageState.expandedItemIds}
              inspectedFileId={inspectedFileId}
              inspectedPartitionPathNodeId={inspectedPartitionPathNodeId}
              inspectedPartitionId={inspectedPartitionId}
              onCollapseMany={pageState.collapseMany}
              onExpandMany={pageState.expandItems}
              onInspectFile={pageState.inspectFile}
              onInspectPartitionPathNode={pageState.inspectPartitionPathNode}
              onInspectPartition={pageState.inspectPartition}
              onToggleExpanded={pageState.toggleExpanded}
              onToggleChecked={pageState.toggleChecked}
              onToggleFiles={pageState.toggleFiles}
              onViewInGraph={handleViewInGraph}
              partitions={partitions}
              partitionPathNodes={partitionPathNodes}
              search={pageState.search}
              viewMode={pageState.viewMode}
            />
          )}
        </main>
        {pageState.inspectedItem !== null && (
          <FileTreeInspector
            inspectedItem={pageState.inspectedItem}
            onClose={pageState.closeInspector}
            onViewInGraph={handleViewInGraph}
          />
        )}
      </div>
    </div>
  );
};

export default FileTreeView;
