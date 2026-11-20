import type { MouseEvent } from "react";
import { useViewInGraph } from "../../hooks/useViewInGraph";
import FileTreeContent from "../fileTree/components/FileTreeContent";
import FileTreeInspector from "../fileTree/components/FileTreeInspector";
import { isDataFileNode } from "../fileTree/model/graphIndex";
import { getAllPartitionPathNodeIds } from "../fileTree/model/partitionModel";
import type { FileTreeContext } from "../fileTree/schemas";
import type { PartitionFilesSummary } from "./api/partitionFilesSchemas";
import PartitionFilesToolbar from "./components/PartitionFilesToolbar";
import PartitionGroupBySelector from "./components/PartitionGroupBySelector";
import {
  groupPartitionFiles,
  listGroupableFields,
} from "./partitionFileGroups";
import { usePartitionFilesState } from "./usePartitionFilesState";

interface PartitionFilesViewProps {
  fileTreeContext: FileTreeContext;
  summary: PartitionFilesSummary;
}

const PartitionFilesView = ({
  fileTreeContext,
  summary,
}: PartitionFilesViewProps) => {
  const { viewInGraph } = useViewInGraph("/partition/graph");
  const state = usePartitionFilesState();
  const files = fileTreeContext.nodes.filter(isDataFileNode);
  const groupableFields = listGroupableFields(
    files,
    summary.filters.map(({ name }) => name),
  );
  const groupByFields = state.requestedGroupByFields.filter((fieldName) =>
    groupableFields.includes(fieldName),
  );
  const { partitions, partitionPathNodes } = groupPartitionFiles(
    files,
    groupByFields,
    state.fileSearch,
  );
  const visibleFiles = partitionPathNodes.flatMap((node) => node.allFiles);
  const expandedItemIds =
    state.expandedItemIds ?? new Set(partitionPathNodes.map((node) => node.id));
  const updateExpanded = (nextIds: Set<string>) => {
    state.setExpandedItemIds(nextIds);
  };
  const checkedFilePaths = [
    ...new Set(
      files
        .filter((file) => state.checkedFileIds.has(file.id))
        .map((file) => file.details.file_path ?? file.id),
    ),
  ];
  const inspectedItem = state.inspectedItem;
  const handleViewInGraph = (
    event: MouseEvent<HTMLButtonElement>,
    fileId: string,
  ) => {
    viewInGraph(event, fileId);
  };

  return (
    <div className="h-graph flex w-full min-h-0 flex-none flex-col overflow-hidden bg-canvas">
      <PartitionFilesToolbar
        checkedFilePaths={checkedFilePaths}
        fileCount={visibleFiles.length}
        onClearSelection={() => {
          state.selectFiles([]);
        }}
        onCollapseAll={() => {
          updateExpanded(new Set());
        }}
        onExpandAll={() => {
          updateExpanded(
            new Set(getAllPartitionPathNodeIds(partitionPathNodes)),
          );
        }}
        onSearchChange={state.setFileSearch}
        onSelectAll={() => {
          state.selectFiles(visibleFiles);
        }}
        search={state.fileSearch}
      />
      <div className="flex min-h-0 flex-1 flex-col md:flex-row">
        <main className="flex min-h-0 min-w-0 flex-1 flex-col gap-3 overflow-hidden px-3 py-3 sm:px-8 sm:py-4">
          <div className="rounded-lg border border-edge bg-surface px-3 py-2 shadow-md">
            <PartitionGroupBySelector
              fieldNames={groupableFields}
              groupByFields={groupByFields}
              onChange={state.setGroupByFields}
            />
          </div>
          <FileTreeContent
            checkedFileIds={state.checkedFileIds}
            expandedItemIds={expandedItemIds}
            inspectedFileId={
              inspectedItem?.kind === "file" ? inspectedItem.file.id : null
            }
            inspectedPartitionPathNodeId={
              inspectedItem?.kind === "partition-path"
                ? inspectedItem.partitionPathNode.id
                : null
            }
            inspectedPartitionId={null}
            onCollapseMany={(itemIds) => {
              const nextIds = new Set(expandedItemIds);
              for (const itemId of itemIds) nextIds.delete(itemId);
              updateExpanded(nextIds);
            }}
            onExpandMany={(itemIds) => {
              updateExpanded(new Set([...expandedItemIds, ...itemIds]));
            }}
            onInspectFile={(file) => {
              state.setInspectedItem({ file, kind: "file" });
            }}
            onInspectPartitionPathNode={(partitionPathNode) => {
              state.setInspectedItem({
                kind: "partition-path",
                partitionPathNode,
              });
            }}
            onInspectPartition={(partition) => {
              state.setInspectedItem({ kind: "partition", partition });
            }}
            onToggleExpanded={(itemId) => {
              const nextIds = new Set(expandedItemIds);
              if (nextIds.has(itemId)) nextIds.delete(itemId);
              else nextIds.add(itemId);
              updateExpanded(nextIds);
            }}
            onToggleChecked={state.toggleChecked}
            onToggleFiles={state.toggleFiles}
            onViewInGraph={handleViewInGraph}
            partitions={partitions}
            partitionPathNodes={partitionPathNodes}
            search={state.fileSearch}
            viewMode="tree"
          />
        </main>
        {inspectedItem !== null && (
          <FileTreeInspector
            inspectedItem={inspectedItem}
            onClose={() => {
              state.setInspectedItem(null);
            }}
            onViewInGraph={handleViewInGraph}
          />
        )}
      </div>
    </div>
  );
};

export default PartitionFilesView;
