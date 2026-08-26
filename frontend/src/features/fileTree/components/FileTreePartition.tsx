import type { ReactNode } from "react";
import { cn } from "../../../shared/lib/cn";
import type { DataFileNode, PartitionGroup } from "../types";
import FileTreeGroupHeader from "./FileTreeGroupHeader";

interface FileTreePartitionProps {
  checkedFileIds: Set<string>;
  expandedItemIds: Set<string>;
  isInspected: boolean;
  onInspectPartition: (partition: PartitionGroup) => void;
  onToggleExpanded: (itemId: string) => void;
  onToggleFiles: (files: DataFileNode[]) => void;
  partition: PartitionGroup;
  renderFile: (file: DataFileNode, isTreeItem: boolean) => ReactNode;
}

const FileTreePartition = ({
  checkedFileIds,
  expandedItemIds,
  isInspected,
  onInspectPartition,
  onToggleExpanded,
  onToggleFiles,
  partition,
  renderFile,
}: FileTreePartitionProps) => {
  const isExpanded = expandedItemIds.has(partition.id);

  return (
    <section
      aria-label={partition.name}
      className={cn(
        "overflow-hidden rounded-lg border bg-surface",
        isInspected ? "border-accent" : "border-edge",
      )}
    >
      <FileTreeGroupHeader
        accessibleLabel={partition.name}
        additionalActions={null}
        checkedFileIds={checkedFileIds}
        files={partition.files}
        icon={null}
        isExpanded={isExpanded}
        label={partition.name}
        onInspect={() => {
          onInspectPartition(partition);
        }}
        onToggleExpanded={() => {
          onToggleExpanded(partition.id);
        }}
        onToggleFiles={onToggleFiles}
      />
      {isExpanded && (
        <div
          role="list"
          className="flex flex-col gap-1 border-t border-edge px-4 py-2"
        >
          {partition.files.map((file) => renderFile(file, false))}
        </div>
      )}
    </section>
  );
};

export default FileTreePartition;
