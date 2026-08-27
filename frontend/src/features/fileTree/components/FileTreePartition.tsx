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
}

const FileTreePartition = ({
  checkedFileIds,
  expandedItemIds,
  isInspected,
  onInspectPartition,
  onToggleExpanded,
  onToggleFiles,
  partition,
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
    </section>
  );
};

export default FileTreePartition;
