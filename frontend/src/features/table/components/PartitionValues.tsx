import { useState } from "react";
import {
  DEFAULT_COLLAPSE_LINES,
  PANEL_COLLAPSE_TOGGLE_CLASS,
  PanelSectionTitle,
} from "../../../components/PanelContent";
import { cn } from "../../../shared/lib/cn";
import type { Partition } from "../api/graphSchemas";
import { formatPartitionLabel } from "../partitionLabel";
import PartitionLabel from "./PartitionLabel";

export interface ManifestPartitionFilter {
  activePartitionLabel: string | null;
  onShowPartition: (partitionLabel: string) => void;
}

type PartitionValuesProps =
  | { kind: "file"; value: Partition }
  | {
      kind: "manifest";
      value: Partition[];
      partitionFilter?: ManifestPartitionFilter | undefined;
    };

const ShowPartitionButton = ({
  partition,
  partitionFilter,
}: {
  partition: Partition;
  partitionFilter: ManifestPartitionFilter;
}) => {
  const partitionLabel = formatPartitionLabel(partition);
  const isActive = partitionFilter.activePartitionLabel === partitionLabel;

  return (
    <button
      type="button"
      aria-label={`Show only the files of partition ${partitionLabel}`}
      aria-pressed={isActive}
      title="Show only this partition's files in the graph"
      onClick={() => {
        partitionFilter.onShowPartition(partitionLabel);
      }}
      className={cn(
        "flex size-5 shrink-0 cursor-pointer items-center justify-center rounded text-sm leading-none text-slate-500 transition hover:bg-amber-400/15 hover:text-amber-400",
        isActive && "bg-amber-400/20 text-amber-400",
      )}
    >
      ▸
    </button>
  );
};

const PartitionValues = (props: PartitionValuesProps) => {
  const { kind, value } = props;
  const partitions = kind === "file" ? [value] : value;
  const partitionFilter =
    props.kind === "manifest" ? props.partitionFilter : undefined;
  const title = kind === "file" ? "Partition" : "Partitions";
  const [isCollapsed, setIsCollapsed] = useState(true);
  const isCollapsible = partitions.length > DEFAULT_COLLAPSE_LINES;
  const visiblePartitions =
    isCollapsible && isCollapsed
      ? partitions.filter(
          (partition, index) =>
            index < DEFAULT_COLLAPSE_LINES ||
            formatPartitionLabel(partition) ===
              partitionFilter?.activePartitionLabel,
        )
      : partitions;

  return (
    <div>
      <div className="mb-1 flex items-center justify-between gap-2">
        <PanelSectionTitle>{title}</PanelSectionTitle>
        {isCollapsible && (
          <button
            type="button"
            aria-label={`${isCollapsed ? "Expand" : "Collapse"} partitions`}
            aria-expanded={!isCollapsed}
            onClick={() => {
              setIsCollapsed((previous) => !previous);
            }}
            className={PANEL_COLLAPSE_TOGGLE_CLASS}
          >
            {isCollapsed ? `(${String(partitions.length)} partitions) ▼` : "▲"}
          </button>
        )}
      </div>
      <div className="overflow-x-auto rounded-lg border border-edge bg-canvas font-mono text-xs font-normal text-white">
        <ul aria-label={title} className="min-w-max divide-y divide-edge">
          {partitions.length === 0 ? (
            <li className="whitespace-nowrap px-3 py-2">
              No partitions collected
            </li>
          ) : (
            visiblePartitions.map((partition, index) => (
              <li
                key={index}
                className={cn(
                  "flex items-center gap-2 whitespace-nowrap px-3 py-2",
                  partitionFilter?.activePartitionLabel ===
                    formatPartitionLabel(partition) && "bg-amber-400/15",
                )}
              >
                {partitionFilter !== undefined && (
                  <ShowPartitionButton
                    partition={partition}
                    partitionFilter={partitionFilter}
                  />
                )}
                <span>
                  {partition.length === 0 ? (
                    "Root (unpartitioned)"
                  ) : (
                    <PartitionLabel partition={partition} />
                  )}
                </span>
              </li>
            ))
          )}
        </ul>
      </div>
    </div>
  );
};

export default PartitionValues;
