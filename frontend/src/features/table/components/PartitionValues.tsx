import { useState } from "react";
import {
  DEFAULT_COLLAPSE_LINES,
  PANEL_COLLAPSE_TOGGLE_CLASS,
  PanelSectionTitle,
} from "../../../components/PanelContent";
import type { Partition } from "../api/graphSchemas";
import PartitionLabel from "./PartitionLabel";

type PartitionValuesProps =
  { kind: "file"; value: Partition } | { kind: "manifest"; value: Partition[] };

const PartitionValues = ({ kind, value }: PartitionValuesProps) => {
  const partitions = kind === "file" ? [value] : value;
  const title = kind === "file" ? "Partition" : "Partitions";
  const [isCollapsed, setIsCollapsed] = useState(true);
  const isCollapsible = partitions.length > DEFAULT_COLLAPSE_LINES;
  const visiblePartitions =
    isCollapsible && isCollapsed
      ? partitions.slice(0, DEFAULT_COLLAPSE_LINES)
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
              <li key={index} className="whitespace-nowrap px-3 py-2">
                {partition.length === 0 ? (
                  "Root (unpartitioned)"
                ) : (
                  <PartitionLabel partition={partition} />
                )}
              </li>
            ))
          )}
        </ul>
      </div>
    </div>
  );
};

export default PartitionValues;
