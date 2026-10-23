import { PanelSectionTitle } from "../../../components/PanelContent";
import type { Partition } from "../api/graphSchemas";
import PartitionLabel from "./PartitionLabel";

type PartitionValuesProps =
  { kind: "file"; value: Partition } | { kind: "manifest"; value: Partition[] };

const PartitionValues = ({ kind, value }: PartitionValuesProps) => {
  const partitions = kind === "file" ? [value] : value;
  const title = kind === "file" ? "Partition" : "Partitions";

  return (
    <div>
      <PanelSectionTitle>{title}</PanelSectionTitle>
      <div className="overflow-x-auto rounded-lg border border-edge bg-canvas font-mono text-xs font-normal text-white">
        <ul aria-label={title} className="min-w-max divide-y divide-edge">
          {partitions.length === 0 ? (
            <li className="whitespace-nowrap px-3 py-2">
              No partitions collected
            </li>
          ) : (
            partitions.map((partition, index) => (
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
