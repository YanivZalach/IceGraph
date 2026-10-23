import { Fragment } from "react";
import HelpTerm from "../../../shared/components/HelpTerm";
import type { Partition } from "../api/graphSchemas";
import {
  formatPartitionFieldLabel,
  ROOT_PARTITION_LABEL,
} from "../partitionLabel";

interface PartitionLabelProps {
  partition: Partition;
}

const PartitionLabel = ({ partition }: PartitionLabelProps) => {
  if (partition.length === 0) return ROOT_PARTITION_LABEL;

  return partition.map((field, fieldIndex) => (
    <Fragment key={fieldIndex}>
      {fieldIndex > 0 ? ", " : null}
      <HelpTerm label={formatPartitionFieldLabel(field)}>
        <span className="block whitespace-pre-wrap break-all font-mono">
          {JSON.stringify(field, null, 2)}
        </span>
      </HelpTerm>
    </Fragment>
  ));
};

export default PartitionLabel;
