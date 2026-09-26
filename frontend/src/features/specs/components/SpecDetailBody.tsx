import type { TableMetadata } from "../../table/api/metadataSchemas";
import { findPreviousSpec, type SpecDetail } from "../tableSpecs";
import {
  diffSpecRows,
  plainSpecRows,
  type PartitionField,
  type SortField,
} from "../specFieldRows";
import PartitionFieldTable from "./PartitionFieldTable";
import SortFieldTable from "./SortFieldTable";
import SchemaDiffView from "../../schema/components/SchemaDiffView";
import SchemaFieldList from "../../schema/components/SchemaFieldList";

interface SpecDetailBodyProps {
  metadata: TableMetadata;
  detail: SpecDetail;
  isDiff: boolean;
  columnNames: ReadonlyMap<string, string>;
}

const partitionFieldIdentity = (field: PartitionField) => field["field-id"];

const sortFieldIdentity = (field: SortField) =>
  `${String(field["source-id"])}\u0000${field.transform ?? ""}`;

const SpecDetailBody = ({
  metadata,
  detail,
  isDiff,
  columnNames,
}: SpecDetailBodyProps) => {
  if (detail.type === "schema") {
    const previous = findPreviousSpec(metadata.schemas ?? [], detail.data);
    return isDiff && previous !== null ? (
      <SchemaDiffView previousSchema={previous} currentSchema={detail.data} />
    ) : (
      <SchemaFieldList schema={detail.data} />
    );
  }
  if (detail.type === "partition") {
    const previous = findPreviousSpec(
      metadata["partition-specs"] ?? [],
      detail.data,
    );
    const rows =
      isDiff && previous !== null
        ? diffSpecRows(
            previous.fields,
            detail.data.fields,
            partitionFieldIdentity,
          )
        : plainSpecRows(detail.data.fields);
    return <PartitionFieldTable rows={rows} columnNames={columnNames} />;
  }
  const previous = findPreviousSpec(metadata["sort-orders"] ?? [], detail.data);
  const rows =
    isDiff && previous !== null
      ? diffSpecRows(
          previous.fields,
          detail.data.fields,
          sortFieldIdentity,
          true,
        )
      : plainSpecRows(detail.data.fields);
  return <SortFieldTable rows={rows} columnNames={columnNames} />;
};
export default SpecDetailBody;
