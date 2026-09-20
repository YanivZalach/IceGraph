import MetadataHelp from "../../metadata/components/MetadataHelp";
import {
  SPEC_CELL_CLASS,
  SPEC_HEAD_CLASS,
  SPEC_TABLE_CLASS,
  SpecHeaderCell,
  SpecRow,
  SpecSourceColumn,
  SpecValue,
} from "./SpecFieldTable";
import {
  SPEC_ROW_MARKER,
  type PartitionField,
  type SpecFieldRow,
} from "../specFieldRows";

interface PartitionFieldTableProps {
  rows: SpecFieldRow<PartitionField>[];
  columnNames: ReadonlyMap<string, string>;
}

const PartitionFieldTable = ({
  rows,
  columnNames,
}: PartitionFieldTableProps) => {
  if (rows.length === 0)
    return <p className="text-sm italic text-slate-400">Unpartitioned.</p>;
  return (
    <div className="overflow-x-auto">
      <table className={SPEC_TABLE_CLASS}>
        <thead className={SPEC_HEAD_CLASS}>
          <tr>
            <SpecHeaderCell>
              <MetadataHelp label="Partition field ID">
                The ID Iceberg assigns to the partition field itself, separate
                from the source column ID.
              </MetadataHelp>
            </SpecHeaderCell>
            <SpecHeaderCell>Source column (schema ID)</SpecHeaderCell>
            <SpecHeaderCell>
              <MetadataHelp label="Transform">
                Derives the partition value: identity keeps it unchanged;
                hour/day/month/year group time values; bucket[N] hashes into N
                buckets; truncate[W] applies width W; void always produces null.
              </MetadataHelp>
            </SpecHeaderCell>
            <SpecHeaderCell>
              <MetadataHelp label="Partition field name">
                The name used for partition values in file exploration.
              </MetadataHelp>
            </SpecHeaderCell>
          </tr>
        </thead>
        <tbody>
          {rows.map(({ status, field, previous }, index) => (
            <SpecRow
              key={`${String(field["field-id"] ?? "none")}.${String(index)}`}
              status={status}
            >
              <td className={`${SPEC_CELL_CLASS} font-mono text-xs`}>
                <span className="text-slate-400">
                  {(status === null ? null : SPEC_ROW_MARKER[status]) ??
                    (field["field-id"] === undefined
                      ? "—"
                      : String(field["field-id"]))}
                </span>
              </td>
              <td className={SPEC_CELL_CLASS}>
                <SpecSourceColumn
                  columnNames={columnNames}
                  sourceId={field["source-id"]}
                  previousSourceId={previous?.["source-id"]}
                />
              </td>
              <td className={SPEC_CELL_CLASS}>
                <SpecValue
                  value={field.transform ?? "—"}
                  previous={previous?.transform}
                  tone="text-accent-text"
                />
              </td>
              <td className={SPEC_CELL_CLASS}>
                <SpecValue
                  value={field.name ?? "—"}
                  previous={previous?.name}
                  tone="text-ink"
                />
              </td>
            </SpecRow>
          ))}
        </tbody>
      </table>
    </div>
  );
};
export default PartitionFieldTable;
