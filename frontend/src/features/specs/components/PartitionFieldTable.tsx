import HelpTerm from "../../../shared/components/HelpTerm";
import {
  SPEC_CELL_CLASS,
  SPEC_HEAD_CLASS,
  SPEC_TABLE_CLASS,
  SpecDiffMarker,
  SpecHeaderCell,
  SpecRow,
  SpecSourceColumn,
  SpecValue,
} from "./SpecFieldTable";
import {
  SPEC_ROW_MARKER,
  SPEC_ROW_MARKER_TONE,
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
    return (
      <p className="px-5 py-4 text-sm italic text-slate-400">Unpartitioned.</p>
    );
  return (
    <div className="overflow-x-auto">
      <table className={SPEC_TABLE_CLASS}>
        <thead className={SPEC_HEAD_CLASS}>
          <tr>
            <SpecHeaderCell>
              <HelpTerm label="Partition field ID">
                The ID Iceberg assigns to the partition field itself, separate
                from the source column ID.
              </HelpTerm>
            </SpecHeaderCell>
            <SpecHeaderCell>Source column (schema ID)</SpecHeaderCell>
            <SpecHeaderCell>
              <HelpTerm label="Transform">
                Derives the partition value: identity keeps it unchanged;
                hour/day/month/year group time values; bucket[N] hashes into N
                buckets; truncate[W] applies width W; void always produces null.
              </HelpTerm>
            </SpecHeaderCell>
            <SpecHeaderCell>
              <HelpTerm label="Partition field name">
                The name used for partition values in file exploration.
              </HelpTerm>
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
                <span className="flex items-center gap-2 text-slate-400">
                  {status !== null && (
                    <SpecDiffMarker
                      marker={SPEC_ROW_MARKER[status] ?? ""}
                      label={`${status} partition field`}
                      tone={SPEC_ROW_MARKER_TONE[status]}
                    />
                  )}
                  <span
                    className={
                      status === "removed"
                        ? "text-red-400 line-through"
                        : status === "added"
                          ? "text-green-400"
                          : undefined
                    }
                  >
                    {field["field-id"] === undefined
                      ? "?"
                      : String(field["field-id"])}
                  </span>
                </span>
              </td>
              <td className={SPEC_CELL_CLASS}>
                <SpecSourceColumn
                  columnNames={columnNames}
                  sourceId={field["source-id"]}
                  previousSourceId={previous?.["source-id"]}
                  status={status}
                />
              </td>
              <td className={SPEC_CELL_CLASS}>
                <SpecValue
                  value={field.transform ?? "?"}
                  previous={previous?.transform}
                  tone="text-accent-text"
                  status={status}
                />
              </td>
              <td className={SPEC_CELL_CLASS}>
                <SpecValue
                  value={field.name ?? "?"}
                  previous={previous?.name}
                  tone="text-ink"
                  status={status}
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
