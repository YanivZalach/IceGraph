import MetadataHelp from "../../metadata/components/MetadataHelp";
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
  type SortField,
  type SpecFieldRow,
} from "../specFieldRows";

interface SortFieldTableProps {
  rows: SpecFieldRow<SortField>[];
  columnNames: ReadonlyMap<string, string>;
}

const SortFieldTable = ({ rows, columnNames }: SortFieldTableProps) => {
  if (rows.length === 0)
    return <p className="px-5 py-4 text-sm italic text-slate-400">Unsorted.</p>;
  return (
    <div className="overflow-x-auto">
      <table className={SPEC_TABLE_CLASS}>
        <thead className={SPEC_HEAD_CLASS}>
          <tr>
            <SpecHeaderCell>
              <MetadataHelp label="#">
                Sort priority. Rows are ordered by the first column, then ties
                are broken by the next one down.
              </MetadataHelp>
            </SpecHeaderCell>
            <SpecHeaderCell>Source column (schema ID)</SpecHeaderCell>
            <SpecHeaderCell>
              <MetadataHelp label="Transform">
                Determines the sort key. identity uses the original column value
                unchanged.
              </MetadataHelp>
            </SpecHeaderCell>
            <SpecHeaderCell>
              <MetadataHelp label="Direction">
                asc sorts ascending, desc sorts descending.
              </MetadataHelp>
            </SpecHeaderCell>
            <SpecHeaderCell>
              <MetadataHelp label="Nulls">
                nulls-first places null values before non-null values;
                nulls-last places them after.
              </MetadataHelp>
            </SpecHeaderCell>
          </tr>
        </thead>
        <tbody>
          {rows.map(
            (
              { status, field, previous, position, previousPosition },
              index,
            ) => (
              <SpecRow
                key={`${String(field["source-id"] ?? "none")}.${String(index)}`}
                status={status}
              >
                <td className={`${SPEC_CELL_CLASS} font-mono text-xs`}>
                  <span className="flex items-center gap-2 text-slate-400">
                    {status !== null && (
                      <SpecDiffMarker
                        marker={SPEC_ROW_MARKER[status] ?? ""}
                        label={`${status} sort field`}
                      />
                    )}
                    <SpecValue
                      value={position}
                      previous={previousPosition ?? undefined}
                      tone="text-slate-400"
                    />
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
                    value={field.transform ?? "?"}
                    previous={previous?.transform}
                    tone="text-accent-text"
                  />
                </td>
                <td className={SPEC_CELL_CLASS}>
                  <SpecValue
                    value={field.direction ?? "?"}
                    previous={previous?.direction}
                    tone="text-ink"
                  />
                </td>
                <td className={SPEC_CELL_CLASS}>
                  <SpecValue
                    value={field["null-order"] ?? "?"}
                    previous={previous?.["null-order"]}
                    tone="text-ink"
                  />
                </td>
              </SpecRow>
            ),
          )}
        </tbody>
      </table>
    </div>
  );
};
export default SortFieldTable;
