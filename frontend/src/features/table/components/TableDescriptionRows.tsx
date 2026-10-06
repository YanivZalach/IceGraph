import {
  SPEC_CELL_CLASS,
  SPEC_HEAD_CLASS,
  SPEC_TABLE_CLASS,
  SpecHeaderCell,
} from "../../specs/components/SpecFieldTable";
import type { TableDescriptionRow } from "../api/tableDescriptionQueries";

interface TableDescriptionRowsProps {
  rows: TableDescriptionRow[];
}

const TableDescriptionRows = ({ rows }: TableDescriptionRowsProps) => {
  const hasComments = rows.some((row) => row.comment !== "");

  return (
    <div className="overflow-x-auto">
      <table className={SPEC_TABLE_CLASS}>
        <thead className={SPEC_HEAD_CLASS}>
          <tr>
            <SpecHeaderCell>Name</SpecHeaderCell>
            <SpecHeaderCell>Type / Transform</SpecHeaderCell>
            {hasComments && <SpecHeaderCell>Comment</SpecHeaderCell>}
          </tr>
        </thead>
        <tbody className="font-mono text-xs">
          {rows.map((row, rowIndex) => (
            <tr
              key={`${row.name}.${String(rowIndex)}`}
              className="border-b border-edge last:border-0"
            >
              <td className={`${SPEC_CELL_CLASS} text-slate-300`}>
                {row.name}
              </td>
              <td className={`${SPEC_CELL_CLASS} break-all text-ink`}>
                {row.value}
              </td>
              {hasComments && (
                <td className={`${SPEC_CELL_CLASS} text-slate-400`}>
                  {row.comment}
                </td>
              )}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default TableDescriptionRows;
