import { cn } from "../../../../shared/lib/cn";
import { PANEL_FIELD_LABEL_CLASS } from "../../../../components/PanelContent";
import Chip from "../Chip";
import type {
  ChangeCountCell,
  ChangeCountRow,
  ChangeValueRow,
} from "../../lib/details/summaryTables";
import { humanizeLabel } from "../../lib/format/humanizeSummary";
import { SECTION_TITLE_CLASS } from "./sectionTitleClass";

interface ChangeCountsSectionProps {
  title: string;
  counts: ChangeCountRow[];
  rows: ChangeValueRow[];
}

interface CountValueProps {
  cell: ChangeCountCell | null;
}

const CountValue = ({ cell }: CountValueProps) => {
  if (cell === null) {
    return <>—</>;
  }
  if (cell.emphasis === null) {
    return <>{cell.text}</>;
  }
  return <Chip text={cell.text} tone={cell.emphasis} isCompact />;
};

const CELL_CLASS = cn(
  "py-2 pl-3 text-right font-mono text-sm whitespace-nowrap",
  "text-slate-200",
);
const LABEL_CELL_CLASS = cn(
  PANEL_FIELD_LABEL_CLASS,
  "py-2 text-slate-400 normal-case break-words",
);
const HEADER_CELL_CLASS = cn(PANEL_FIELD_LABEL_CLASS, "pl-3 text-right");

const ChangeCountsSection = ({
  title,
  counts,
  rows,
}: ChangeCountsSectionProps) => (
  <details open>
    <summary className={`${SECTION_TITLE_CLASS} cursor-pointer`}>
      {title}
    </summary>
    <table className="w-full table-fixed">
      <colgroup>
        <col className="w-1/2" />
        <col />
        <col />
      </colgroup>
      {counts.length > 0 && (
        <thead>
          <tr>
            <td />
            <th className={HEADER_CELL_CLASS}>added</th>
            <th className={HEADER_CELL_CLASS}>removed</th>
          </tr>
        </thead>
      )}
      <tbody>
        {counts.map((count) => (
          <tr key={count.metric} className="border-b border-edge last:border-0">
            <td className={LABEL_CELL_CLASS}>{humanizeLabel(count.metric)}</td>
            <td className={CELL_CLASS}>
              <CountValue cell={count.added} />
            </td>
            <td className={CELL_CLASS}>
              <CountValue cell={count.removed} />
            </td>
          </tr>
        ))}
        {rows.map((row) => (
          <tr key={row.label} className="border-b border-edge last:border-0">
            <td className={LABEL_CELL_CLASS}>{humanizeLabel(row.label)}</td>
            <td
              colSpan={2}
              className={cn(
                "py-2 pl-3 text-right font-mono text-sm break-words",
                row.isEmphasized
                  ? "font-semibold text-slate-100"
                  : "text-slate-200",
              )}
            >
              {row.text ?? "—"}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  </details>
);

export default ChangeCountsSection;
