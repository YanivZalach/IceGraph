import { cn } from "../../../../shared/lib/cn";
import { UI_FIELD_LABEL_CLASS } from "../../../../uiTypography";
import Chip from "../Chip";
import type {
  ChangeCountCell,
  ChangeCountRow,
} from "../../lib/details/summaryTables";
import { humanizeLabel } from "../../lib/format/humanizeSummary";
import { SECTION_TITLE_CLASS } from "./sectionTitleClass";

interface ChangeCountsSectionProps {
  title: string;
  counts: ChangeCountRow[];
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
  return <Chip text={cell.text} tone={cell.emphasis} />;
};

const CELL_CLASS = cn(
  "py-2 pl-3 text-right font-mono text-xs whitespace-nowrap",
  "text-slate-400",
);
const HEADER_CELL_CLASS = cn(UI_FIELD_LABEL_CLASS, "pl-3 text-right");

const ChangeCountsSection = ({ title, counts }: ChangeCountsSectionProps) => (
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
      <thead>
        <tr>
          <td />
          <th className={HEADER_CELL_CLASS}>added</th>
          <th className={HEADER_CELL_CLASS}>removed</th>
        </tr>
      </thead>
      <tbody>
        {counts.map((count) => (
          <tr key={count.metric} className="border-b border-edge last:border-0">
            <td
              className={cn(
                UI_FIELD_LABEL_CLASS,
                "py-2 normal-case break-words",
              )}
            >
              {humanizeLabel(count.metric)}
            </td>
            <td className={CELL_CLASS}>
              <CountValue cell={count.added} />
            </td>
            <td className={CELL_CLASS}>
              <CountValue cell={count.removed} />
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  </details>
);

export default ChangeCountsSection;
