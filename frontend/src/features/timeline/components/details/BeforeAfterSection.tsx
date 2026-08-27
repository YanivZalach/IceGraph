import { cn } from "../../../../shared/lib/cn";
import { UI_FIELD_LABEL_CLASS } from "../../../../uiTypography";
import type { BeforeAfterRow } from "../../lib/details/summaryTables";
import { humanizeLabel, humanizeValue } from "../../lib/format/humanizeSummary";
import { SECTION_TITLE_CLASS } from "./sectionTitleClass";

interface BeforeAfterSectionProps {
  title: string;
  rows: BeforeAfterRow[];
}

const CELL_CLASS = "py-2 pl-3 text-right font-mono text-xs whitespace-nowrap";

const BeforeAfterSection = ({ title, rows }: BeforeAfterSectionProps) => (
  <details>
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
          <th className={cn(UI_FIELD_LABEL_CLASS, "pl-3 text-right")}>
            before
          </th>
          <th className={cn(UI_FIELD_LABEL_CLASS, "pl-3 text-right")}>after</th>
        </tr>
      </thead>
      <tbody>
        {rows.map((row) => {
          const hasChanged = row.before !== null && row.before !== row.after;
          return (
            <tr key={row.metric} className="border-b border-edge last:border-0">
              <td
                className={cn(
                  UI_FIELD_LABEL_CLASS,
                  "py-2 normal-case break-words",
                )}
              >
                {humanizeLabel(row.metric)}
              </td>
              <td className={cn(CELL_CLASS, "text-slate-400")}>
                {row.before === null ? "—" : humanizeValue(row.before, false)}
              </td>
              <td
                className={cn(
                  CELL_CLASS,
                  hasChanged
                    ? "font-semibold text-slate-100"
                    : "text-slate-400",
                )}
              >
                {humanizeValue(row.after, false)}
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  </details>
);

export default BeforeAfterSection;
