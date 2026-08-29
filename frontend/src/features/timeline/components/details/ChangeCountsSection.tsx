import { cn } from "../../../../shared/lib/cn";
import { UI_FIELD_LABEL_CLASS } from "../../../../uiTypography";
import Chip, { type ChipTone } from "../Chip";
import type { DetailRowData } from "../../lib/details/buildEventDetails";
import type { ChangeCountRow } from "../../lib/details/summaryTables";
import { parseBackendSizeToBytes } from "../../lib/format/backendSize";
import { humanizeLabel, humanizeValue } from "../../lib/format/humanizeSummary";
import { SECTION_TITLE_CLASS } from "./sectionTitleClass";

interface ChangeCountsSectionProps {
  title: string;
  counts: ChangeCountRow[];
  rows: DetailRowData[];
}

const isZeroValue = (value: string): boolean =>
  /^0+$/.test(value) || parseBackendSizeToBytes(value) === 0;

interface CountCell {
  text: string;
  tone: ChipTone | null;
}

const TONE_BY_SIGN = { "+": "added", "−": "removed" } as const;

const countCell = (value: string | null, sign: "+" | "−"): CountCell => {
  if (value === null) {
    return { text: "—", tone: null };
  }
  if (isZeroValue(value)) {
    return { text: humanizeValue(value, false), tone: null };
  }
  return {
    text: `${sign}${humanizeValue(value, false)}`,
    tone: TONE_BY_SIGN[sign],
  };
};

const CELL_CLASS = "py-2 pl-3 text-right font-mono text-xs whitespace-nowrap";
const LABEL_CELL_CLASS = cn(
  UI_FIELD_LABEL_CLASS,
  "py-2 normal-case break-words",
);

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
            <th className={cn(UI_FIELD_LABEL_CLASS, "pl-3 text-right")}>
              added
            </th>
            <th className={cn(UI_FIELD_LABEL_CLASS, "pl-3 text-right")}>
              removed
            </th>
          </tr>
        </thead>
      )}
      <tbody>
        {counts.map((count) => {
          const added = countCell(count.added, "+");
          const removed = countCell(count.removed, "−");
          return (
            <tr
              key={count.metric}
              className="border-b border-edge last:border-0"
            >
              <td className={LABEL_CELL_CLASS}>
                {humanizeLabel(count.metric)}
              </td>
              <td className={cn(CELL_CLASS, "text-slate-400")}>
                {added.tone === null ? (
                  added.text
                ) : (
                  <Chip text={added.text} tone={added.tone} />
                )}
              </td>
              <td className={cn(CELL_CLASS, "text-slate-400")}>
                {removed.tone === null ? (
                  removed.text
                ) : (
                  <Chip text={removed.text} tone={removed.tone} />
                )}
              </td>
            </tr>
          );
        })}
        {rows.map((row) => (
          <tr key={row.label} className="border-b border-edge last:border-0">
            <td className={LABEL_CELL_CLASS}>{humanizeLabel(row.label)}</td>
            <td
              colSpan={2}
              className={cn(
                "py-2 pl-3 text-right font-mono text-xs break-words",
                row.value === "" || isZeroValue(row.value)
                  ? "text-slate-400"
                  : "font-semibold text-slate-100",
              )}
            >
              {row.value === "" ? "—" : humanizeValue(row.value, false)}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  </details>
);

export default ChangeCountsSection;
