import CopyIconButton from "../../../../components/CopyIconButton";
import { cn } from "../../../../shared/lib/cn";
import { UI_FIELD_LABEL_CLASS } from "../../../../uiTypography";
import type { DetailRowData } from "../../lib/details/buildEventDetails";
import { humanizeLabel, humanizeValue } from "../../lib/format/humanizeSummary";
import { SECTION_TITLE_CLASS } from "./sectionTitleClass";

interface CollapsibleTableSectionProps {
  title: string;
  rows: DetailRowData[];
  isVerbatim?: boolean;
}

const CollapsibleTableSection = ({
  title,
  rows,
  isVerbatim = false,
}: CollapsibleTableSectionProps) => (
  <details>
    <summary className={`${SECTION_TITLE_CLASS} cursor-pointer`}>
      {title}
    </summary>
    <div className="flex flex-col">
      {rows.map((row) => (
        <div
          key={row.id ?? row.label}
          className="flex flex-wrap items-center justify-between gap-x-3 gap-y-1 border-b border-edge/50 py-1.5 last:border-0"
        >
          <span
            className={cn(
              UI_FIELD_LABEL_CLASS,
              "min-w-0 normal-case break-words",
              "text-slate-400",
            )}
          >
            {isVerbatim ? row.label : humanizeLabel(row.label)}
          </span>
          <span
            className={cn(
              "ml-auto flex max-w-full min-w-0 items-center gap-1.5 rounded bg-canvas px-2 py-0.5 text-right font-mono text-xs break-all text-slate-200",
              isVerbatim && "whitespace-pre-wrap",
            )}
          >
            {row.value === ""
              ? "—"
              : isVerbatim
                ? row.value
                : humanizeValue(row.value, row.isCopyable === true)}
            {row.isCopyable === true && row.value !== "" && (
              <CopyIconButton text={row.value} />
            )}
          </span>
        </div>
      ))}
    </div>
  </details>
);

export default CollapsibleTableSection;
