import CopyIconButton from "../../../components/CopyIconButton";
import { UI_FIELD_LABEL_CLASS } from "../../../uiTypography";
import type { DetailRowData } from "../lib/buildEventDetailSections";
import { SECTION_TITLE_CLASS } from "./EventDetails";

interface CollapsibleTableSectionProps {
  title: string;
  rows: DetailRowData[];
  isOpenByDefault?: boolean;
}

const CollapsibleTableSection = ({
  title,
  rows,
  isOpenByDefault = false,
}: CollapsibleTableSectionProps) => (
  <details open={isOpenByDefault}>
    <summary className={`${SECTION_TITLE_CLASS} cursor-pointer`}>
      {title}
    </summary>
    <div className="flex flex-col">
      {rows.map((row) => (
        <div
          key={row.label}
          className="flex items-start justify-between gap-3 border-b border-edge/50 py-1.5 last:border-0"
        >
          <span className={`${UI_FIELD_LABEL_CLASS} shrink-0 normal-case`}>
            {row.label}
          </span>
          <span className="flex min-w-0 items-start gap-1.5 text-right font-mono text-xs break-all text-slate-200">
            {row.value === "" ? "—" : row.value}
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
