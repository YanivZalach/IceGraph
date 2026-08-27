import CopyIconButton from "../../../components/CopyIconButton";
import { cn } from "../../../shared/lib/cn";
import { UI_FIELD_LABEL_CLASS } from "../../../uiTypography";
import type { DetailRowData } from "../lib/buildEventDetailSections";
import {
  formatByteSize,
  parseBackendSizeToBytes,
} from "../lib/format/backendSize";
import { SECTION_TITLE_CLASS } from "./EventDetails";

interface CollapsibleTableSectionProps {
  title: string;
  rows: DetailRowData[];
  isOpenByDefault?: boolean;
}

const thousandsSeparatorFormatter = new Intl.NumberFormat("en-US");
const WHOLE_NUMBER_PATTERN = /^\d+$/;

const humanizeLabel = (label: string): string => {
  const spaced = label.replace(/-/g, " ");
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
};

const humanizeValue = (value: string): string => {
  const sizeBytes = parseBackendSizeToBytes(value);
  if (sizeBytes !== null) {
    return formatByteSize(sizeBytes);
  }
  if (WHOLE_NUMBER_PATTERN.test(value)) {
    return thousandsSeparatorFormatter.format(Number(value));
  }
  return value;
};

const CollapsibleTableSection = ({
  title,
  rows,
  isOpenByDefault = false,
}: CollapsibleTableSectionProps) => (
  <details open={isOpenByDefault}>
    <summary className={`${SECTION_TITLE_CLASS} cursor-pointer`}>
      {title} ({rows.length.toString()})
    </summary>
    <div className="flex flex-col">
      {rows.map((row) => (
        <div
          key={row.label}
          className="flex items-start justify-between gap-3 border-b border-edge/50 py-1.5 last:border-0"
        >
          <span
            className={cn(
              UI_FIELD_LABEL_CLASS,
              "shrink-0 normal-case",
              "text-slate-400",
            )}
          >
            {humanizeLabel(row.label)}
          </span>
          <span className="flex min-w-0 items-start gap-1.5 rounded bg-canvas px-2 py-0.5 text-right font-mono text-xs break-all text-slate-200">
            {row.value === "" ? "—" : humanizeValue(row.value)}
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
