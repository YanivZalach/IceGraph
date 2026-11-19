import { cn } from "../../../../shared/lib/cn";
import {
  PANEL_DIFF_AFTER_LABEL_CLASS,
  PANEL_COLLAPSE_TOGGLE_CLASS,
  PANEL_DIFF_AFTER_VALUE_CLASS,
  PANEL_DIFF_BEFORE_LABEL_CLASS,
  PANEL_DIFF_BEFORE_VALUE_CLASS,
} from "../../../../components/PanelContent";
import type { DescribedFieldDiff } from "../../lib/details/describeFieldDiff";
import DiffEntryRow from "./DiffEntryRow";

interface FieldDiffBlockProps {
  field: DescribedFieldDiff;
}

const RAW_VALUE_CLASS =
  "overflow-x-auto pr-2 text-sm break-normal whitespace-pre";

const TOGGLE_CLASS = cn("cursor-pointer", PANEL_COLLAPSE_TOGGLE_CLASS);

const unchangedCountText = (count: number): string =>
  `${count.toString()} unchanged value${count === 1 ? "" : "s"}`;

const changeSummary = (field: DescribedFieldDiff): string => {
  const counts = new Map<string, number>();
  for (const entry of field.changedEntries) {
    counts.set(entry.status, (counts.get(entry.status) ?? 0) + 1);
  }
  return [...counts.entries()]
    .map(([status, count]) => `${count.toString()} ${status}`)
    .join(" · ");
};

const FieldDiffBlock = ({ field }: FieldDiffBlockProps) => (
  <div className="@container flex flex-col gap-2 border-b border-edge py-3 last:border-0">
    <div className="flex flex-wrap items-baseline gap-x-2">
      <span className="text-base font-medium text-ink">{field.title}</span>
      <code className="text-sm text-slate-300">{field.key}</code>
      <span className="ml-auto text-sm text-slate-300">
        {changeSummary(field)}
      </span>
    </div>
    <ul className="flex flex-col gap-1.5">
      {field.changedEntries.map((entry) => (
        <DiffEntryRow key={`${entry.status}:${entry.label}`} entry={entry} />
      ))}
    </ul>
    {field.unchangedEntries.length > 0 && (
      <details className="text-sm">
        <summary className={TOGGLE_CLASS}>
          {unchangedCountText(field.unchangedEntries.length)}
        </summary>
        <ul className="mt-1.5 flex flex-col gap-1.5">
          {field.unchangedEntries.map((entry) => (
            <DiffEntryRow key={entry.label} entry={entry} />
          ))}
        </ul>
      </details>
    )}
    <details className="text-sm">
      <summary className={TOGGLE_CLASS}>Raw JSON</summary>
      <div className="mt-1.5 grid grid-cols-1 gap-2 @lg:grid-cols-2">
        <div className="min-w-0">
          <div className={PANEL_DIFF_BEFORE_LABEL_CLASS}>Before</div>
          <pre className={cn(PANEL_DIFF_BEFORE_VALUE_CLASS, RAW_VALUE_CLASS)}>
            {field.rawBefore}
          </pre>
        </div>
        <div className="min-w-0">
          <div className={PANEL_DIFF_AFTER_LABEL_CLASS}>After</div>
          <pre className={cn(PANEL_DIFF_AFTER_VALUE_CLASS, RAW_VALUE_CLASS)}>
            {field.rawAfter}
          </pre>
        </div>
      </div>
    </details>
  </div>
);

export default FieldDiffBlock;
