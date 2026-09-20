import type { ReactNode } from "react";
import { cn } from "../../../shared/lib/cn";
import { columnName } from "../../metadata/metadataPresentation";
import { SPEC_ROW_BACKGROUND, type SpecFieldStatus } from "../specFieldRows";
import type { IcebergInteger } from "../../table/api/metadataSchemas";

export const SPEC_TABLE_CLASS = "w-full min-w-[36rem] text-left text-sm";
export const SPEC_HEAD_CLASS = "bg-surface-deep text-xs text-slate-400";
export const SPEC_CELL_CLASS = "px-5 py-3 align-top";

export const SpecHeaderCell = ({ children }: { children: ReactNode }) => (
  <th className="px-5 py-3 font-medium">{children}</th>
);

export const SpecRow = ({
  status,
  children,
}: {
  status: SpecFieldStatus | null;
  children: ReactNode;
}) => (
  <tr
    className={cn(
      "border-b border-edge last:border-0",
      status !== null && SPEC_ROW_BACKGROUND[status],
    )}
  >
    {children}
  </tr>
);

export const SpecDiffMarker = ({
  marker,
  label,
  tone,
}: {
  marker: string;
  label: string;
  tone?: string;
}) => (
  <span
    className={cn(
      "inline-block w-3 shrink-0 text-center font-mono text-xs text-slate-500",
      tone,
    )}
    title={label}
  >
    <span aria-hidden="true">{marker}</span>
    <span className="sr-only">{label}</span>
  </span>
);

// Values render exactly as Iceberg records them. Changed values show the
// previous value struck through beside the current one rather than being
// merged or relabelled.
export const SpecValue = ({
  value,
  previous,
  tone,
}: {
  value: ReactNode;
  previous?: ReactNode;
  tone?: string;
}) => (
  <span className={cn("font-mono text-xs", tone)}>
    {value}
    {previous !== undefined && previous !== null && previous !== value && (
      <span className="ml-2 text-amber-600 line-through">{previous}</span>
    )}
  </span>
);

export const SpecSourceColumn = ({
  columnNames,
  sourceId,
  previousSourceId,
}: {
  columnNames: ReadonlyMap<string, string>;
  sourceId: IcebergInteger | undefined;
  previousSourceId?: IcebergInteger | undefined;
}) => (
  <>
    <code className="text-xs text-ink">
      {columnName(columnNames, sourceId)}
    </code>{" "}
    <span className="text-xs text-slate-500">
      ({sourceId === undefined ? "unknown" : String(sourceId)})
    </span>
    {previousSourceId !== undefined &&
      String(previousSourceId) !== String(sourceId) && (
        <span className="ml-2 text-xs text-amber-600 line-through">
          {columnName(columnNames, previousSourceId)} (
          {String(previousSourceId)})
        </span>
      )}
  </>
);
