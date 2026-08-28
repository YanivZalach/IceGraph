import type { ReactNode } from "react";
import { cn } from "../../../shared/lib/cn";
import type { SchemaFieldDiff } from "../schemaDiff";
import {
  getSchemaFieldDiffLabel,
  getSchemaFieldDiffMarker,
} from "../schemaDiffPresentation";
import SchemaDiffValue from "./SchemaDiffValue";
import SchemaDiffTypeView from "./SchemaDiffTypeView";

interface SchemaDiffFieldRowProps {
  fieldDiff: SchemaFieldDiff;
  renderNestedFields: (fields: SchemaFieldDiff[]) => ReactNode;
}

const formatRequired = (isRequired: boolean | null): string =>
  isRequired === null ? "unknown" : isRequired ? "required" : "optional";

const SchemaDiffFieldRow = ({
  fieldDiff,
  renderNestedFields,
}: SchemaDiffFieldRowProps) => {
  const field = fieldDiff.after ?? fieldDiff.before;
  if (field === null) {
    return null;
  }

  const beforeName = fieldDiff.before?.name ?? "missing";
  const afterName = fieldDiff.after?.name ?? "missing";
  const beforeRequired = formatRequired(fieldDiff.before?.isRequired ?? null);
  const afterRequired = formatRequired(fieldDiff.after?.isRequired ?? null);

  return (
    <div
      className={cn(
        "border-b border-edge py-3 last:border-0",
        fieldDiff.status === "unchanged" && "opacity-45",
        fieldDiff.status === "added" && "bg-green-900/20",
        fieldDiff.status === "removed" && "bg-red-900/20",
        fieldDiff.status === "changed" && "border-l-2 border-l-amber-500/60",
        fieldDiff.status === "moved" && "border-l-2 border-l-sky-500/60",
      )}
    >
      <div className="grid grid-cols-[1rem_2.5rem_minmax(0,1fr)_auto] items-center gap-x-3 px-1">
        <span
          title={getSchemaFieldDiffLabel(fieldDiff)}
          className={cn(
            "text-center font-mono text-sm text-slate-500",
            fieldDiff.status === "added" && "text-green-400",
            fieldDiff.status === "removed" && "text-red-400",
            fieldDiff.status === "changed" && "text-amber-400",
            fieldDiff.status === "moved" && "text-sky-400",
          )}
        >
          <span aria-hidden="true">{getSchemaFieldDiffMarker(fieldDiff)}</span>
          <span className="sr-only">{getSchemaFieldDiffLabel(fieldDiff)}</span>
        </span>
        <span className="text-right font-mono text-sm tabular-nums text-slate-500">
          {field.id ?? "?"}
        </span>
        <div className="min-w-0 text-sm font-semibold">
          {fieldDiff.isNameChanged ? (
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-red-400 line-through">{beforeName}</span>
              <span className="text-slate-500">→</span>
              <span className="text-green-400">{afterName}</span>
            </div>
          ) : (
            <span
              className={cn(
                "text-ink",
                fieldDiff.status === "added" && "text-green-300",
                fieldDiff.status === "removed" && "text-red-300 line-through",
              )}
            >
              {field.name}
            </span>
          )}
        </div>
        {fieldDiff.isRequiredChanged ? (
          <SchemaDiffValue
            label=""
            before={beforeRequired}
            after={afterRequired}
            status="changed"
          />
        ) : field.isRequired === false ? (
          <span className="text-xs font-bold uppercase text-slate-400">
            optional
          </span>
        ) : (
          <span />
        )}
      </div>
      <div className="ml-16 mt-3">
        <SchemaDiffTypeView
          typeDiff={fieldDiff.type}
          renderNestedFields={renderNestedFields}
        />
      </div>
    </div>
  );
};

export default SchemaDiffFieldRow;
