import type { ReactNode } from "react";
import { cn } from "../../../shared/lib/cn";
import type { SchemaFieldDiff } from "../schemaDiff";
import { formatRequiredness } from "../schemaModel";
import {
  getSchemaFieldDiffLabel,
  getSchemaFieldDiffMarker,
} from "../schemaDiffPresentation";
import SchemaDiffValue from "./SchemaDiffValue";
import SchemaFieldDoc from "./SchemaFieldDoc";
import SchemaDiffTypeView from "./SchemaDiffTypeView";
import {
  SPEC_CELL_CLASS,
  SpecDiffMarker,
  SpecRow,
} from "../../specs/components/SpecFieldTable";
import type { SpecFieldStatus } from "../../specs/specFieldRows";

interface SchemaDiffFieldRowProps {
  fieldDiff: SchemaFieldDiff;
  isNested: boolean;
  renderNestedFields: (fields: SchemaFieldDiff[]) => ReactNode;
}

const tableStatus = (fieldDiff: SchemaFieldDiff): SpecFieldStatus => {
  if (
    fieldDiff.status === "added" ||
    fieldDiff.status === "removed" ||
    fieldDiff.status === "unchanged"
  ) {
    return fieldDiff.status;
  }
  return "changed";
};

const markerTone = (fieldDiff: SchemaFieldDiff): string => {
  if (fieldDiff.status === "added") return "text-green-400";
  if (fieldDiff.status === "removed") return "text-red-400";
  if (fieldDiff.status === "moved") return "text-sky-400";
  if (fieldDiff.status === "changed") return "text-amber-400";
  return "text-slate-500";
};

const SchemaDiffFieldRow = ({
  fieldDiff,
  isNested,
  renderNestedFields,
}: SchemaDiffFieldRowProps) => {
  const field = fieldDiff.after ?? fieldDiff.before;
  if (field === null) {
    return null;
  }

  const beforeName = fieldDiff.before?.name ?? "missing";
  const afterName = fieldDiff.after?.name ?? "missing";
  const beforeRequired = formatRequiredness(
    fieldDiff.before?.isRequired ?? null,
  );
  const afterRequired = formatRequiredness(fieldDiff.after?.isRequired ?? null);
  const marker = getSchemaFieldDiffMarker(fieldDiff);
  const markerLabel = getSchemaFieldDiffLabel(fieldDiff);
  const name = fieldDiff.isNameChanged ? (
    <SchemaDiffValue before={beforeName} after={afterName} status="changed" />
  ) : (
    <code
      className={cn(
        "text-xs text-ink",
        fieldDiff.status === "added" && "text-green-400",
        fieldDiff.status === "removed" && "text-red-400 line-through",
      )}
    >
      {field.name}
    </code>
  );
  const doc = fieldDiff.isDocChanged ? (
    <SchemaFieldDoc className="mt-1">
      <span className="flex flex-wrap gap-2">
        <span className="text-red-400 line-through">
          {fieldDiff.before?.doc ?? "none"}
        </span>
        <span className="text-slate-500">→</span>
        <span className="text-green-400">{fieldDiff.after?.doc ?? "none"}</span>
      </span>
    </SchemaFieldDoc>
  ) : field.doc ? (
    <SchemaFieldDoc className="mt-1">{field.doc}</SchemaFieldDoc>
  ) : null;
  const column = (
    <div>
      {name}
      {doc}
    </div>
  );
  const required = (
    <SchemaDiffValue
      before={beforeRequired}
      after={afterRequired}
      status={fieldDiff.isRequiredChanged ? "changed" : fieldDiff.status}
    />
  );
  const type = (
    <SchemaDiffTypeView
      typeDiff={fieldDiff.type}
      renderNestedFields={renderNestedFields}
    />
  );

  if (!isNested) {
    return (
      <SpecRow status={tableStatus(fieldDiff)}>
        <td className={`${SPEC_CELL_CLASS} text-slate-400`}>
          <span className="flex items-center gap-2">
            <SpecDiffMarker
              marker={marker}
              label={markerLabel}
              tone={markerTone(fieldDiff)}
            />
            <span className="font-mono text-xs">{field.id ?? "?"}</span>
          </span>
        </td>
        <td className={SPEC_CELL_CLASS}>{column}</td>
        <td className={SPEC_CELL_CLASS}>{type}</td>
        <td className={SPEC_CELL_CLASS}>{required}</td>
      </SpecRow>
    );
  }

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
      <div className="flex items-center gap-2 px-1">
        <SpecDiffMarker
          marker={marker}
          label={markerLabel}
          tone={markerTone(fieldDiff)}
        />
        <span className="w-7 shrink-0 text-right font-mono text-sm text-slate-500">
          {field.id ?? "?"}
        </span>
        <div className="min-w-0 text-sm font-semibold">{column}</div>
        {required}
      </div>
      <div className="ml-12 mt-3">{type}</div>
    </div>
  );
};

export default SchemaDiffFieldRow;
