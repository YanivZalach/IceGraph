import type { ReactNode } from "react";
import { cn } from "../../../shared/lib/cn";
import type {
  SchemaFieldDiff,
  SchemaTypeDiff,
  SchemaDiffStatus,
} from "../schemaDiff";
import { formatUnknownType } from "../schemaModel";
import SchemaDiffValue from "./SchemaDiffValue";
import SchemaTypeView from "./SchemaTypeView";

interface SchemaDiffTypeViewProps {
  typeDiff: SchemaTypeDiff;
  renderNestedFields: (fields: SchemaFieldDiff[]) => ReactNode;
}

const formatId = (id: string | null): string =>
  id === null ? "ID unknown" : `ID ${id}`;

const formatRequired = (isRequired: boolean | null): string =>
  isRequired === null ? "unknown" : isRequired ? "required" : "optional";

const typeBadge = (label: string, status: SchemaDiffStatus): ReactNode => (
  <span
    className={cn(
      "w-fit rounded px-2 py-0.5 font-mono text-xs",
      label === "struct" && "bg-violet-900/30 text-violet-400",
      label === "list" && "bg-amber-900/40 text-amber-400",
      label === "map" && "bg-emerald-900/40 text-emerald-400",
      status === "added" && "text-green-400",
      status === "removed" && "text-red-400 line-through",
    )}
  >
    {label}
  </span>
);

const SchemaDiffTypeView = ({
  typeDiff,
  renderNestedFields,
}: SchemaDiffTypeViewProps) => {
  switch (typeDiff.kind) {
    case "primitive":
      return (
        <SchemaDiffValue
          label="Type"
          before={typeDiff.before ?? "unknown"}
          after={typeDiff.after ?? "unknown"}
          status={typeDiff.status}
        />
      );
    case "unknown":
      return (
        <div className="grid gap-2">
          {typeDiff.status !== "added" && (
            <pre
              className={cn(
                "overflow-x-auto rounded border border-edge bg-canvas p-2 font-mono text-detail text-slate-400",
                typeDiff.status === "changed" &&
                  "border-red-900/50 bg-red-950/20 text-red-300 line-through",
                typeDiff.status === "unchanged" && "opacity-60",
              )}
            >
              {formatUnknownType(typeDiff.before)}
            </pre>
          )}
          {typeDiff.status !== "removed" && typeDiff.status !== "unchanged" && (
            <pre className="overflow-x-auto rounded border border-green-900/50 bg-green-950/20 p-2 font-mono text-detail text-green-300">
              {formatUnknownType(typeDiff.after)}
            </pre>
          )}
        </div>
      );
    case "replacement":
      return (
        <div className="grid gap-3">
          <div className="rounded border border-red-900/50 bg-red-950/20 p-3">
            <div className="mb-2 text-xs font-bold uppercase text-red-400">
              Before
            </div>
            <SchemaTypeView type={typeDiff.before} />
          </div>
          <div className="rounded border border-green-900/50 bg-green-950/20 p-3">
            <div className="mb-2 text-xs font-bold uppercase text-green-400">
              After
            </div>
            <SchemaTypeView type={typeDiff.after} />
          </div>
        </div>
      );
    case "struct":
      return (
        <div className="flex flex-col gap-2">
          {typeBadge("struct", typeDiff.status)}
          {renderNestedFields(typeDiff.fields)}
        </div>
      );
    case "list":
      return (
        <div className="flex flex-col gap-2">
          {typeBadge("list", typeDiff.status)}
          <div className="ml-3 flex flex-col gap-2 border-l-2 border-edge py-1 pl-4">
            <SchemaDiffValue
              label="Element"
              before={formatId(typeDiff.beforeElementId)}
              after={formatId(typeDiff.afterElementId)}
              status={typeDiff.status}
            />
            <SchemaDiffValue
              label="Required"
              before={formatRequired(typeDiff.beforeIsElementRequired)}
              after={formatRequired(typeDiff.afterIsElementRequired)}
              status={typeDiff.status}
            />
            <SchemaDiffTypeView
              typeDiff={typeDiff.element}
              renderNestedFields={renderNestedFields}
            />
          </div>
        </div>
      );
    case "map":
      return (
        <div className="flex flex-col gap-2">
          {typeBadge("map", typeDiff.status)}
          <div className="ml-3 flex flex-col gap-4 border-l-2 border-edge py-1 pl-4">
            <div className="flex flex-col gap-2">
              <SchemaDiffValue
                label="Key"
                before={formatId(typeDiff.beforeKeyId)}
                after={formatId(typeDiff.afterKeyId)}
                status={typeDiff.status}
              />
              <SchemaDiffTypeView
                typeDiff={typeDiff.key}
                renderNestedFields={renderNestedFields}
              />
            </div>
            <div className="flex flex-col gap-2">
              <SchemaDiffValue
                label="Value"
                before={formatId(typeDiff.beforeValueId)}
                after={formatId(typeDiff.afterValueId)}
                status={typeDiff.status}
              />
              <SchemaDiffValue
                label="Required"
                before={formatRequired(typeDiff.beforeIsValueRequired)}
                after={formatRequired(typeDiff.afterIsValueRequired)}
                status={typeDiff.status}
              />
              <SchemaDiffTypeView
                typeDiff={typeDiff.value}
                renderNestedFields={renderNestedFields}
              />
            </div>
          </div>
        </div>
      );
  }
};

export default SchemaDiffTypeView;
