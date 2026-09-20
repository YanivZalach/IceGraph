import type { ReactNode } from "react";
import { cn } from "../../../shared/lib/cn";
import type {
  SchemaFieldDiff,
  SchemaTypeDiff,
  SchemaDiffStatus,
} from "../schemaDiff";
import { formatUnknownType } from "../schemaModel";
import SchemaCollectionMember from "./SchemaCollectionMember";
import SchemaDiffValue from "./SchemaDiffValue";
import SchemaTypeView from "./SchemaTypeView";
import SchemaTypeBadge from "./SchemaTypeBadge";

interface SchemaDiffTypeViewProps {
  typeDiff: SchemaTypeDiff;
  renderNestedFields: (fields: SchemaFieldDiff[]) => ReactNode;
}

const formatId = (id: string | null): string => id ?? "?";

const formatRequired = (isRequired: boolean | null): string =>
  isRequired === null ? "unknown" : isRequired ? "required" : "optional";

const badgeTone = (status: SchemaDiffStatus): string | undefined => {
  if (status === "added") return "text-green-400";
  if (status === "removed") return "text-red-400 line-through";
  return undefined;
};

const primitiveDiff = (
  before: string | null,
  after: string | null,
  status: SchemaDiffStatus,
): ReactNode => {
  const beforeValue = before ?? "unknown";
  const afterValue = after ?? "unknown";
  if (status === "added") {
    return (
      <SchemaTypeBadge kind="primitive" className="text-green-400">
        {afterValue}
      </SchemaTypeBadge>
    );
  }
  if (status === "removed") {
    return (
      <SchemaTypeBadge kind="primitive" className="text-red-400 line-through">
        {beforeValue}
      </SchemaTypeBadge>
    );
  }
  if (beforeValue !== afterValue) {
    return (
      <span className="flex flex-wrap items-center gap-2">
        <SchemaTypeBadge kind="primitive" className="text-red-400 line-through">
          {beforeValue}
        </SchemaTypeBadge>
        <span className="text-slate-500">→</span>
        <SchemaTypeBadge kind="primitive" className="text-green-400">
          {afterValue}
        </SchemaTypeBadge>
      </span>
    );
  }
  return <SchemaTypeBadge kind="primitive">{afterValue}</SchemaTypeBadge>;
};

const SchemaDiffTypeView = ({
  typeDiff,
  renderNestedFields,
}: SchemaDiffTypeViewProps) => {
  switch (typeDiff.kind) {
    case "primitive":
      return primitiveDiff(typeDiff.before, typeDiff.after, typeDiff.status);
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
          <SchemaTypeBadge kind="struct" className={badgeTone(typeDiff.status)}>
            struct
          </SchemaTypeBadge>
          {renderNestedFields(typeDiff.fields)}
        </div>
      );
    case "list":
      return (
        <div className="flex flex-col gap-2">
          <SchemaTypeBadge kind="list" className={badgeTone(typeDiff.status)}>
            list
          </SchemaTypeBadge>
          <div className="ml-3 flex flex-col gap-2 border-l-2 border-edge py-1 pl-4">
            <SchemaCollectionMember
              id={
                <SchemaDiffValue
                  before={formatId(typeDiff.beforeElementId)}
                  after={formatId(typeDiff.afterElementId)}
                  status={typeDiff.status}
                />
              }
              name="element"
              requiredness={
                <SchemaDiffValue
                  before={formatRequired(typeDiff.beforeIsElementRequired)}
                  after={formatRequired(typeDiff.afterIsElementRequired)}
                  status={typeDiff.status}
                />
              }
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
          <SchemaTypeBadge kind="map" className={badgeTone(typeDiff.status)}>
            map
          </SchemaTypeBadge>
          <div className="ml-3 flex flex-col gap-4 border-l-2 border-edge py-1 pl-4">
            <div className="flex flex-col gap-2">
              <SchemaCollectionMember
                id={
                  <SchemaDiffValue
                    before={formatId(typeDiff.beforeKeyId)}
                    after={formatId(typeDiff.afterKeyId)}
                    status={typeDiff.status}
                  />
                }
                name="key"
              />
              <SchemaDiffTypeView
                typeDiff={typeDiff.key}
                renderNestedFields={renderNestedFields}
              />
            </div>
            <div className="flex flex-col gap-2">
              <SchemaCollectionMember
                id={
                  <SchemaDiffValue
                    before={formatId(typeDiff.beforeValueId)}
                    after={formatId(typeDiff.afterValueId)}
                    status={typeDiff.status}
                  />
                }
                name="value"
                requiredness={
                  <SchemaDiffValue
                    before={formatRequired(typeDiff.beforeIsValueRequired)}
                    after={formatRequired(typeDiff.afterIsValueRequired)}
                    status={typeDiff.status}
                  />
                }
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
