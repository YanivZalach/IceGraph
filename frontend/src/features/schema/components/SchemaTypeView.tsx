import type { IcebergType } from "../schemaModel";
import { formatRequiredness, formatUnknownType } from "../schemaModel";
import SchemaCollectionMember from "./SchemaCollectionMember";
import SchemaTypeBadge from "./SchemaTypeBadge";
import SchemaFieldDoc from "./SchemaFieldDoc";
import { cn } from "../../../shared/lib/cn";

interface SchemaTypeViewProps {
  type: IcebergType;
  showIds?: boolean;
}

const SchemaTypeView = ({ type, showIds = true }: SchemaTypeViewProps) => {
  switch (type.kind) {
    case "primitive":
      return <SchemaTypeBadge kind="primitive">{type.name}</SchemaTypeBadge>;
    case "struct":
      return (
        <div className="flex flex-col gap-2">
          <SchemaTypeBadge kind="struct">struct</SchemaTypeBadge>
          <div className="ml-3 flex flex-col border-l-2 border-edge pl-4">
            {type.fields.map((field, fieldIndex) => (
              <div
                key={`${field.id ?? "missing-id"}.${String(fieldIndex)}`}
                className="flex flex-col gap-1.5 border-b border-edge py-3 last:border-0"
              >
                <div className="flex items-center gap-2">
                  {showIds && (
                    <span className="w-7 shrink-0 text-right font-mono text-sm text-slate-500">
                      {field.id ?? "?"}
                    </span>
                  )}
                  <span className="font-mono text-sm font-semibold text-ink">
                    {field.name}
                  </span>
                  <span className="font-mono text-xs text-slate-400">
                    {formatRequiredness(field.isRequired)}
                  </span>
                </div>
                <div className={cn(showIds && "ml-9")}>
                  <SchemaTypeView type={field.type} showIds={showIds} />
                </div>
                {field.doc && (
                  <SchemaFieldDoc className={cn(showIds && "ml-9")}>
                    {field.doc}
                  </SchemaFieldDoc>
                )}
              </div>
            ))}
          </div>
        </div>
      );
    case "list":
      return (
        <div className="flex flex-col gap-2">
          <SchemaTypeBadge kind="list">list</SchemaTypeBadge>
          <div className="ml-3 border-l-2 border-edge py-1 pl-4">
            <div className="mb-2">
              <SchemaCollectionMember
                id={showIds ? (type.elementId ?? "?") : undefined}
                name="element"
                requiredness={formatRequiredness(type.isElementRequired)}
              />
            </div>
            <SchemaTypeView type={type.element} showIds={showIds} />
          </div>
        </div>
      );
    case "map":
      return (
        <div className="flex flex-col gap-2">
          <SchemaTypeBadge kind="map">map</SchemaTypeBadge>
          <div className="ml-3 flex flex-col gap-4 border-l-2 border-edge py-1 pl-4">
            <div>
              <div className="mb-2">
                <SchemaCollectionMember
                  id={showIds ? (type.keyId ?? "?") : undefined}
                  name="key"
                  requiredness="required"
                />
              </div>
              <SchemaTypeView type={type.key} showIds={showIds} />
            </div>
            <div>
              <div className="mb-2">
                <SchemaCollectionMember
                  id={showIds ? (type.valueId ?? "?") : undefined}
                  name="value"
                  requiredness={formatRequiredness(type.isValueRequired)}
                />
              </div>
              <SchemaTypeView type={type.value} showIds={showIds} />
            </div>
          </div>
        </div>
      );
    case "unknown":
      return (
        <pre className="overflow-x-auto rounded border border-edge bg-canvas p-2 font-mono text-detail text-slate-300">
          {formatUnknownType(type.value)}
        </pre>
      );
  }
};

export default SchemaTypeView;
