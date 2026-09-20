import type { IcebergType } from "../schemaModel";
import { formatUnknownType } from "../schemaModel";
import SchemaCollectionMember from "./SchemaCollectionMember";
import SchemaTypeBadge from "./SchemaTypeBadge";

interface SchemaTypeViewProps {
  type: IcebergType;
}

const formatRequired = (isRequired: boolean | null): string =>
  isRequired === null ? "unknown" : isRequired ? "required" : "optional";

const SchemaTypeView = ({ type }: SchemaTypeViewProps) => {
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
                  <span className="w-7 shrink-0 text-right font-mono text-sm text-slate-500">
                    {field.id ?? "?"}
                  </span>
                  <span className="font-mono text-sm font-semibold text-ink">
                    {field.name}
                  </span>
                  <span className="font-mono text-xs text-slate-400">
                    {field.isRequired === null
                      ? "unknown"
                      : field.isRequired
                        ? "required"
                        : "optional"}
                  </span>
                </div>
                {field.doc && (
                  <p className="ml-9 font-sans text-xs text-slate-400">
                    {field.doc}
                  </p>
                )}
                <div className="ml-9">
                  <SchemaTypeView type={field.type} />
                </div>
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
                id={type.elementId ?? "?"}
                name="element"
                requiredness={formatRequired(type.isElementRequired)}
              />
            </div>
            <SchemaTypeView type={type.element} />
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
                <SchemaCollectionMember id={type.keyId ?? "?"} name="key" />
              </div>
              <SchemaTypeView type={type.key} />
            </div>
            <div>
              <div className="mb-2">
                <SchemaCollectionMember
                  id={type.valueId ?? "?"}
                  name="value"
                  requiredness={formatRequired(type.isValueRequired)}
                />
              </div>
              <SchemaTypeView type={type.value} />
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
