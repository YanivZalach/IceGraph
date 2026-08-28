import type { IcebergType } from "../schemaModel";
import { formatUnknownType } from "../schemaModel";

interface SchemaTypeViewProps {
  type: IcebergType;
}

const formatMemberLabel = (
  label: string,
  id: string | null,
  isRequired: boolean | null,
): string => {
  const idLabel = id === null ? "ID unknown" : `ID ${id}`;
  const requirementLabel =
    isRequired === false
      ? "optional"
      : isRequired === null
        ? "requiredness unknown"
        : "required";
  return `${label}, ${idLabel}, ${requirementLabel}`;
};

const SchemaTypeView = ({ type }: SchemaTypeViewProps) => {
  switch (type.kind) {
    case "primitive":
      return (
        <span className="w-fit rounded bg-accent-muted px-2 py-0.5 font-mono text-xs text-accent">
          {type.name}
        </span>
      );
    case "struct":
      return (
        <div className="flex flex-col gap-2">
          <span className="w-fit rounded bg-violet-900/30 px-2 py-0.5 font-mono text-xs text-violet-400">
            struct
          </span>
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
                  <span className="text-sm font-semibold text-ink">
                    {field.name}
                  </span>
                  {field.isRequired === false && (
                    <span className="text-xs font-bold uppercase text-slate-600">
                      optional
                    </span>
                  )}
                </div>
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
          <span className="w-fit rounded bg-amber-900/40 px-2 py-0.5 font-mono text-xs text-amber-400">
            list
          </span>
          <div className="ml-3 border-l-2 border-edge py-1 pl-4">
            <div className="mb-2 text-xs font-bold uppercase text-slate-500">
              {formatMemberLabel(
                "Element",
                type.elementId,
                type.isElementRequired,
              )}
            </div>
            <SchemaTypeView type={type.element} />
          </div>
        </div>
      );
    case "map":
      return (
        <div className="flex flex-col gap-2">
          <span className="w-fit rounded bg-emerald-900/40 px-2 py-0.5 font-mono text-xs text-emerald-400">
            map
          </span>
          <div className="ml-3 flex flex-col gap-4 border-l-2 border-edge py-1 pl-4">
            <div>
              <div className="mb-2 text-xs font-bold uppercase text-slate-500">
                Key, {type.keyId === null ? "ID unknown" : `ID ${type.keyId}`}
              </div>
              <SchemaTypeView type={type.key} />
            </div>
            <div>
              <div className="mb-2 text-xs font-bold uppercase text-slate-500">
                {formatMemberLabel("Value", type.valueId, type.isValueRequired)}
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
