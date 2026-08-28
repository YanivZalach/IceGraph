import SchemaFieldHeader from "./SchemaFieldHeader";
import SchemaTypeView from "./SchemaTypeView";
import { parseIcebergSchema } from "../schemaModel";

interface SchemaFieldListProps {
  schema: unknown;
}

const SchemaFieldList = ({ schema }: SchemaFieldListProps) => {
  const parsedSchema = parseIcebergSchema(schema);

  if (parsedSchema.fields.length === 0) {
    return <p className="text-sm italic text-slate-400">No fields defined.</p>;
  }

  return (
    <div>
      <SchemaFieldHeader />
      {parsedSchema.fields.map((field, fieldIndex) => (
        <div
          key={`${field.identity}.${String(fieldIndex)}`}
          className="border-b border-edge py-4 last:border-0"
        >
          <div className="grid grid-cols-[1rem_2.5rem_minmax(0,1fr)_auto] items-center gap-x-3">
            <span />
            <span className="text-right font-mono text-base tabular-nums text-slate-500">
              {field.id ?? "?"}
            </span>
            <span className="min-w-0 text-sm font-bold text-ink-bright">
              {field.name}
            </span>
            {field.isRequired === false ? (
              <span className="shrink-0 text-xs font-bold uppercase text-slate-400">
                optional
              </span>
            ) : (
              <span />
            )}
          </div>
          <div className="ml-16 mt-3">
            <SchemaTypeView type={field.type} />
          </div>
        </div>
      ))}
    </div>
  );
};

export default SchemaFieldList;
