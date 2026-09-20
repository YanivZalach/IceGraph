import SchemaTypeView from "./SchemaTypeView";
import SchemaFieldHeader from "./SchemaFieldHeader";
import { parseIcebergSchema } from "../schemaModel";
import {
  SPEC_CELL_CLASS,
  SPEC_TABLE_CLASS,
} from "../../specs/components/SpecFieldTable";

interface SchemaFieldListProps {
  schema: unknown;
}

const requirednessText = (isRequired: boolean | null): string =>
  isRequired === null ? "unknown" : isRequired ? "required" : "optional";

const SchemaFieldList = ({ schema }: SchemaFieldListProps) => {
  const parsedSchema = parseIcebergSchema(schema);

  if (parsedSchema.fields.length === 0) {
    return (
      <p className="px-5 py-4 text-sm italic text-slate-400">
        No fields defined.
      </p>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className={SPEC_TABLE_CLASS}>
        <SchemaFieldHeader />
        <tbody className="font-mono">
          {parsedSchema.fields.map((field, fieldIndex) => (
            <tr
              key={`${field.id ?? "missing-id"}.${String(fieldIndex)}`}
              className="border-b border-edge last:border-0"
            >
              <td
                className={`${SPEC_CELL_CLASS} font-mono text-xs text-slate-400`}
              >
                {field.id ?? "?"}
              </td>
              <td className={SPEC_CELL_CLASS}>
                <code className="text-xs text-ink">{field.name}</code>
                {field.doc && (
                  <p className="mt-1 max-w-xs font-sans text-xs text-slate-400">
                    {field.doc}
                  </p>
                )}
              </td>
              <td className={SPEC_CELL_CLASS}>
                <SchemaTypeView type={field.type} />
              </td>
              <td className={`${SPEC_CELL_CLASS} font-mono text-xs text-ink`}>
                {requirednessText(field.isRequired)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

export default SchemaFieldList;
