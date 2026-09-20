import SchemaFieldHeader from "./SchemaFieldHeader";
import SchemaDiffFieldList from "./SchemaDiffFieldList";
import SchemaDiffValue from "./SchemaDiffValue";
import { diffIcebergSchemas } from "../schemaDiff";
import { SPEC_TABLE_CLASS } from "../../specs/components/SpecFieldTable";

interface SchemaDiffViewProps {
  previousSchema: unknown;
  currentSchema: unknown;
}

const SchemaDiffView = ({
  previousSchema,
  currentSchema,
}: SchemaDiffViewProps) => {
  const schemaDiff = diffIcebergSchemas(previousSchema, currentSchema);

  if (
    schemaDiff.fields.length === 0 &&
    !schemaDiff.areIdentifierFieldsChanged
  ) {
    return (
      <p className="px-5 py-4 text-sm italic text-slate-400">
        No field changes.
      </p>
    );
  }

  return (
    <>
      {schemaDiff.areIdentifierFieldsChanged && (
        <div className="flex flex-wrap items-center gap-2 border-b border-edge px-5 py-4 text-xs">
          <span className="font-sans font-medium text-slate-300">
            Identifier field IDs
          </span>
          <SchemaDiffValue
            before={schemaDiff.beforeIdentifierFieldIds.join(", ") || "None"}
            after={schemaDiff.afterIdentifierFieldIds.join(", ") || "None"}
            status="changed"
          />
        </div>
      )}
      {schemaDiff.fields.length > 0 && (
        <div className="overflow-x-auto">
          <table className={SPEC_TABLE_CLASS}>
            <SchemaFieldHeader />
            <tbody className="font-mono">
              <SchemaDiffFieldList
                fields={schemaDiff.fields}
                isNested={false}
              />
            </tbody>
          </table>
        </div>
      )}
    </>
  );
};

export default SchemaDiffView;
