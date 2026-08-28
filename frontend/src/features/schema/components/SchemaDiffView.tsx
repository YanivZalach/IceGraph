import SchemaFieldHeader from "./SchemaFieldHeader";
import SchemaDiffFieldList from "./SchemaDiffFieldList";
import { diffIcebergSchemas } from "../schemaDiff";

interface SchemaDiffViewProps {
  previousSchema: unknown;
  currentSchema: unknown;
}

const SchemaDiffView = ({
  previousSchema,
  currentSchema,
}: SchemaDiffViewProps) => {
  const schemaDiff = diffIcebergSchemas(previousSchema, currentSchema);

  if (schemaDiff.fields.length === 0) {
    return <p className="text-sm italic text-slate-400">No field changes.</p>;
  }

  return (
    <div>
      <SchemaFieldHeader />
      <SchemaDiffFieldList fields={schemaDiff.fields} isNested={false} />
    </div>
  );
};

export default SchemaDiffView;
