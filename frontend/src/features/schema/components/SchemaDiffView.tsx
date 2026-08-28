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

  return (
    <div>
      <SchemaFieldHeader />
      <SchemaDiffFieldList fields={schemaDiff.fields} isNested={false} />
    </div>
  );
};

export default SchemaDiffView;
