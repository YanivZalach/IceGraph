import type { ReactNode } from "react";
import type { SchemaFieldDiff } from "../schemaDiff";
import SchemaDiffFieldRow from "./SchemaDiffFieldRow";

interface SchemaDiffFieldListProps {
  fields: SchemaFieldDiff[];
  isNested: boolean;
}

const SchemaDiffFieldList = ({
  fields,
  isNested,
}: SchemaDiffFieldListProps) => {
  const renderNestedFields = (nestedFields: SchemaFieldDiff[]): ReactNode => (
    <SchemaDiffFieldList fields={nestedFields} isNested />
  );

  return (
    <div className={isNested ? "ml-3 border-l-2 border-edge pl-4" : ""}>
      {fields.map((field, fieldIndex) => (
        <SchemaDiffFieldRow
          key={`${field.after?.id ?? field.before?.id ?? "missing-id"}.${String(fieldIndex)}`}
          fieldDiff={field}
          renderNestedFields={renderNestedFields}
        />
      ))}
    </div>
  );
};

export default SchemaDiffFieldList;
