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

  const rows = fields.map((field, fieldIndex) => (
    <SchemaDiffFieldRow
      key={`${field.after?.id ?? field.before?.id ?? "missing-id"}.${String(fieldIndex)}`}
      fieldDiff={field}
      isNested={isNested}
      renderNestedFields={renderNestedFields}
    />
  ));

  return isNested ? (
    <div className="ml-3 border-l-2 border-edge pl-4">{rows}</div>
  ) : (
    <>{rows}</>
  );
};

export default SchemaDiffFieldList;
