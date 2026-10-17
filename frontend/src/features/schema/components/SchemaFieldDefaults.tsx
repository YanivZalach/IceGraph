import SchemaFieldDoc from "./SchemaFieldDoc";
import type { IcebergSchemaField } from "../schemaModel";

interface SchemaFieldDefaultsProps {
  field: IcebergSchemaField;
  className: string;
}

const SchemaFieldDefaults = ({
  field,
  className,
}: SchemaFieldDefaultsProps) => {
  const { initialDefault, writeDefault } = field;
  if (initialDefault === undefined && writeDefault === undefined) return null;

  if (initialDefault === writeDefault) {
    return (
      <SchemaFieldDoc className={className} label="default">
        {initialDefault}
      </SchemaFieldDoc>
    );
  }

  return (
    <>
      {initialDefault !== undefined && (
        <SchemaFieldDoc className={className} label="initial default">
          {initialDefault}
        </SchemaFieldDoc>
      )}
      {writeDefault !== undefined && (
        <SchemaFieldDoc className={className} label="write default">
          {writeDefault}
        </SchemaFieldDoc>
      )}
    </>
  );
};

export default SchemaFieldDefaults;
