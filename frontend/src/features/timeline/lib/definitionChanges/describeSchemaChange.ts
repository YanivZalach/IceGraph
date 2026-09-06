import type {
  IcebergStructField,
  IcebergType,
} from "../../api/icebergTypeSchemas";
import type { TableMetadata } from "../../api/tableMetadataSchema";
import {
  formatId,
  sameTextChange,
  type DescribedChange,
} from "./describedChange";

const formatTypeName = (type: IcebergType): string =>
  typeof type === "string" ? type : type.type;

const isSameType = (previousType: IcebergType, currentType: IcebergType) =>
  JSON.stringify(previousType) === JSON.stringify(currentType);

const describeColumnAttributes = (field: IcebergStructField): string =>
  `(${formatTypeName(field.type)}, ${field.required ? "required" : "optional"})`;

const findSchema = (tableMetadata: TableMetadata, schemaId: number | null) =>
  schemaId === null
    ? undefined
    : tableMetadata.schemas?.find((schema) => schema["schema-id"] === schemaId);

const diffSchemaFields = (
  previousFields: IcebergStructField[],
  currentFields: IcebergStructField[],
): DescribedChange[] => {
  const previousFieldsById = new Map(
    previousFields.map((field) => [field.id, field]),
  );
  const currentFieldIds = new Set(currentFields.map((field) => field.id));

  const changes: DescribedChange[] = [];

  for (const field of currentFields) {
    const previousField = previousFieldsById.get(field.id);

    if (previousField === undefined) {
      changes.push({
        impact: `added column ${field.name}`,
        detail: `added column ${field.name} ${describeColumnAttributes(field)}`,
      });
      continue;
    }

    if (previousField.name !== field.name) {
      changes.push(
        sameTextChange(`renamed column ${previousField.name} → ${field.name}`),
      );
    }

    if (!isSameType(previousField.type, field.type)) {
      changes.push({
        impact: `column ${field.name} type changed`,
        detail: `column ${field.name} type changed (${formatTypeName(previousField.type)} → ${formatTypeName(field.type)})`,
      });
    }

    if (previousField.required !== field.required) {
      changes.push(
        sameTextChange(
          `column ${field.name} now ${field.required ? "required" : "optional"}`,
        ),
      );
    }
  }

  for (const field of previousFields) {
    if (!currentFieldIds.has(field.id)) {
      changes.push({
        impact: `removed column ${field.name}`,
        detail: `removed column ${field.name} ${describeColumnAttributes(field)}`,
      });
    }
  }

  return changes;
};

export const describeSchemaChange = (
  previousSchemaId: number | null,
  currentSchemaId: number | null,
  tableMetadata: TableMetadata,
): DescribedChange[] => {
  if (previousSchemaId === currentSchemaId) {
    return [];
  }

  const previousSchema = findSchema(tableMetadata, previousSchemaId);
  const currentSchema = findSchema(tableMetadata, currentSchemaId);

  if (previousSchema === undefined || currentSchema === undefined) {
    return [
      sameTextChange(
        `schema changed (v${formatId(previousSchemaId)} → v${formatId(currentSchemaId)})`,
      ),
    ];
  }

  return diffSchemaFields(previousSchema.fields, currentSchema.fields);
};
