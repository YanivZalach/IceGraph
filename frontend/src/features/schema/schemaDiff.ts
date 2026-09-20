import { createSchemaDiffContext } from "./schemaFieldIndex";
import { diffSchemaFields } from "./schemaFieldDiff";
import type { SchemaDiff } from "./schemaDiffTypes";
import { parseIcebergSchema } from "./schemaModel";

export type {
  ListTypeDiff,
  MapTypeDiff,
  PrimitiveTypeDiff,
  ReplacementTypeDiff,
  SchemaDiff,
  SchemaDiffStatus,
  SchemaFieldDiff,
  SchemaTypeDiff,
  StructTypeDiff,
  UnknownTypeDiff,
} from "./schemaDiffTypes";

export const diffIcebergSchemas = (
  before: unknown,
  after: unknown,
): SchemaDiff => {
  const beforeSchema = parseIcebergSchema(before);
  const afterSchema = parseIcebergSchema(after);
  const context = createSchemaDiffContext(beforeSchema, afterSchema);
  const afterIdentifierFieldIds = new Set(afterSchema.identifierFieldIds);
  const areIdentifierFieldsChanged =
    beforeSchema.identifierFieldIds.length !==
      afterSchema.identifierFieldIds.length ||
    beforeSchema.identifierFieldIds.some(
      (id) => !afterIdentifierFieldIds.has(id),
    );

  return {
    fields: diffSchemaFields(beforeSchema.fields, afterSchema.fields, context),
    beforeIdentifierFieldIds: beforeSchema.identifierFieldIds,
    afterIdentifierFieldIds: afterSchema.identifierFieldIds,
    areIdentifierFieldsChanged,
  };
};
