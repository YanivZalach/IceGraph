const isUnknownRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const parseSchemaId = (schema: unknown): number | null => {
  if (!isUnknownRecord(schema)) {
    return null;
  }

  const schemaId = schema["schema-id"];
  const numericSchemaId =
    typeof schemaId === "string" && schemaId.trim() !== ""
      ? Number(schemaId)
      : schemaId;

  return typeof numericSchemaId === "number" &&
    Number.isSafeInteger(numericSchemaId) &&
    numericSchemaId >= 0
    ? numericSchemaId
    : null;
};

export const findPreviousSchema = (
  schemas: unknown[],
  selectedSchema: unknown,
): Record<string, unknown> | null => {
  const selectedSchemaId = parseSchemaId(selectedSchema);

  if (selectedSchemaId === null) {
    return null;
  }

  let previousSchema: Record<string, unknown> | null = null;
  let previousSchemaId = -1;

  schemas.forEach((schema) => {
    const schemaId = parseSchemaId(schema);

    if (
      isUnknownRecord(schema) &&
      schemaId !== null &&
      schemaId < selectedSchemaId &&
      schemaId > previousSchemaId
    ) {
      previousSchema = schema;
      previousSchemaId = schemaId;
    }
  });

  return previousSchema;
};
