import type {
  IcebergSchema,
  IcebergSchemaField,
  IcebergType,
} from "./schemaModel";

export interface SchemaDiffContext {
  matchableFieldIds: ReadonlySet<string>;
  movedFieldIds: ReadonlySet<string>;
  beforeFieldsById: ReadonlyMap<string, IcebergSchemaField>;
  afterFieldsById: ReadonlyMap<string, IcebergSchemaField>;
}

interface SchemaFieldIndex {
  fieldsById: ReadonlyMap<string, IcebergSchemaField>;
  idCounts: ReadonlyMap<string, number>;
  locationsById: ReadonlyMap<string, string>;
}

const indexSchemaFields = (
  fields: IcebergSchemaField[],
  parentId: string | null,
  containerPath: string[],
  fieldsById: Map<string, IcebergSchemaField>,
  idCounts: Map<string, number>,
  locationsById: Map<string, string>,
): void => {
  const indexNestedType = (
    type: IcebergType,
    nestedParentId: string | null,
    nestedContainerPath: string[],
  ): void => {
    switch (type.kind) {
      case "struct":
        indexSchemaFields(
          type.fields,
          nestedParentId,
          [...nestedContainerPath, "struct"],
          fieldsById,
          idCounts,
          locationsById,
        );
        break;
      case "list":
        indexNestedType(type.element, nestedParentId, [
          ...nestedContainerPath,
          "list.element",
        ]);
        break;
      case "map":
        indexNestedType(type.key, nestedParentId, [
          ...nestedContainerPath,
          "map.key",
        ]);
        indexNestedType(type.value, nestedParentId, [
          ...nestedContainerPath,
          "map.value",
        ]);
        break;
      case "primitive":
      case "unknown":
        break;
    }
  };

  fields.forEach((field) => {
    if (field.id !== null) {
      fieldsById.set(field.id, field);
      idCounts.set(field.id, (idCounts.get(field.id) ?? 0) + 1);
      locationsById.set(field.id, JSON.stringify([parentId, ...containerPath]));
    }

    indexNestedType(field.type, field.id, []);
  });
};

const createSchemaFieldIndex = (schema: IcebergSchema): SchemaFieldIndex => {
  const fieldsById = new Map<string, IcebergSchemaField>();
  const idCounts = new Map<string, number>();
  const locationsById = new Map<string, string>();
  indexSchemaFields(
    schema.fields,
    null,
    ["root"],
    fieldsById,
    idCounts,
    locationsById,
  );

  return { fieldsById, idCounts, locationsById };
};

export const createSchemaDiffContext = (
  beforeSchema: IcebergSchema,
  afterSchema: IcebergSchema,
): SchemaDiffContext => {
  const beforeIndex = createSchemaFieldIndex(beforeSchema);
  const afterIndex = createSchemaFieldIndex(afterSchema);
  const matchableFieldIds = new Set(
    [...afterIndex.idCounts.keys()].filter(
      (id) =>
        beforeIndex.idCounts.get(id) === 1 && afterIndex.idCounts.get(id) === 1,
    ),
  );
  const movedFieldIds = new Set(
    [...matchableFieldIds].filter(
      (id) =>
        beforeIndex.locationsById.get(id) !== afterIndex.locationsById.get(id),
    ),
  );

  return {
    matchableFieldIds,
    movedFieldIds,
    beforeFieldsById: beforeIndex.fieldsById,
    afterFieldsById: afterIndex.fieldsById,
  };
};
