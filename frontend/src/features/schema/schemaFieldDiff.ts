import { areUnknownValuesEqual, nestedStatus } from "./schemaDiffComparison";
import type { SchemaDiffContext } from "./schemaFieldIndex";
import type {
  ListTypeDiff,
  MapTypeDiff,
  SchemaFieldDiff,
  SchemaTypeDiff,
  StructTypeDiff,
} from "./schemaDiffTypes";
import type {
  IcebergSchemaField,
  IcebergType,
  ListType,
  MapType,
  StructType,
} from "./schemaModel";

const createPresentFieldDiff = (
  field: IcebergSchemaField,
  status: "added" | "removed",
): SchemaFieldDiff => ({
  status,
  movement: null,
  before: status === "removed" ? field : null,
  after: status === "added" ? field : null,
  isNameChanged: false,
  isRequiredChanged: false,
  type: createPresentTypeDiff(field.type, status),
});

const createPresentTypeDiff = (
  type: IcebergType,
  status: "added" | "removed",
): SchemaTypeDiff => {
  switch (type.kind) {
    case "primitive":
      return {
        kind: "primitive",
        status,
        before: status === "removed" ? type.name : null,
        after: status === "added" ? type.name : null,
      };
    case "struct":
      return {
        kind: "struct",
        status,
        fields: type.fields.map((field) =>
          createPresentFieldDiff(field, status),
        ),
      };
    case "list":
      return {
        kind: "list",
        status,
        beforeElementId: status === "removed" ? type.elementId : null,
        afterElementId: status === "added" ? type.elementId : null,
        beforeIsElementRequired:
          status === "removed" ? type.isElementRequired : null,
        afterIsElementRequired:
          status === "added" ? type.isElementRequired : null,
        element: createPresentTypeDiff(type.element, status),
      };
    case "map":
      return {
        kind: "map",
        status,
        beforeKeyId: status === "removed" ? type.keyId : null,
        afterKeyId: status === "added" ? type.keyId : null,
        key: createPresentTypeDiff(type.key, status),
        beforeValueId: status === "removed" ? type.valueId : null,
        afterValueId: status === "added" ? type.valueId : null,
        beforeIsValueRequired:
          status === "removed" ? type.isValueRequired : null,
        afterIsValueRequired: status === "added" ? type.isValueRequired : null,
        value: createPresentTypeDiff(type.value, status),
      };
    case "unknown":
      return {
        kind: "unknown",
        status,
        before: status === "removed" ? type.value : null,
        after: status === "added" ? type.value : null,
      };
  }
};

const diffStructTypes = (
  before: StructType,
  after: StructType,
  context: SchemaDiffContext,
): StructTypeDiff => {
  const fields = diffSchemaFields(before.fields, after.fields, context);
  return {
    kind: "struct",
    status: nestedStatus(
      false,
      fields.map((field) => field.status),
    ),
    fields,
  };
};

const diffListTypes = (
  before: ListType,
  after: ListType,
  context: SchemaDiffContext,
): ListTypeDiff => {
  const element = diffSchemaTypes(before.element, after.element, context);
  const hasDirectChange =
    before.elementId !== after.elementId ||
    before.isElementRequired !== after.isElementRequired;

  return {
    kind: "list",
    status: nestedStatus(hasDirectChange, [element.status]),
    beforeElementId: before.elementId,
    afterElementId: after.elementId,
    beforeIsElementRequired: before.isElementRequired,
    afterIsElementRequired: after.isElementRequired,
    element,
  };
};

const diffMapTypes = (
  before: MapType,
  after: MapType,
  context: SchemaDiffContext,
): MapTypeDiff => {
  const key = diffSchemaTypes(before.key, after.key, context);
  const value = diffSchemaTypes(before.value, after.value, context);
  const hasDirectChange =
    before.keyId !== after.keyId ||
    before.valueId !== after.valueId ||
    before.isValueRequired !== after.isValueRequired;

  return {
    kind: "map",
    status: nestedStatus(hasDirectChange, [key.status, value.status]),
    beforeKeyId: before.keyId,
    afterKeyId: after.keyId,
    key,
    beforeValueId: before.valueId,
    afterValueId: after.valueId,
    beforeIsValueRequired: before.isValueRequired,
    afterIsValueRequired: after.isValueRequired,
    value,
  };
};

const diffSchemaTypes = (
  before: IcebergType,
  after: IcebergType,
  context: SchemaDiffContext,
): SchemaTypeDiff => {
  if (before.kind !== after.kind) {
    return { kind: "replacement", status: "changed", before, after };
  }

  switch (before.kind) {
    case "primitive": {
      const afterName = after.kind === "primitive" ? after.name : null;
      return {
        kind: "primitive",
        status: before.name === afterName ? "unchanged" : "changed",
        before: before.name,
        after: afterName,
      };
    }
    case "struct":
      return after.kind === "struct"
        ? diffStructTypes(before, after, context)
        : { kind: "replacement", status: "changed", before, after };
    case "list":
      return after.kind === "list"
        ? diffListTypes(before, after, context)
        : { kind: "replacement", status: "changed", before, after };
    case "map":
      return after.kind === "map"
        ? diffMapTypes(before, after, context)
        : { kind: "replacement", status: "changed", before, after };
    case "unknown": {
      const afterValue = after.kind === "unknown" ? after.value : null;
      return {
        kind: "unknown",
        status: areUnknownValuesEqual(before.value, afterValue)
          ? "unchanged"
          : "changed",
        before: before.value,
        after: afterValue,
      };
    }
  }
};

const diffMatchedFields = (
  before: IcebergSchemaField,
  after: IcebergSchemaField,
  context: SchemaDiffContext,
): SchemaFieldDiff => {
  const type = diffSchemaTypes(before.type, after.type, context);
  const isNameChanged = before.name !== after.name;
  const isRequiredChanged = before.isRequired !== after.isRequired;

  return {
    status: nestedStatus(isNameChanged || isRequiredChanged, [type.status]),
    movement: null,
    before,
    after,
    isNameChanged,
    isRequiredChanged,
    type,
  };
};

const createMovedFieldDiff = (
  before: IcebergSchemaField,
  after: IcebergSchemaField,
  movement: "from" | "to",
  context: SchemaDiffContext,
): SchemaFieldDiff => ({
  ...diffMatchedFields(before, after, context),
  status: "moved",
  movement,
});

export const diffSchemaFields = (
  beforeFields: IcebergSchemaField[],
  afterFields: IcebergSchemaField[],
  context: SchemaDiffContext,
): SchemaFieldDiff[] => {
  const matchedBeforeIndexes = new Set<number>();
  const fieldDiffs = afterFields.map((afterField) => {
    const canMatch =
      afterField.id !== null && context.matchableFieldIds.has(afterField.id);
    const beforeIndex = canMatch
      ? beforeFields.findIndex(
          (beforeField) => beforeField.id === afterField.id,
        )
      : -1;

    if (beforeIndex < 0) {
      const beforeField =
        afterField.id === null
          ? undefined
          : context.beforeFieldsById.get(afterField.id);

      if (
        afterField.id !== null &&
        context.movedFieldIds.has(afterField.id) &&
        beforeField !== undefined
      ) {
        return createMovedFieldDiff(beforeField, afterField, "to", context);
      }

      return createPresentFieldDiff(afterField, "added");
    }

    const beforeField = beforeFields[beforeIndex];
    matchedBeforeIndexes.add(beforeIndex);

    return beforeField === undefined
      ? createPresentFieldDiff(afterField, "added")
      : diffMatchedFields(beforeField, afterField, context);
  });

  const allFieldDiffs = [
    ...fieldDiffs,
    ...beforeFields.flatMap((field, fieldIndex) => {
      if (matchedBeforeIndexes.has(fieldIndex)) {
        return [];
      }

      const afterField =
        field.id === null ? undefined : context.afterFieldsById.get(field.id);

      return field.id !== null &&
        context.movedFieldIds.has(field.id) &&
        afterField !== undefined
        ? [createMovedFieldDiff(field, afterField, "from", context)]
        : [createPresentFieldDiff(field, "removed")];
    }),
  ];

  return allFieldDiffs.filter((field) => field.status !== "unchanged");
};
