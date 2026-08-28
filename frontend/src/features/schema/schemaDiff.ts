import type {
  IcebergSchema,
  IcebergSchemaField,
  IcebergType,
  ListType,
  MapType,
  StructType,
} from "./schemaModel";
import { parseIcebergSchema } from "./schemaModel";

export type SchemaDiffStatus =
  "unchanged" | "added" | "removed" | "changed" | "descendant-changed";

interface TypeDiffBase {
  status: SchemaDiffStatus;
}

export interface PrimitiveTypeDiff extends TypeDiffBase {
  kind: "primitive";
  before: string | null;
  after: string | null;
}

export interface StructTypeDiff extends TypeDiffBase {
  kind: "struct";
  fields: SchemaFieldDiff[];
}

export interface ListTypeDiff extends TypeDiffBase {
  kind: "list";
  beforeElementId: string | null;
  afterElementId: string | null;
  beforeIsElementRequired: boolean | null;
  afterIsElementRequired: boolean | null;
  element: SchemaTypeDiff;
}

export interface MapTypeDiff extends TypeDiffBase {
  kind: "map";
  beforeKeyId: string | null;
  afterKeyId: string | null;
  key: SchemaTypeDiff;
  beforeValueId: string | null;
  afterValueId: string | null;
  beforeIsValueRequired: boolean | null;
  afterIsValueRequired: boolean | null;
  value: SchemaTypeDiff;
}

export interface UnknownTypeDiff extends TypeDiffBase {
  kind: "unknown";
  before: unknown;
  after: unknown;
}

export interface ReplacementTypeDiff extends TypeDiffBase {
  kind: "replacement";
  before: IcebergType;
  after: IcebergType;
}

export type SchemaTypeDiff =
  | PrimitiveTypeDiff
  | StructTypeDiff
  | ListTypeDiff
  | MapTypeDiff
  | UnknownTypeDiff
  | ReplacementTypeDiff;

export interface SchemaFieldDiff {
  status: SchemaDiffStatus;
  identity: string;
  before: IcebergSchemaField | null;
  after: IcebergSchemaField | null;
  isNameChanged: boolean;
  isRequiredChanged: boolean;
  type: SchemaTypeDiff;
}

export interface SchemaDiff {
  fields: SchemaFieldDiff[];
}

const hasChangedStatus = (status: SchemaDiffStatus): boolean =>
  status !== "unchanged";

const nestedStatus = (
  hasDirectChange: boolean,
  childStatuses: SchemaDiffStatus[],
): SchemaDiffStatus => {
  if (hasDirectChange) {
    return "changed";
  }

  return childStatuses.some(hasChangedStatus)
    ? "descendant-changed"
    : "unchanged";
};

const isUnknownRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const areUnknownValuesEqual = (before: unknown, after: unknown): boolean => {
  if (Object.is(before, after)) {
    return true;
  }

  if (Array.isArray(before) && Array.isArray(after)) {
    return (
      before.length === after.length &&
      before.every((value, index) => areUnknownValuesEqual(value, after[index]))
    );
  }

  if (isUnknownRecord(before) && isUnknownRecord(after)) {
    const beforeEntries = Object.entries(before);
    const afterEntries = Object.entries(after);

    return (
      beforeEntries.length === afterEntries.length &&
      beforeEntries.every(([key, value]) =>
        areUnknownValuesEqual(value, after[key]),
      )
    );
  }

  return false;
};

const createPresentFieldDiff = (
  field: IcebergSchemaField,
  status: "added" | "removed",
): SchemaFieldDiff => ({
  status,
  identity: field.identity,
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
): StructTypeDiff => {
  const fields = diffSchemaFields(before.fields, after.fields);
  return {
    kind: "struct",
    status: nestedStatus(
      false,
      fields.map((field) => field.status),
    ),
    fields,
  };
};

const diffListTypes = (before: ListType, after: ListType): ListTypeDiff => {
  const element = diffSchemaTypes(before.element, after.element);
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

const diffMapTypes = (before: MapType, after: MapType): MapTypeDiff => {
  const key = diffSchemaTypes(before.key, after.key);
  const value = diffSchemaTypes(before.value, after.value);
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
        ? diffStructTypes(before, after)
        : { kind: "replacement", status: "changed", before, after };
    case "list":
      return after.kind === "list"
        ? diffListTypes(before, after)
        : { kind: "replacement", status: "changed", before, after };
    case "map":
      return after.kind === "map"
        ? diffMapTypes(before, after)
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
): SchemaFieldDiff => {
  const type = diffSchemaTypes(before.type, after.type);
  const isNameChanged = before.name !== after.name;
  const isRequiredChanged = before.isRequired !== after.isRequired;

  return {
    status: nestedStatus(isNameChanged || isRequiredChanged, [type.status]),
    identity: after.identity,
    before,
    after,
    isNameChanged,
    isRequiredChanged,
    type,
  };
};

const diffSchemaFields = (
  beforeFields: IcebergSchemaField[],
  afterFields: IcebergSchemaField[],
): SchemaFieldDiff[] => {
  const unmatchedBeforeFields = [...beforeFields];
  const fieldDiffs = afterFields.map((afterField) => {
    const beforeIndex = unmatchedBeforeFields.findIndex(
      (beforeField) => beforeField.identity === afterField.identity,
    );

    if (beforeIndex < 0) {
      return createPresentFieldDiff(afterField, "added");
    }

    const beforeField = unmatchedBeforeFields[beforeIndex];
    unmatchedBeforeFields.splice(beforeIndex, 1);

    return beforeField === undefined
      ? createPresentFieldDiff(afterField, "added")
      : diffMatchedFields(beforeField, afterField);
  });

  const allFieldDiffs = [
    ...fieldDiffs,
    ...unmatchedBeforeFields.map((field) =>
      createPresentFieldDiff(field, "removed"),
    ),
  ];

  return allFieldDiffs.filter((field) => field.status !== "unchanged");
};

export const diffIcebergSchemas = (
  before: unknown,
  after: unknown,
): SchemaDiff => {
  const beforeSchema: IcebergSchema = parseIcebergSchema(before);
  const afterSchema: IcebergSchema = parseIcebergSchema(after);

  return { fields: diffSchemaFields(beforeSchema.fields, afterSchema.fields) };
};
