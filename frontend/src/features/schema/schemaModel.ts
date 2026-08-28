export interface IcebergSchema {
  fields: IcebergSchemaField[];
}

export interface IcebergSchemaField {
  id: string | null;
  identity: string;
  name: string;
  isRequired: boolean | null;
  type: IcebergType;
}

export interface PrimitiveType {
  kind: "primitive";
  name: string;
}

export interface StructType {
  kind: "struct";
  fields: IcebergSchemaField[];
}

export interface ListType {
  kind: "list";
  elementId: string | null;
  isElementRequired: boolean | null;
  element: IcebergType;
}

export interface MapType {
  kind: "map";
  keyId: string | null;
  key: IcebergType;
  valueId: string | null;
  isValueRequired: boolean | null;
  value: IcebergType;
}

export interface UnknownType {
  kind: "unknown";
  value: unknown;
}

export type IcebergType =
  PrimitiveType | StructType | ListType | MapType | UnknownType;

const isUnknownRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const readProperty = (value: unknown, property: string): unknown =>
  isUnknownRecord(value) ? value[property] : undefined;

const parseId = (value: unknown): string | null => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return String(value);
  }

  if (typeof value === "string" && value.length > 0) {
    return value;
  }

  return null;
};

const parseRequired = (value: unknown): boolean | null =>
  typeof value === "boolean" ? value : null;

const parseFieldId = (value: unknown): string | null =>
  parseId(readProperty(value, "field-id")) ??
  parseId(readProperty(value, "id"));

const parseFields = (
  value: unknown,
  parentIdentity: string,
): IcebergSchemaField[] => {
  if (!Array.isArray(value)) {
    return [];
  }

  return value.map((field, fieldIndex) => {
    const id = parseFieldId(field);
    const positionalIdentity = `${parentIdentity}.field.${String(fieldIndex)}`;
    const identity = id === null ? positionalIdentity : `field-id.${id}`;
    const name = readProperty(field, "name");

    return {
      id,
      identity,
      name: typeof name === "string" ? name : "Unnamed field",
      isRequired: parseRequired(readProperty(field, "required")),
      type: parseType(readProperty(field, "type"), identity),
    };
  });
};

const parseType = (value: unknown, parentIdentity: string): IcebergType => {
  if (typeof value === "string") {
    return { kind: "primitive", name: value };
  }

  const typeName = readProperty(value, "type");

  if (typeName === "struct") {
    return {
      kind: "struct",
      fields: parseFields(readProperty(value, "fields"), parentIdentity),
    };
  }

  if (typeName === "list") {
    return {
      kind: "list",
      elementId: parseId(readProperty(value, "element-id")),
      isElementRequired: parseRequired(readProperty(value, "element-required")),
      element: parseType(
        readProperty(value, "element"),
        `${parentIdentity}.element`,
      ),
    };
  }

  if (typeName === "map") {
    return {
      kind: "map",
      keyId: parseId(readProperty(value, "key-id")),
      key: parseType(readProperty(value, "key"), `${parentIdentity}.key`),
      valueId: parseId(readProperty(value, "value-id")),
      isValueRequired: parseRequired(readProperty(value, "value-required")),
      value: parseType(readProperty(value, "value"), `${parentIdentity}.value`),
    };
  }

  return { kind: "unknown", value };
};

export const parseIcebergSchema = (value: unknown): IcebergSchema => ({
  fields: parseFields(readProperty(value, "fields"), "schema"),
});

export const formatUnknownType = (value: unknown): string => {
  if (
    value === undefined ||
    typeof value === "function" ||
    typeof value === "symbol"
  ) {
    return String(value);
  }

  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return "Unrenderable schema type";
  }
};
