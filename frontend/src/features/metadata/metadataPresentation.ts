import type { IcebergInteger } from "../table/api/metadataSchemas";
import type { IcebergSchema, IcebergType } from "../schema/schemaModel";
import { formatLocaleDateTime, parseUtcDate } from "../../utils/dateUtils";

export const integerText = (
  value: IcebergInteger | null | undefined,
): string | null => (value == null ? null : String(value));

export const formatCount = (value: string | null | undefined): string => {
  if (value == null || !/^\d+$/.test(value)) return "Unavailable";
  return value.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
};

export const formatMetadataTime = (
  value: IcebergInteger | null | undefined,
): string => {
  if (value == null) return "Unavailable";
  const date = new Date(Number(value));
  return Number.isNaN(date.getTime())
    ? "Unavailable"
    : formatLocaleDateTime(date);
};

export const formatSnapshotTime = (
  value: string | null | undefined,
): string => {
  if (!value) return "Unavailable";
  const date = parseUtcDate(value);
  return date === null ? value : formatLocaleDateTime(date);
};

// This is a display-name lookup only. IDs, transforms, and collection semantics
// remain those supplied by the backend.
export const schemaColumnNames = (
  schema: IcebergSchema,
): ReadonlyMap<string, string> => {
  const names = new Map<string, string>();
  const visit = (type: IcebergType, path: string): void => {
    if (type.kind === "struct") {
      type.fields.forEach((field) => {
        const name = path ? `${path}.${field.name}` : field.name;
        if (field.id !== null) names.set(field.id, name);
        visit(field.type, name);
      });
    } else if (type.kind === "list") {
      if (type.elementId !== null) names.set(type.elementId, `${path}.element`);
      visit(type.element, `${path}.element`);
    } else if (type.kind === "map") {
      if (type.keyId !== null) names.set(type.keyId, `${path}.key`);
      if (type.valueId !== null) names.set(type.valueId, `${path}.value`);
      visit(type.key, `${path}.key`);
      visit(type.value, `${path}.value`);
    }
  };
  visit({ kind: "struct", fields: schema.fields }, "");
  return names;
};
