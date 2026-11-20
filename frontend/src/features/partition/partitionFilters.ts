import {
  integerText,
  schemaColumnNames,
} from "../metadata/metadataPresentation";
import { parseIcebergSchema } from "../schema/schemaModel";
import type { TableMetadata } from "../table/api/metadataSchemas";

const PARTITION_FILTER_PREFIX = "p.";
const VOID_TRANSFORM = "void";

export interface PartitionFilterValue {
  name: string;
  value: string;
}

export interface PartitionFieldOption {
  name: string;
  transform: string;
  sourceName: string | null;
  specIds: string[];
}

export const readPartitionFilters = (
  search: Record<string, unknown>,
): PartitionFilterValue[] =>
  Object.entries(search).flatMap(([key, value]) =>
    key.startsWith(PARTITION_FILTER_PREFIX) && typeof value === "string"
      ? [{ name: key.slice(PARTITION_FILTER_PREFIX.length), value }]
      : [],
  );

export const isPartitionFilterKey = (key: string): boolean =>
  key.startsWith(PARTITION_FILTER_PREFIX);

export const partitionFilterSearch = (
  filters: PartitionFilterValue[],
): Record<string, string> =>
  Object.fromEntries(
    filters.map(({ name, value }) => [
      `${PARTITION_FILTER_PREFIX}${name}`,
      value,
    ]),
  );

export const formatPartitionFilters = (
  filters: PartitionFilterValue[],
): string => filters.map(({ name, value }) => `${name} = ${value}`).join(", ");

const TIME_TRANSFORM_PLACEHOLDERS: Record<string, string> = {
  year: "yyyy (UTC)",
  month: "yyyy-MM (UTC)",
  day: "yyyy-MM-dd (UTC)",
  hour: "yyyy-MM-dd-HH (UTC)",
};

export const partitionValuePlaceholder = (transform: string): string =>
  TIME_TRANSFORM_PLACEHOLDERS[transform] ??
  (transform.startsWith("bucket")
    ? "bucket number"
    : "value as stored in the partition");

export const listPartitionFields = (
  metadata: TableMetadata | undefined,
): PartitionFieldOption[] => {
  if (metadata === undefined) return [];
  const currentSchema = metadata.schemas?.find(
    (schema) =>
      integerText(schema["schema-id"]) ===
      integerText(metadata["current-schema-id"]),
  );
  const columnNames = schemaColumnNames(parseIcebergSchema(currentSchema));
  const specs = [...(metadata["partition-specs"] ?? [])].sort(
    (first, second) => Number(second["spec-id"]) - Number(first["spec-id"]),
  );
  const fieldsById = new Map<string, PartitionFieldOption>();
  for (const spec of specs) {
    const specId = String(spec["spec-id"]);
    (spec.fields ?? []).forEach((field, position) => {
      if (field.transform === VOID_TRANSFORM || field.name === undefined)
        return;
      const fieldId = String(field["field-id"] ?? 1000 + position);
      const existingField = fieldsById.get(fieldId);
      if (existingField !== undefined) {
        existingField.specIds.unshift(specId);
        return;
      }
      const sourceId = field["source-id"];
      fieldsById.set(fieldId, {
        name: field.name,
        transform: field.transform ?? "",
        sourceName:
          sourceId === undefined
            ? null
            : (columnNames.get(String(sourceId)) ?? null),
        specIds: [specId],
      });
    });
  }
  const seenNames = new Set<string>();
  return [...fieldsById.values()].filter(({ name }) => {
    if (seenNames.has(name)) return false;
    seenNames.add(name);
    return true;
  });
};
