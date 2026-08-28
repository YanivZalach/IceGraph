import type { SchemaDiffStatus, SchemaFieldDiff } from "./schemaDiff";

const statusMarker = (status: SchemaDiffStatus): string => {
  switch (status) {
    case "added":
      return "+";
    case "removed":
      return "−";
    case "changed":
      return "~";
    case "moved":
      return "↔";
    case "descendant-changed":
      return "·";
    case "unchanged":
      return "";
  }
};

export const getSchemaFieldDiffMarker = (
  fieldDiff: SchemaFieldDiff,
): string => {
  if (fieldDiff.movement === "from") {
    return "←";
  }

  if (fieldDiff.movement === "to") {
    return "→";
  }

  return statusMarker(fieldDiff.status);
};
