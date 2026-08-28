import type { SchemaDiffStatus, SchemaFieldDiff } from "./schemaDiffTypes";

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

export const getSchemaFieldDiffLabel = (fieldDiff: SchemaFieldDiff): string => {
  if (fieldDiff.movement === "from") {
    return "Moved from this location";
  }

  if (fieldDiff.movement === "to") {
    return "Moved to this location";
  }

  switch (fieldDiff.status) {
    case "added":
      return "Added field";
    case "removed":
      return "Removed field";
    case "changed":
      return "Changed field";
    case "moved":
      return "Moved field";
    case "descendant-changed":
      return "Contains nested changes";
    case "unchanged":
      return "Unchanged field";
  }
};
