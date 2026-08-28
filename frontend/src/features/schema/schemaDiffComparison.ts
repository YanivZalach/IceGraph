import type { SchemaDiffStatus } from "./schemaDiffTypes";

export const nestedStatus = (
  hasDirectChange: boolean,
  childStatuses: SchemaDiffStatus[],
): SchemaDiffStatus => {
  if (hasDirectChange) {
    return "changed";
  }

  return childStatuses.some((status) => status !== "unchanged")
    ? "descendant-changed"
    : "unchanged";
};

const isUnknownRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

export const areUnknownValuesEqual = (
  before: unknown,
  after: unknown,
): boolean => {
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
