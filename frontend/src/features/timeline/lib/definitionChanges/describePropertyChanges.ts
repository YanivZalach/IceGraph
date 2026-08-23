import type { TableProperties } from "../../api/nodeSchemas";
import { formatByteSize } from "../format/backendSize";
import { formatShortId } from "../format/formatShortId";
import { sameTextChange, type DescribedChange } from "./describedChange";

const BYTE_SIZE_PROPERTY_PATTERN = /-size(-bytes)?$/;
const ALL_DIGITS_PATTERN = /^\d+$/;

/** Set by Nessie and Spark themselves, not by anyone. */
const IGNORED_PROPERTY_KEYS = new Set([
  "nessie.commit.id",
  "write.distribution-mode",
]);

/** The hidden keys, for rows where they are the only thing that changed. */
export const describeIgnoredPropertyChanges = (
  previousProperties: TableProperties,
  currentProperties: TableProperties,
): DescribedChange[] =>
  [...IGNORED_PROPERTY_KEYS]
    .filter((key) => previousProperties[key] !== currentProperties[key])
    .map((key) =>
      sameTextChange(
        `${key} ${formatShortId(previousProperties[key] ?? null)} → ${formatShortId(currentProperties[key] ?? null)}`,
      ),
    );

const formatPropertyValue = (key: string, value: string): string =>
  BYTE_SIZE_PROPERTY_PATTERN.test(key) && ALL_DIGITS_PATTERN.test(value)
    ? formatByteSize(Number(value))
    : value;

export const describePropertyChanges = (
  previousProperties: TableProperties,
  currentProperties: TableProperties,
): DescribedChange[] => {
  const changes: DescribedChange[] = [];

  for (const [key, currentValue] of Object.entries(currentProperties)) {
    const previousValue = previousProperties[key];

    if (IGNORED_PROPERTY_KEYS.has(key)) {
      continue;
    }

    if (previousValue === undefined) {
      changes.push(
        sameTextChange(
          `${key} set to ${formatPropertyValue(key, currentValue)}`,
        ),
      );
    } else if (previousValue !== currentValue) {
      changes.push(
        sameTextChange(
          `${key} ${formatPropertyValue(key, previousValue)} → ${formatPropertyValue(key, currentValue)}`,
        ),
      );
    }
  }

  for (const key of Object.keys(previousProperties)) {
    if (IGNORED_PROPERTY_KEYS.has(key)) {
      continue;
    }
    if (currentProperties[key] === undefined) {
      changes.push(sameTextChange(`${key} removed`));
    }
  }

  return changes;
};
