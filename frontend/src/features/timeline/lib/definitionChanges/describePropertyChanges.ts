import type { TableProperties } from "../../api/nodeSchemas";
import { formatByteSize } from "../format/backendSize";
import { sameTextChange, type DescribedChange } from "./describedChange";

/** `-size` alone is not a unit: `read.parquet.vectorization.batch-size` counts rows. */
const BYTE_SIZE_PROPERTY_PATTERN = /-bytes$/;
/** 15 digits keeps Number() exact; longer values are shown as written. */
const SAFE_WHOLE_NUMBER_PATTERN = /^\d{1,15}$/;

/** Stamped fresh by Nessie on every commit — bookkeeping, not a setting change. */
const ALWAYS_IGNORED_KEYS = new Set(["nessie.commit.id"]);

/** Spark sets this itself as part of `WRITE ORDERED BY`. */
const SORT_ORDER_SIDE_EFFECT_KEYS = new Set(["write.distribution-mode"]);

const valueOrUnset = (value: string | undefined): string => value ?? "unset";

export const describeIgnoredPropertyChanges = (
  previousProperties: TableProperties,
  currentProperties: TableProperties,
): DescribedChange[] =>
  [...ALWAYS_IGNORED_KEYS]
    .filter((key) => previousProperties[key] !== currentProperties[key])
    .map((key) =>
      sameTextChange(
        `${key} ${valueOrUnset(previousProperties[key])} → ${valueOrUnset(currentProperties[key])}`,
      ),
    );

const formatPropertyValue = (key: string, value: string): string =>
  BYTE_SIZE_PROPERTY_PATTERN.test(key) && SAFE_WHOLE_NUMBER_PATTERN.test(value)
    ? formatByteSize(Number(value))
    : value;

export const describePropertyChanges = (
  previousProperties: TableProperties,
  currentProperties: TableProperties,
  hasSortOrderChanged: boolean,
): DescribedChange[] => {
  const isIgnored = (key: string): boolean =>
    ALWAYS_IGNORED_KEYS.has(key) ||
    (hasSortOrderChanged && SORT_ORDER_SIDE_EFFECT_KEYS.has(key));

  const changes: DescribedChange[] = [];

  for (const [key, currentValue] of Object.entries(currentProperties)) {
    const previousValue = previousProperties[key];

    if (isIgnored(key)) {
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
    if (isIgnored(key)) {
      continue;
    }
    if (currentProperties[key] === undefined) {
      changes.push(sameTextChange(`${key} removed`));
    }
  }

  return changes;
};
