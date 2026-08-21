const BACKEND_SIZE_PATTERN = /^(\d+(?:\.\d+)?) GB$/;
const BYTES_PER_GIGABYTE = 1024 ** 3;

const SIZE_UNITS = [
  { unit: "TB", bytesPerUnit: 1024 ** 4 },
  { unit: "GB", bytesPerUnit: 1024 ** 3 },
  { unit: "MB", bytesPerUnit: 1024 ** 2 },
  { unit: "KB", bytesPerUnit: 1024 },
] as const;

/**
 * The backend pre-formats every `*files-size` summary value as `"0.08203 GB"` — bytes divided by
 * 1024³ (backend/collectors/collect_snapshots.py:68). This reverses that so the timeline can show
 * a readable unit instead.
 */
export const parseBackendSizeToBytes = (
  rawSize: string | undefined,
): number | null => {
  const gigabytesText = rawSize?.match(BACKEND_SIZE_PATTERN)?.[1];

  return gigabytesText === undefined
    ? null
    : Math.round(Number(gigabytesText) * BYTES_PER_GIGABYTE);
};

export const formatByteSize = (sizeInBytes: number): string => {
  const matchedUnit = SIZE_UNITS.find(
    ({ bytesPerUnit }) => sizeInBytes >= bytesPerUnit,
  );

  if (matchedUnit === undefined) {
    return `${sizeInBytes.toString()} B`;
  }

  const valueInUnit = sizeInBytes / matchedUnit.bytesPerUnit;
  const roundedValue =
    valueInUnit >= 10
      ? Math.round(valueInUnit)
      : Math.round(valueInUnit * 10) / 10;

  const hasRoundedIntoNextUnit = roundedValue === 1024;
  if (hasRoundedIntoNextUnit) {
    return formatByteSize(sizeInBytes + matchedUnit.bytesPerUnit);
  }

  return `${roundedValue.toString()} ${matchedUnit.unit}`;
};
