const BYTES_IN_GIBIBYTE = 1024 ** 3;
const GIBIBYTE_FRACTION_DIGITS = 5;
const TINY_VALUE_SIGNIFICANT_DIGITS = 3;

export const isByteFieldName = (fieldName: string): boolean =>
  fieldName.endsWith("bytes");

export const formatBytesAsGibibytes = (
  byteCount: string | number | null | undefined,
): string => {
  if (byteCount == null || byteCount === "") return "";

  const parsedByteCount = Number(byteCount);
  if (!Number.isFinite(parsedByteCount)) return String(byteCount);

  const gibibytes = parsedByteCount / BYTES_IN_GIBIBYTE;
  const rounded = gibibytes.toFixed(GIBIBYTE_FRACTION_DIGITS);
  const isRoundedAwayToZero = Number(rounded) === 0 && parsedByteCount !== 0;

  return `${isRoundedAwayToZero ? gibibytes.toExponential(TINY_VALUE_SIGNIFICANT_DIGITS) : rounded} GiB`;
};
