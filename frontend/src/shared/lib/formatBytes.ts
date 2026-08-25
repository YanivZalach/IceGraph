const BYTES_IN_GIBIBYTE = 1024 ** 3;
const GIBIBYTE_FRACTION_DIGITS = 5;
const TINY_VALUE_SIGNIFICANT_DIGITS = 3;

const BYTE_FIELD_SUFFIX_PATTERN = /(^|[-_])(in[-_])?bytes$/;

export const isByteFieldName = (fieldName: string): boolean =>
  BYTE_FIELD_SUFFIX_PATTERN.test(fieldName);

export const stripByteUnitFromFieldName = (fieldName: string): string =>
  fieldName.replace(BYTE_FIELD_SUFFIX_PATTERN, "") || fieldName;

export const formatBytesAsGibibytes = (byteCount: string): string => {
  if (!byteCount) return "";

  const parsedByteCount = Number(byteCount);
  if (!Number.isFinite(parsedByteCount)) return byteCount;

  const gibibytes = parsedByteCount / BYTES_IN_GIBIBYTE;
  const rounded = gibibytes.toFixed(GIBIBYTE_FRACTION_DIGITS);
  const isRoundedAwayToZero = Number(rounded) === 0 && parsedByteCount !== 0;

  return `${isRoundedAwayToZero ? gibibytes.toExponential(TINY_VALUE_SIGNIFICANT_DIGITS) : rounded} GiB`;
};
