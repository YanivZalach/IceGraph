const BYTES_IN_MEBIBYTE = 1024 ** 2;
const BYTES_IN_GIBIBYTE = 1024 ** 3;
const FRACTION_DIGITS = 5;

const BYTE_FIELD_SUFFIX_PATTERN = /(^|[-_])(in[-_])?bytes$/;

export const isByteFieldName = (fieldName: string): boolean =>
  BYTE_FIELD_SUFFIX_PATTERN.test(fieldName);

export const stripByteUnitFromFieldName = (fieldName: string): string =>
  fieldName.replace(BYTE_FIELD_SUFFIX_PATTERN, "") || fieldName;

const formatBytesInUnit = (
  byteCount: string | number,
  bytesPerUnit: number,
  unitLabel: string,
): string => {
  if (byteCount === "") return "";

  const parsedByteCount = Number(byteCount);
  if (!Number.isFinite(parsedByteCount)) return String(byteCount);

  return `${(parsedByteCount / bytesPerUnit).toFixed(FRACTION_DIGITS)} ${unitLabel}`;
};

export const formatBytesAsMebibytes = (byteCount: string | number): string =>
  formatBytesInUnit(byteCount, BYTES_IN_MEBIBYTE, "MiB");

export const formatBytesAsGibibytes = (byteCount: string | number): string =>
  formatBytesInUnit(byteCount, BYTES_IN_GIBIBYTE, "GiB");

export const formatBytes = (byteCount: string | number): string => {
  if (byteCount === "") return "";
  const parsedByteCount = Number(byteCount);
  if (!Number.isFinite(parsedByteCount)) return String(byteCount);
  if (parsedByteCount < 1024) return `${String(parsedByteCount)} bytes`;
  if (parsedByteCount < BYTES_IN_MEBIBYTE)
    return `${(parsedByteCount / 1024).toFixed(2)} KiB`;
  if (parsedByteCount < BYTES_IN_GIBIBYTE)
    return `${(parsedByteCount / BYTES_IN_MEBIBYTE).toFixed(2)} MiB`;
  return `${(parsedByteCount / BYTES_IN_GIBIBYTE).toFixed(2)} GiB`;
};
