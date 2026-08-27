const BYTES_IN_MEBIBYTE = 1024 ** 2;
const BYTES_IN_GIBIBYTE = 1024 ** 3;
const FRACTION_DIGITS = 5;

const BYTE_FIELD_SUFFIX_PATTERN = /(^|[-_])(in[-_])?bytes$/;

export const isByteFieldName = (fieldName: string): boolean =>
  BYTE_FIELD_SUFFIX_PATTERN.test(fieldName);

export const stripByteUnitFromFieldName = (fieldName: string): string =>
  fieldName.replace(BYTE_FIELD_SUFFIX_PATTERN, "") || fieldName;

const formatBytes = (
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
  formatBytes(byteCount, BYTES_IN_MEBIBYTE, "MiB");

export const formatBytesAsGibibytes = (byteCount: string | number): string =>
  formatBytes(byteCount, BYTES_IN_GIBIBYTE, "GiB");
