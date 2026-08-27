import { formatByteSize, parseBackendSizeToBytes } from "./backendSize";

const thousandsSeparatorFormatter = new Intl.NumberFormat("en-US");

/** 15 digits keeps Number() exact; anything longer is an id, not a count. */
const SMALL_WHOLE_NUMBER_PATTERN = /^\d{1,15}$/;

export const humanizeLabel = (label: string): string => {
  if (label.includes(".")) {
    return label;
  }
  const spaced = label.replace(/-/g, " ");
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
};

export const humanizeValue = (value: string, isCopyable: boolean): string => {
  if (isCopyable) {
    return value;
  }
  const sizeBytes = parseBackendSizeToBytes(value);
  if (sizeBytes !== null) {
    return formatByteSize(sizeBytes);
  }
  if (SMALL_WHOLE_NUMBER_PATTERN.test(value)) {
    return thousandsSeparatorFormatter.format(Number(value));
  }
  return value;
};
