export interface MetricRatio {
  denominator: bigint;
  numerator: bigint;
}

const UNSIGNED_INTEGER_PATTERN = /^\d+$/;
const SIGNED_INTEGER_PATTERN = /^-?\d+$/;
const MAXIMUM_SAFE_COUNT = BigInt(Number.MAX_SAFE_INTEGER);

export const parseExactCount = (value: unknown): bigint | undefined => {
  if (typeof value === "bigint") return value >= 0n ? value : undefined;
  if (typeof value === "number") {
    return Number.isSafeInteger(value) && value >= 0
      ? BigInt(value)
      : undefined;
  }
  if (typeof value === "string" && UNSIGNED_INTEGER_PATTERN.test(value)) {
    return BigInt(value);
  }
  return undefined;
};

export const parseExactSignedCount = (value: unknown): bigint | undefined => {
  if (typeof value === "bigint") return value;
  if (typeof value === "number") {
    return Number.isSafeInteger(value) ? BigInt(value) : undefined;
  }
  if (typeof value === "string" && SIGNED_INTEGER_PATTERN.test(value)) {
    return BigInt(value);
  }
  return undefined;
};

export const parseFileSizeBytes = (
  value: string | number | null | undefined,
): number | null => {
  const byteCount = parseExactCount(value);
  return byteCount === undefined || byteCount > MAXIMUM_SAFE_COUNT
    ? null
    : Number(byteCount);
};

export const compareBigInts = (first: bigint, second: bigint): number =>
  first === second ? 0 : first < second ? -1 : 1;

export const createMetricRatio = (
  numerator: bigint | undefined,
  denominator: bigint | undefined,
  requirePercentageRange = false,
): MetricRatio | undefined => {
  if (
    numerator === undefined ||
    denominator === undefined ||
    denominator <= 0n ||
    (requirePercentageRange && numerator > denominator)
  ) {
    return undefined;
  }
  return { denominator, numerator };
};

export const compareMetricRatios = (
  first: MetricRatio,
  second: MetricRatio,
): number =>
  compareBigInts(
    first.numerator * second.denominator,
    second.numerator * first.denominator,
  );
