const MILLISECONDS_PER_UNIT = [
  ["d", 24n * 60n * 60n * 1000n],
  ["h", 60n * 60n * 1000n],
  ["m", 60n * 1000n],
  ["s", 1000n],
] as const;

const DECIMAL_INTEGER = /^-?\d+$/;

/** Whole units only (`7d`, `36h`), so the reader sees the value as Iceberg stores it. */
export const formatDurationMs = (milliseconds: number | string): string => {
  const text = String(milliseconds);
  if (!DECIMAL_INTEGER.test(text)) {
    return `${text} ms`;
  }
  const totalMs = BigInt(text);
  for (const [suffix, unitMs] of MILLISECONDS_PER_UNIT) {
    if (totalMs >= unitMs && totalMs % unitMs === 0n) {
      return `${(totalMs / unitMs).toString()}${suffix}`;
    }
  }
  return `${totalMs.toString()} ms`;
};
