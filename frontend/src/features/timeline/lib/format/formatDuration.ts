const MILLISECONDS_PER_UNIT = [
  ["d", 24 * 60 * 60 * 1000],
  ["h", 60 * 60 * 1000],
  ["m", 60 * 1000],
  ["s", 1000],
] as const;

/** Whole units only (`7d`, `36h`), so the reader sees the value as Iceberg stores it. */
export const formatDurationMs = (milliseconds: number | string): string => {
  const totalMs = Number(milliseconds);
  for (const [suffix, unitMs] of MILLISECONDS_PER_UNIT) {
    if (totalMs >= unitMs && totalMs % unitMs === 0) {
      return `${(totalMs / unitMs).toString()}${suffix}`;
    }
  }
  return `${totalMs.toString()} ms`;
};
