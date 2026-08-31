import { format, intlFormatDistance } from "date-fns";

const ISO_MILLISECOND_LENGTH = "yyyy-MM-ddTHH:mm:ss.SSS".length;

const MILLISECONDS_PER_HOUR = 60 * 60 * 1000;
const MILLISECONDS_PER_DAY = 24 * MILLISECONDS_PER_HOUR;

const NARROW_DISTANCE_OPTIONS = {
  locale: "en",
  style: "narrow",
  numeric: "always",
} as const;

const DAY_AND_MONTH_PATTERN = "MMM d";
const DAY_MONTH_AND_CLOCK_PATTERN = "MMM d, HH:mm";
const DAY_MONTH_YEAR_AND_CLOCK_PATTERN = "MMM d, yyyy, HH:mm";
const ABSOLUTE_PATTERN = "MMM d, yyyy, HH:mm:ss.SSS XXX";

/**
 * The backend stamps every timestamp as `yyyy-MM-dd HH:mm:ss.SSSSSS` in UTC with no zone
 * (backend/constants.py:10); swapping in the `T`, trimming to milliseconds and adding `Z` makes
 * it real ISO.
 */
export const parseUtcTimestampToMillis = (
  rawTimestamp: string,
): number | null => {
  const isoTimestamp = `${rawTimestamp.replace(" ", "T").slice(0, ISO_MILLISECOND_LENGTH)}Z`;
  const timestampMs = Date.parse(isoTimestamp);

  return Number.isNaN(timestampMs) ? null : timestampMs;
};

/**
 * The hour unit is forced below a day because `intlFormatDistance` rounds to days from about 16
 * hours, which would read `1d ago` on a row the column would still show a clock time for.
 */
export const formatEventTime = (timestampMs: number, nowMs: number): string => {
  const elapsedMs = Math.abs(nowMs - timestampMs);

  if (elapsedMs >= MILLISECONDS_PER_DAY) {
    return format(timestampMs, DAY_MONTH_AND_CLOCK_PATTERN);
  }

  if (elapsedMs >= MILLISECONDS_PER_HOUR) {
    return intlFormatDistance(timestampMs, nowMs, {
      unit: "hour",
      ...NARROW_DISTANCE_OPTIONS,
    });
  }

  return intlFormatDistance(timestampMs, nowMs, NARROW_DISTANCE_OPTIONS);
};

export const formatAbsoluteTimestamp = (timestampMs: number): string =>
  format(timestampMs, ABSOLUTE_PATTERN);

export const formatDayAndMonth = (timestampMs: number): string =>
  format(timestampMs, DAY_AND_MONTH_PATTERN);

export const formatDayMonthYearAndClock = (timestampMs: number): string =>
  format(timestampMs, DAY_MONTH_YEAR_AND_CLOCK_PATTERN);
