const SHORT_ID_DIGIT_COUNT = 6;

export const formatShortId = (snapshotId: string | null): string =>
  snapshotId === null ? "" : `…${snapshotId.slice(-SHORT_ID_DIGIT_COUNT)}`;
