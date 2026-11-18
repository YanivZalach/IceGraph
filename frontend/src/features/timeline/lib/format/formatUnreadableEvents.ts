export const formatUnreadableEvents = (count: number): string =>
  count === 1
    ? "1 event could not be read"
    : `${count.toString()} events could not be read`;
