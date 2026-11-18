const COULD_NOT_BE_READ = "could not be read";

export const formatUnreadableEvents = (count: number): string =>
  `${count === 1 ? "1 event" : `${count.toString()} events`} ${COULD_NOT_BE_READ}`;

/** For text that has already named the events, such as the toolbar's "9 events · 3 could not be read". */
export const formatUnreadableCount = (count: number): string =>
  `${count.toString()} ${COULD_NOT_BE_READ}`;
