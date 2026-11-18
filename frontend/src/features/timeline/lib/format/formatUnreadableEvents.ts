const COULD_NOT_BE_READ = "could not be read";

export const formatUnreadableEvents = (count: number): string =>
  `${count === 1 ? "1 event" : `${count.toString()} events`} ${COULD_NOT_BE_READ}`;

export const formatUnreadableCount = (count: number): string =>
  `${count.toString()} ${COULD_NOT_BE_READ}`;
