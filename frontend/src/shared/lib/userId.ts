const USER_ID_KEY = "icegraphUserId";
const USER_ID_BYTES = 16;

// crypto.randomUUID is unavailable on plain HTTP deployments.
const createUserId = (): string =>
  Array.from(crypto.getRandomValues(new Uint8Array(USER_ID_BYTES)), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("");

let sessionUserId: string | undefined;

export const getUserId = (): string => {
  sessionUserId ??= createUserId();

  try {
    const savedUserId = localStorage.getItem(USER_ID_KEY);
    if (savedUserId !== null) {
      return savedUserId;
    }
    localStorage.setItem(USER_ID_KEY, sessionUserId);
  } catch {
    return sessionUserId;
  }

  return sessionUserId;
};
