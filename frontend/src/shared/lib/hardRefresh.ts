const HARD_REFRESH_URL_KEY = "icegraph:hard-refresh-url";

export const shouldBypassPersistentGraphCache = (): boolean => {
  try {
    return (
      sessionStorage.getItem(HARD_REFRESH_URL_KEY) === window.location.href
    );
  } catch (storageError) {
    console.warn("Failed to read the hard refresh marker", storageError);
    return false;
  }
};

export const clearHardRefreshRequest = (): void => {
  try {
    if (sessionStorage.getItem(HARD_REFRESH_URL_KEY) === window.location.href) {
      sessionStorage.removeItem(HARD_REFRESH_URL_KEY);
    }
  } catch (storageError) {
    console.warn("Failed to clear the hard refresh marker", storageError);
  }
};

export const registerHardRefreshShortcut = (): void => {
  window.addEventListener(
    "keydown",
    (event) => {
      const isModifiedReload =
        event.shiftKey &&
        ((event.key.toLowerCase() === "r" &&
          (event.metaKey || event.ctrlKey)) ||
          event.key === "F5");
      if (!isModifiedReload) return;

      try {
        sessionStorage.setItem(HARD_REFRESH_URL_KEY, window.location.href);
      } catch (storageError) {
        console.warn("Failed to mark the hard refresh", storageError);
      }
    },
    { capture: true },
  );
};
