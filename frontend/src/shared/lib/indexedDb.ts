const DATABASE_NAME = "icegraph_cache";
const DATABASE_VERSION = 1;
const OBJECT_STORE_NAME = "app_state";

interface CachedEntry {
  key: IDBValidKey;
  value: unknown;
}

let databasePromise: Promise<IDBDatabase> | undefined;

const getDatabaseError = (
  error: DOMException | null,
  fallbackMessage: string,
): Error => error ?? new Error(fallbackMessage);

const openDatabase = (): Promise<IDBDatabase> => {
  if (databasePromise !== undefined) return databasePromise;

  databasePromise = new Promise((resolve, reject) => {
    const request = indexedDB.open(DATABASE_NAME, DATABASE_VERSION);
    request.onupgradeneeded = () => {
      if (!request.result.objectStoreNames.contains(OBJECT_STORE_NAME)) {
        request.result.createObjectStore(OBJECT_STORE_NAME);
      }
    };
    request.onsuccess = () => {
      request.result.onversionchange = () => {
        request.result.close();
        databasePromise = undefined;
      };
      resolve(request.result);
    };
    request.onerror = () => {
      reject(getDatabaseError(request.error, "Failed to open browser cache"));
    };
  });

  return databasePromise;
};

export const getCachedValue = async (key: IDBValidKey): Promise<unknown> => {
  const database = await openDatabase();
  const request: IDBRequest<unknown> = database
    .transaction(OBJECT_STORE_NAME, "readonly")
    .objectStore(OBJECT_STORE_NAME)
    .get(key);

  return new Promise((resolve, reject) => {
    request.onsuccess = () => {
      resolve(request.result);
    };
    request.onerror = () => {
      reject(getDatabaseError(request.error, "Failed to read browser cache"));
    };
  });
};

export const setCachedValue = async (
  key: IDBValidKey,
  value: unknown,
): Promise<void> => {
  const database = await openDatabase();
  const transaction = database.transaction(OBJECT_STORE_NAME, "readwrite");
  transaction.objectStore(OBJECT_STORE_NAME).put(value, key);

  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => {
      resolve();
    };
    transaction.onerror = () => {
      reject(
        getDatabaseError(transaction.error, "Failed to write browser cache"),
      );
    };
    transaction.onabort = () => {
      reject(
        getDatabaseError(transaction.error, "Browser cache write was aborted"),
      );
    };
  });
};

export const deleteCachedValue = async (key: IDBValidKey): Promise<void> => {
  const database = await openDatabase();
  const transaction = database.transaction(OBJECT_STORE_NAME, "readwrite");
  transaction.objectStore(OBJECT_STORE_NAME).delete(key);

  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => {
      resolve();
    };
    transaction.onerror = () => {
      reject(
        getDatabaseError(
          transaction.error,
          "Failed to delete browser cache entry",
        ),
      );
    };
    transaction.onabort = () => {
      reject(
        getDatabaseError(
          transaction.error,
          "Browser cache deletion was aborted",
        ),
      );
    };
  });
};

export const getAllCachedEntries = async (): Promise<CachedEntry[]> => {
  const database = await openDatabase();
  const request = database
    .transaction(OBJECT_STORE_NAME, "readonly")
    .objectStore(OBJECT_STORE_NAME)
    .openCursor();

  return new Promise((resolve, reject) => {
    const entries: CachedEntry[] = [];
    request.onsuccess = () => {
      const cursor = request.result;
      if (cursor === null) {
        resolve(entries);
        return;
      }
      entries.push({ key: cursor.primaryKey, value: cursor.value });
      cursor.continue();
    };
    request.onerror = () => {
      reject(getDatabaseError(request.error, "Failed to scan browser cache"));
    };
  });
};
