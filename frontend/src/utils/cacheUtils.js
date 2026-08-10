async function openDB() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open("icegraph_cache", 1);
    req.onupgradeneeded = () => {
      req.result.createObjectStore("app_state");
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function cacheData(key, data) {
  const db = await openDB();
  const tx = db.transaction("app_state", "readwrite");
  return new Promise((resolve, reject) => {
    const req = tx.objectStore("app_state").put(data, key);
    req.onsuccess = () => resolve();
    req.onerror = () => reject(req.error);
  });
}

async function getCachedData(key) {
  const db = await openDB();
  const tx = db.transaction("app_state", "readonly");
  return new Promise((resolve, reject) => {
    const req = tx.objectStore("app_state").get(key);
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

async function clearCachedData(key) {
  const db = await openDB();
  const tx = db.transaction("app_state", "readwrite");
  return new Promise((resolve, reject) => {
    const req = tx.objectStore("app_state").delete(key);
    req.onsuccess = () => resolve();
    req.onerror = () => reject(req.error);
  });
}

export { cacheData, getCachedData, openDB, clearCachedData };
