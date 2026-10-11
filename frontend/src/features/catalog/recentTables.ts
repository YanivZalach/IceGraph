const RECENT_TABLES_KEY = "tableHistory";
const RECENT_TABLES_LENGTH = 5;

export const readRecentTables = (): string[] => {
  try {
    const saved: unknown = JSON.parse(
      localStorage.getItem(RECENT_TABLES_KEY) ?? "[]",
    );
    return Array.isArray(saved)
      ? saved.filter((item: unknown) => typeof item === "string")
      : [];
  } catch {
    return [];
  }
};

export const rememberRecentTable = (tableName: string): void => {
  localStorage.setItem(
    RECENT_TABLES_KEY,
    JSON.stringify(
      [...new Set([tableName, ...readRecentTables()])].slice(
        0,
        RECENT_TABLES_LENGTH,
      ),
    ),
  );
};
