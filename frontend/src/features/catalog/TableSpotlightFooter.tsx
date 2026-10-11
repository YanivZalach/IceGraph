import { useEffect, useState } from "react";
import type { CatalogTables } from "./useCatalogTables";
import { cn } from "../../shared/lib/cn";
import {
  UI_ERROR_TEXT_CLASS,
  UI_HELPER_TEXT_CLASS,
  UI_LINK_BUTTON_CLASS,
} from "../../uiTypography";

interface TableSpotlightFooterProps {
  catalogTables: CatalogTables;
  escapeHint: string;
  onRefresh: () => void;
}

const RELATIVE_TIME_UNITS = [
  { unit: "day", seconds: 86400 },
  { unit: "hour", seconds: 3600 },
  { unit: "minute", seconds: 60 },
] as const;

const formatUpdatedAgo = (timestamp: number, now: number): string => {
  const elapsedSeconds = Math.max(0, (now - timestamp) / 1000);
  const formatter = new Intl.RelativeTimeFormat(undefined, {
    numeric: "auto",
    style: "short",
  });
  for (const { unit, seconds } of RELATIVE_TIME_UNITS) {
    if (elapsedSeconds >= seconds) {
      return formatter.format(-Math.floor(elapsedSeconds / seconds), unit);
    }
  }
  return "just now";
};

const describeCatalog = (
  { catalog, isLoading }: CatalogTables,
  now: number,
): string => {
  if (catalog === null) return isLoading ? "Loading tables…" : "No tables";
  const count = catalog.data.tables.length;
  return `${count.toLocaleString()} ${count === 1 ? "table" : "tables"} · updated ${formatUpdatedAgo(catalog.fetchedAt, now)}`;
};

const CLOCK_INTERVAL_MS = 60_000;

const KEY_HINT_CLASS =
  "rounded border border-edge px-1 font-mono text-[10px] text-slate-400";

const TableSpotlightFooter = ({
  catalogTables,
  escapeHint,
  onRefresh,
}: TableSpotlightFooterProps) => {
  const [now, setNow] = useState(Date.now);
  useEffect(() => {
    const timer = window.setInterval(() => {
      setNow(Date.now());
    }, CLOCK_INTERVAL_MS);
    return () => {
      window.clearInterval(timer);
    };
  }, []);
  const updatedAt = catalogTables.catalog?.fetchedAt;
  return (
    <div className="flex shrink-0 flex-col gap-1 border-t border-edge bg-surface-hover px-3 py-2">
      <div className="flex items-center justify-between gap-3">
        <span
          className={cn(
            UI_HELPER_TEXT_CLASS,
            "hidden items-center gap-1.5 sm:flex",
          )}
        >
          <kbd className={KEY_HINT_CLASS}>↑↓</kbd> navigate
          <kbd className={KEY_HINT_CLASS}>↵</kbd> open
          <kbd className={KEY_HINT_CLASS}>esc</kbd> {escapeHint}
        </span>
        <span className="ml-auto flex min-w-0 items-center gap-2">
          <span
            className={cn(UI_HELPER_TEXT_CLASS, "truncate")}
            title={
              updatedAt === undefined
                ? undefined
                : new Date(updatedAt).toLocaleString()
            }
          >
            {describeCatalog(catalogTables, now)}
          </span>
          <button
            type="button"
            onClick={onRefresh}
            disabled={catalogTables.isRefreshing}
            title="Refresh table list"
            aria-label="Refresh table list"
            className="shrink-0 rounded p-1 text-slate-400 transition hover:bg-edge hover:text-ink disabled:cursor-not-allowed"
          >
            <svg
              viewBox="0 0 24 24"
              className={cn(
                "h-3.5 w-3.5",
                catalogTables.isRefreshing && "animate-spin",
              )}
              fill="none"
              stroke="currentColor"
              strokeWidth={2}
              strokeLinecap="round"
              strokeLinejoin="round"
              aria-hidden="true"
            >
              <path d="M21 12a9 9 0 1 1-2.64-6.36" />
              <path d="M21 3v6h-6" />
            </svg>
          </button>
        </span>
      </div>
      {catalogTables.errorMessage !== null && (
        <p className={cn(UI_ERROR_TEXT_CLASS, "flex items-center gap-2")}>
          <span className="min-w-0 truncate">
            Could not refresh tables: {catalogTables.errorMessage}
          </span>
          <button
            type="button"
            onClick={onRefresh}
            disabled={catalogTables.isRefreshing}
            className={cn(UI_LINK_BUTTON_CLASS, "shrink-0")}
          >
            Retry
          </button>
        </p>
      )}
      {catalogTables.catalog?.data.include_none_iceberg_catalogs === true && (
        <p className={UI_HELPER_TEXT_CLASS}>
          Includes non-Iceberg catalogs, so non-Iceberg tables may also appear.
        </p>
      )}
    </div>
  );
};

export default TableSpotlightFooter;
