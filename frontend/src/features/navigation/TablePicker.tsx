import { useEffect, useRef, useState, type SubmitEvent } from "react";
import { useQuery } from "@tanstack/react-query";
import { useHotkey } from "@tanstack/react-hotkeys";
import { catalogQueryOptions } from "../catalog/api/catalogQueries";
import CatalogTableList from "../../components/CatalogTableList";
import { BASE_PATH, IS_MOCK, MOCK_TABLE } from "../../appConstants";
import {
  UI_ERROR_TEXT_SPACED_CLASS,
  UI_FORM_LABEL_CLASS,
  UI_LINK_BUTTON_CLASS,
  UI_PRIMARY_BUTTON_SM_CLASS,
  UI_TABLE_NAME_BUTTON_CLASS,
  UI_TEXT_INPUT_CLASS,
} from "../../uiTypography";
import { cn } from "../../shared/lib/cn";

interface TablePickerProps {
  tableName: string;
}

const TABLE_HISTORY_KEY = "tableHistory";
const TABLE_HISTORY_LENGTH = 5;

const rememberTable = (tableName: string): void => {
  const saved: unknown = JSON.parse(
    localStorage.getItem(TABLE_HISTORY_KEY) ?? "[]",
  );
  const history = Array.isArray(saved)
    ? saved.filter((item: unknown) => typeof item === "string")
    : [];
  localStorage.setItem(
    TABLE_HISTORY_KEY,
    JSON.stringify(
      [...new Set([tableName, ...history])].slice(0, TABLE_HISTORY_LENGTH),
    ),
  );
};

const tableUrl = (tableName: string): string =>
  `${BASE_PATH}/snapshots-selection?${new URLSearchParams({ table: tableName }).toString()}`;

const TablePicker = ({ tableName }: TablePickerProps) => {
  const [isOpen, setIsOpen] = useState(false);
  const [draftTableName, setDraftTableName] = useState("");
  const [isCatalogListOpen, setIsCatalogListOpen] = useState(false);
  const [catalogFilter, setCatalogFilter] = useState("");
  const catalogQuery = useQuery(catalogQueryOptions());
  const containerRef = useRef<HTMLDivElement>(null);
  useHotkey(
    "Escape",
    () => {
      setIsOpen(false);
    },
    { conflictBehavior: "allow", enabled: isOpen },
  );

  useEffect(() => {
    if (!isOpen) return;
    const handleOutsideClick = (event: MouseEvent): void => {
      const isInside =
        event.target instanceof Node &&
        containerRef.current?.contains(event.target);
      if (!isInside) setIsOpen(false);
    };
    document.addEventListener("mousedown", handleOutsideClick);
    return () => {
      document.removeEventListener("mousedown", handleOutsideClick);
    };
  }, [isOpen]);

  const handleToggle = (): void => {
    setDraftTableName(tableName);
    setIsCatalogListOpen(false);
    setCatalogFilter("");
    setIsOpen((current) => !current);
  };

  const handleBrowseCatalog = (): void => {
    setIsCatalogListOpen(true);
    setCatalogFilter("");
    void catalogQuery.refetch();
  };

  const handleSubmit = (event: SubmitEvent<HTMLFormElement>): void => {
    event.preventDefault();
    const nextTableName = IS_MOCK ? MOCK_TABLE : draftTableName.trim();
    if (nextTableName === "") return;
    rememberTable(nextTableName);
    window.open(tableUrl(nextTableName), "_blank", "noopener,noreferrer");
    setIsOpen(false);
  };

  return (
    <div ref={containerRef} className="relative min-w-20 shrink">
      <button
        type="button"
        onClick={handleToggle}
        className={cn(
          UI_TABLE_NAME_BUTTON_CLASS,
          "block w-full max-w-60 truncate",
        )}
        title={`${tableName} (change table)`}
        aria-expanded={isOpen}
      >
        {tableName}
      </button>
      {isOpen && (
        <form
          onSubmit={handleSubmit}
          className="absolute top-full left-0 z-[70] mt-2 flex w-80 flex-col gap-3 rounded-lg border border-edge bg-surface p-4 shadow-xl"
        >
          <div>
            <div className="mb-1.5 flex items-center justify-between">
              <label
                htmlFor="table-picker-name"
                className={UI_FORM_LABEL_CLASS}
              >
                Change table
              </label>
              <button
                type="button"
                onClick={handleBrowseCatalog}
                disabled={catalogQuery.isFetching}
                className={UI_LINK_BUTTON_CLASS}
              >
                {catalogQuery.isFetching
                  ? catalogQuery.data
                    ? "Refreshing…"
                    : "Loading…"
                  : "Browse catalog"}
              </button>
            </div>
            <input
              id="table-picker-name"
              type="text"
              required
              value={draftTableName}
              onChange={(event) => {
                setDraftTableName(event.target.value);
              }}
              placeholder="default.my_table"
              className={UI_TEXT_INPUT_CLASS}
              autoFocus
            />
            {isCatalogListOpen && catalogQuery.isError && (
              <p className={UI_ERROR_TEXT_SPACED_CLASS}>
                {catalogQuery.error.message}
              </p>
            )}
            <CatalogTableList
              tables={
                isCatalogListOpen ? (catalogQuery.data?.tables ?? null) : null
              }
              selectedName={draftTableName}
              onSelect={setDraftTableName}
              filter={catalogFilter}
              onFilterChange={setCatalogFilter}
              listClassName="max-h-40"
              includeNoneIcebergCatalogs={
                catalogQuery.data?.include_none_iceberg_catalogs ?? false
              }
            />
          </div>
          <button type="submit" className={UI_PRIMARY_BUTTON_SM_CLASS}>
            Continue
          </button>
        </form>
      )}
    </div>
  );
};
export default TablePicker;
