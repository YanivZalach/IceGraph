import { useDeferredValue, useRef, useState, type KeyboardEvent } from "react";
import { useCatalogTables } from "./useCatalogTables";
import { readRecentTables } from "./recentTables";
import { buildTableOptions, optionKey, type TableOption } from "./tableOptions";
import TableOptionList from "./TableOptionList";
import TableSpotlightFooter from "./TableSpotlightFooter";
import { cn } from "../../shared/lib/cn";

interface TableSpotlightPanelProps {
  inputId: string;
  onOpenTable: (tableName: string) => void;
  onClose?: () => void;
  className?: string;
  resultsClassName: string;
}

const TableSpotlightPanel = ({
  inputId,
  onOpenTable,
  onClose,
  className,
  resultsClassName,
}: TableSpotlightPanelProps) => {
  const [recentTables] = useState(readRecentTables);
  const [query, setQuery] = useState("");
  const [highlightedKey, setHighlightedKey] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const deferredQuery = useDeferredValue(query);
  const catalogTables = useCatalogTables();
  const catalogTableNames = catalogTables.catalog?.data.tables ?? [];
  const { options, hiddenCount, groupCounts } = buildTableOptions(
    deferredQuery,
    recentTables,
    catalogTableNames,
  );
  const minimumIndex = deferredQuery.trim() === "" ? -1 : 0;
  const highlightedIndex = Math.max(
    options.findIndex((option) => optionKey(option) === highlightedKey),
    Math.min(minimumIndex, options.length - 1),
  );
  const highlightedOption = options[highlightedIndex];
  const listId = `${inputId}-tables`;
  const optionId = (index: number): string => `${listId}-${String(index)}`;

  const changeQuery = (nextQuery: string): void => {
    setQuery(nextQuery);
    setHighlightedKey(null);
  };

  const handleOpen = (option: TableOption): void => {
    onOpenTable(option.name);
  };

  const handleKeyDown = (event: KeyboardEvent<HTMLInputElement>): void => {
    if (event.nativeEvent.isComposing) return;
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      const step = event.key === "ArrowDown" ? 1 : -1;
      const nextIndex = Math.min(
        Math.max(highlightedIndex + step, minimumIndex),
        options.length - 1,
      );
      const nextOption = options[nextIndex];
      setHighlightedKey(
        nextOption === undefined ? null : optionKey(nextOption),
      );
      document
        .getElementById(optionId(nextIndex))
        ?.scrollIntoView({ block: "nearest" });
    } else if (event.key === "Enter") {
      event.preventDefault();
      const option =
        query === deferredQuery
          ? highlightedOption
          : buildTableOptions(query, recentTables, catalogTableNames)
              .options[0];
      if (option !== undefined) handleOpen(option);
    } else if (event.key === "Escape") {
      event.preventDefault();
      event.stopPropagation();
      if (query !== "") changeQuery("");
      else onClose?.();
    }
  };

  return (
    <div
      className={cn(
        "flex flex-col overflow-hidden rounded-xl border border-edge bg-surface-deep",
        className,
      )}
      onMouseDown={(event) => {
        if (event.target !== inputRef.current) event.preventDefault();
      }}
    >
      <div className="flex shrink-0 items-center gap-3 border-b border-edge px-4">
        <svg
          viewBox="0 0 24 24"
          className="h-4 w-4 shrink-0 text-slate-400"
          fill="none"
          stroke="currentColor"
          strokeWidth={2}
          strokeLinecap="round"
          aria-hidden="true"
        >
          <circle cx="11" cy="11" r="7" />
          <path d="m20 20-3.5-3.5" />
        </svg>
        <input
          ref={inputRef}
          id={inputId}
          type="text"
          role="combobox"
          aria-label="Search tables"
          aria-expanded="true"
          aria-controls={listId}
          aria-autocomplete="list"
          aria-activedescendant={
            highlightedOption === undefined
              ? undefined
              : optionId(highlightedIndex)
          }
          autoComplete="off"
          spellCheck={false}
          value={query}
          onChange={(event) => {
            changeQuery(event.target.value);
          }}
          onKeyDown={handleKeyDown}
          placeholder="Search tables or type a full name"
          className="min-w-0 flex-1 bg-transparent py-3.5 text-base text-ink placeholder-slate-500 outline-none"
          autoFocus
        />
        {query !== "" && (
          <button
            type="button"
            onClick={() => {
              changeQuery("");
              inputRef.current?.focus();
            }}
            aria-label="Clear search"
            className="shrink-0 rounded p-1 text-slate-400 transition hover:bg-edge hover:text-ink"
          >
            <svg
              viewBox="0 0 24 24"
              className="h-3.5 w-3.5"
              fill="none"
              stroke="currentColor"
              strokeWidth={2}
              strokeLinecap="round"
              aria-hidden="true"
            >
              <path d="M6 6l12 12M18 6 6 18" />
            </svg>
          </button>
        )}
      </div>
      <TableOptionList
        listId={listId}
        options={options}
        highlightedIndex={highlightedIndex}
        hiddenCount={hiddenCount}
        groupCounts={groupCounts}
        isLoading={catalogTables.isLoading}
        optionId={optionId}
        onHighlight={(option) => {
          setHighlightedKey(optionKey(option));
        }}
        onOpen={handleOpen}
        className={resultsClassName}
      />
      <TableSpotlightFooter
        catalogTables={catalogTables}
        escapeHint={onClose === undefined ? "clear" : "close"}
        onRefresh={() => {
          inputRef.current?.focus();
          catalogTables.refresh();
        }}
      />
    </div>
  );
};

export default TableSpotlightPanel;
