import { useHotkey } from "@tanstack/react-hotkeys";
import { useEffect, useRef } from "react";
import type {
  Dispatch,
  MouseEvent,
  ReactNode,
  RefObject,
  SetStateAction,
} from "react";
import type { SearchResult } from "../docs";

const escapeRegExp = (text: string): string =>
  text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const highlightMatch = (text: string, query: string): ReactNode => {
  const regularExpression = new RegExp(`(${escapeRegExp(query)})`, "gi");

  return text.split(regularExpression).map((part, index) =>
    part.toLowerCase() === query.toLowerCase() ? (
      <mark
        key={String(index)}
        className="bg-yellow-400 text-black px-1 rounded"
      >
        {part}
      </mark>
    ) : (
      part
    ),
  );
};

const SEARCH_RESULT_ITEM_CLASS = "w-full text-left p-4 border-b border-edge";

interface DocsSearchOverlayProps {
  query: string;
  searchResults: SearchResult[];
  selectedResultIndex: number;
  resultsContainerRef: RefObject<HTMLDivElement | null>;
  onClose: () => void;
  onQueryChange: Dispatch<SetStateAction<string>>;
  onResultSelect: (result: SearchResult) => void;
  onSelectedResultIndexChange: Dispatch<SetStateAction<number>>;
}

const DocsSearchOverlay = ({
  query,
  searchResults,
  selectedResultIndex,
  resultsContainerRef,
  onClose,
  onQueryChange,
  onResultSelect,
  onSelectedResultIndexChange,
}: DocsSearchOverlayProps) => {
  const lastMousePosition = useRef({ x: -1, y: -1 });
  const searchInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    resultsContainerRef.current?.children[selectedResultIndex]?.scrollIntoView({
      block: "nearest",
    });
  }, [resultsContainerRef, selectedResultIndex]);

  useHotkey(
    "Control+N",
    () => {
      onSelectedResultIndexChange((index) =>
        Math.min(index + 1, searchResults.length - 1),
      );
    },
    {
      target: searchInputRef,
      enabled: searchResults.length > 0,
      preventDefault: true,
    },
  );
  useHotkey(
    "Control+P",
    () => {
      onSelectedResultIndexChange((index) => Math.max(index - 1, 0));
    },
    {
      target: searchInputRef,
      enabled: searchResults.length > 0,
      preventDefault: true,
    },
  );
  useHotkey(
    "Enter",
    () => {
      const result = searchResults[selectedResultIndex];
      if (result) onResultSelect(result);
    },
    {
      target: searchInputRef,
      enabled: searchResults.length > 0,
      preventDefault: true,
    },
  );

  const handleQueryChange = (value: string) => {
    onQueryChange(value);
    onSelectedResultIndexChange(0);
  };

  const handleResultMouseMove = (
    event: MouseEvent<HTMLButtonElement>,
    index: number,
  ) => {
    if (
      event.clientX === lastMousePosition.current.x &&
      event.clientY === lastMousePosition.current.y
    ) {
      return;
    }

    lastMousePosition.current = { x: event.clientX, y: event.clientY };
    onSelectedResultIndexChange(index);
  };

  return (
    <div
      className="fixed inset-0 bg-black/50 z-50 flex justify-center items-start pt-20 px-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-4xl h-[80vh] bg-surface-deep border border-edge rounded-lg shadow-xl flex flex-col"
        onClick={(event) => {
          event.stopPropagation();
        }}
      >
        <input
          ref={searchInputRef}
          autoFocus
          value={query}
          onChange={(event) => {
            handleQueryChange(event.target.value);
          }}
          placeholder="Search documentation..."
          className="w-full px-4 py-3 bg-surface-hover text-white outline-none rounded-t-lg shrink-0"
        />

        {query && (
          <div className="border-t border-edge flex-1 min-h-0">
            {searchResults.length > 0 ? (
              <div ref={resultsContainerRef} className="h-full overflow-y-auto">
                {searchResults.map((result, index) => (
                  <button
                    key={`${result.section.id}-${String(result.occurrenceIndex)}`}
                    onMouseMove={(event) => {
                      handleResultMouseMove(event, index);
                    }}
                    onClick={() => {
                      onResultSelect(result);
                    }}
                    className={`${SEARCH_RESULT_ITEM_CLASS} ${
                      index === selectedResultIndex ? "bg-surface-hover" : ""
                    }`}
                  >
                    <div className="text-white font-semibold text-lg">
                      {result.section.title}
                    </div>

                    <div className="text-accent text-xs mt-1">
                      Found in: {result.section.title}
                      {result.totalInSection > 1 &&
                        ` - match ${String(result.occurrenceIndex + 1)} of ${String(result.totalInSection)}`}
                    </div>

                    <div className="text-slate-400 text-sm mt-2 leading-relaxed">
                      {highlightMatch(result.snippet, query)}
                    </div>
                  </button>
                ))}
              </div>
            ) : (
              <div className="p-4 text-slate-400">No results found.</div>
            )}
          </div>
        )}
      </div>
    </div>
  );
};

export default DocsSearchOverlay;
