import { useEffect, useRef } from "react";
import type { Dispatch, RefObject, SetStateAction } from "react";
import { highlightMatch } from "../docsSearch";
import type { SearchResult } from "../docsTypes";

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
  useEffect(() => {
    resultsContainerRef.current?.children[selectedResultIndex]?.scrollIntoView({
      block: "nearest",
    });
  }, [resultsContainerRef, selectedResultIndex]);
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
          autoFocus
          value={query}
          onChange={(e) => {
            onQueryChange(e.target.value);
            onSelectedResultIndexChange(0);
          }}
          onKeyDown={(e) => {
            if (e.ctrlKey && e.key === "n") {
              e.preventDefault();
              onSelectedResultIndexChange((i) =>
                Math.min(i + 1, searchResults.length - 1),
              );
              return;
            }

            if (e.ctrlKey && e.key === "p") {
              e.preventDefault();
              onSelectedResultIndexChange((i) => Math.max(i - 1, 0));
              return;
            }

            if (e.key === "Enter") {
              const result = searchResults[selectedResultIndex];
              if (!result) return;
              e.preventDefault();
              onResultSelect(result);
            }
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
                    onMouseMove={(e) => {
                      if (
                        e.clientX === lastMousePosition.current.x &&
                        e.clientY === lastMousePosition.current.y
                      )
                        return;
                      lastMousePosition.current = {
                        x: e.clientX,
                        y: e.clientY,
                      };
                      onSelectedResultIndexChange(index);
                    }}
                    onClick={() => {
                      onResultSelect(result);
                    }}
                    className={`w-full text-left p-4 border-b border-edge ${
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
