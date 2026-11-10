import { useEffect, useRef, useState, type KeyboardEvent } from "react";
import { fileTypeLabel } from "../../../graphConstants";
import { cn } from "../../../shared/lib/cn";
import type { GraphNode } from "../model/graphModel";
import {
  MAX_LISTED_SEARCH_RESULTS,
  normalizeSearchQuery,
} from "../model/nodeSearch";
import { isFindShortcut, type GraphSearch } from "../hooks/useGraphSearch";

interface GraphSearchBoxProps {
  search: GraphSearch;
  selectedNodeId: string | null;
  onPick: (node: GraphNode) => void;
}

const HighlightedText = ({ text, query }: { text: string; query: string }) => {
  const needle = normalizeSearchQuery(query);
  const start = needle === "" ? -1 : text.toLowerCase().indexOf(needle);
  if (start < 0) return <>{text}</>;
  const end = start + needle.length;
  return (
    <>
      {text.slice(0, start)}
      <mark className="rounded-sm bg-yellow-300/80 text-slate-900">
        {text.slice(start, end)}
      </mark>
      {text.slice(end)}
    </>
  );
};

const GraphSearchBox = ({
  search,
  selectedNodeId,
  onPick,
}: GraphSearchBoxProps) => {
  const [activeIndex, setActiveIndex] = useState(0);
  const listRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const listedMatches = search.matches.slice(0, MAX_LISTED_SEARCH_RESULTS);
  const safeActiveIndex = Math.min(activeIndex, listedMatches.length - 1);

  useEffect(() => {
    const focusInput = (event: globalThis.KeyboardEvent): void => {
      if (!isFindShortcut(event)) return;
      event.preventDefault();
      inputRef.current?.focus();
      inputRef.current?.select();
    };
    window.addEventListener("keydown", focusInput);
    return () => {
      window.removeEventListener("keydown", focusInput);
    };
  }, []);

  const moveActive = (step: number): void => {
    if (listedMatches.length === 0) return;
    const nextIndex =
      (safeActiveIndex + step + listedMatches.length) % listedMatches.length;
    setActiveIndex(nextIndex);
    listRef.current?.children[nextIndex]?.scrollIntoView({ block: "nearest" });
  };

  const handleKeyDown = (event: KeyboardEvent<HTMLInputElement>): void => {
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      moveActive(event.key === "ArrowDown" ? 1 : -1);
    } else if (event.key === "Enter") {
      event.preventDefault();
      const node = listedMatches[safeActiveIndex];
      if (node !== undefined) onPick(node);
    } else if (event.key === "Escape") {
      event.preventDefault();
      search.close();
    }
  };

  return (
    <div className="relative w-full rounded-xl border border-edge bg-surface/80 p-2 shadow-2xl backdrop-blur-md">
      <input
        ref={inputRef}
        autoFocus
        type="search"
        value={search.query}
        placeholder="Find node by name, path, or type"
        className="w-full rounded-lg border border-edge bg-canvas px-3 py-2 text-sm text-ink focus:border-accent focus:outline-none"
        onChange={(event) => {
          setActiveIndex(0);
          search.changeQuery(event.target.value);
        }}
        onKeyDown={handleKeyDown}
      />
      {normalizeSearchQuery(search.query) !== "" && (
        <div className="absolute top-full right-0 left-0 mt-1 rounded-xl border border-edge bg-surface/90 p-2 shadow-2xl backdrop-blur-md">
          <div className="px-1 pb-1 text-xs text-slate-400">
            {search.matches.length === 0
              ? "No matching nodes"
              : `${String(search.matches.length)} matching nodes${search.matches.length > listedMatches.length ? `, showing first ${String(listedMatches.length)}` : ""} · ↑↓ Enter to go · Esc to close`}
          </div>
          <div
            ref={listRef}
            className="flex max-h-[50dvh] flex-col overflow-y-auto"
          >
            {listedMatches.map((node, index) => (
              <button
                type="button"
                key={node.id}
                className={cn(
                  "flex cursor-pointer flex-col items-start gap-0.5 rounded border-l-4 px-2 py-1.5 text-left transition hover:bg-surface-hover",
                  index === safeActiveIndex ? "bg-surface-hover" : "",
                  node.id === selectedNodeId
                    ? "border-accent"
                    : "border-transparent",
                )}
                onMouseDown={(event) => {
                  event.preventDefault();
                }}
                onClick={() => {
                  setActiveIndex(index);
                  onPick(node);
                }}
              >
                <span className="flex items-center gap-2 text-sm text-ink">
                  <span
                    className="size-2.5 shrink-0 rounded-full"
                    style={{ backgroundColor: node.color }}
                  />
                  <span className="text-xs uppercase tracking-wide text-slate-400">
                    <HighlightedText
                      text={fileTypeLabel(node.type)}
                      query={search.query}
                    />
                  </span>
                  <span className="break-all">
                    <HighlightedText text={node.label} query={search.query} />
                  </span>
                </span>
                {node.details.file_path !== undefined && (
                  <span className="break-all pl-4.5 font-mono text-xs text-slate-400">
                    <HighlightedText
                      text={node.details.file_path}
                      query={search.query}
                    />
                  </span>
                )}
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
};

export default GraphSearchBox;
