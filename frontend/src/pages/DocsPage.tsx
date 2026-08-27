import { useEffect, useRef, useState } from "react";
import { UI_DOCS_BODY_CLASS, UI_DOCS_NAV_TITLE_CLASS } from "../uiTypography";
import DocsSearchOverlay from "../features/docs/components/DocsSearchOverlay";
import Key from "../features/docs/components/Key";
import { OVERVIEW_SECTION, SECTIONS } from "../features/docs/docsSections";
import {
  extractText,
  findAllIndices,
  highlightTreeMatches,
} from "../features/docs/docsSearch";
import type { Highlight, SearchResult } from "../features/docs/docsTypes";

const DocsPage = () => {
  const [active, setActive] = useState(OVERVIEW_SECTION.id);
  const [searchOpen, setSearchOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [highlight, setHighlight] = useState<Highlight | null>(null);
  const [selectedResultIndex, setSelectedResultIndex] = useState(0);
  const contentRef = useRef<HTMLDivElement>(null);
  const resultsContainerRef = useRef<HTMLDivElement>(null);

  const closeSearch = () => {
    setSearchOpen(false);
    setQuery("");
  };

  const selectResult = (result: SearchResult) => {
    setActive(result.section.id);
    setHighlight({
      sectionId: result.section.id,
      term: query,
      index: result.occurrenceIndex,
    });
    closeSearch();
  };

  const searchResults: SearchResult[] = query
    ? SECTIONS.flatMap((section) => {
        const content = extractText(section.body);
        const contentMatches = findAllIndices(content, query);

        return contentMatches.map((matchIndex, occurrenceIndex) => {
          const snippetStart = Math.max(0, matchIndex - 40);
          const snippet = content.substring(snippetStart, snippetStart + 140);

          return {
            section,
            snippet,
            occurrenceIndex,
            totalInSection: contentMatches.length,
          };
        });
      })
    : [];

  useEffect(() => {
    if (contentRef.current) contentRef.current.scrollTop = 0;
  }, [active]);

  useEffect(() => {
    const activeMark = contentRef.current?.querySelector(
      '[data-active-match="true"]',
    );
    if (highlight?.index != null && activeMark) {
      activeMark.scrollIntoView({
        behavior: "smooth",
        block: "center",
      });
    }
  }, [highlight, active]);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        closeSearch();
        return;
      }

      if (
        event.target instanceof HTMLElement &&
        (["INPUT", "TEXTAREA", "SELECT"].includes(event.target.tagName) ||
          event.target.isContentEditable)
      )
        return;

      if (event.key === "k") {
        event.preventDefault();
        setSearchOpen(true);
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, []);

  const activeSection =
    SECTIONS.find((section) => section.id === active) ?? OVERVIEW_SECTION;

  return (
    <div className="flex flex-1 overflow-hidden">
      {searchOpen && (
        <DocsSearchOverlay
          query={query}
          searchResults={searchResults}
          selectedResultIndex={selectedResultIndex}
          resultsContainerRef={resultsContainerRef}
          onClose={closeSearch}
          onQueryChange={setQuery}
          onResultSelect={selectResult}
          onSelectedResultIndexChange={setSelectedResultIndex}
        />
      )}
      <aside className="w-52 shrink-0 bg-[#151b26] border-r border-edge overflow-y-auto hidden sm:block">
        <div className="px-4 py-5">
          <div className="mb-4">
            <p className={UI_DOCS_NAV_TITLE_CLASS}>Documentation</p>

            <button
              onClick={() => {
                setSearchOpen(true);
              }}
              className="w-full flex items-center justify-between rounded-md border border-edge bg-surface px-3 py-2 text-sm text-slate-400 hover:text-white hover:bg-surface-hover transition"
            >
              <span>Search docs...</span>
              <Key k="k" />
            </button>
          </div>

          <nav className="flex flex-col gap-0.5">
            {SECTIONS.map((section) => (
              <button
                key={section.id}
                onClick={() => {
                  setActive(section.id);
                  setHighlight(null);
                }}
                className={`text-left text-sm px-3 py-2 rounded-md transition ${
                  active === section.id
                    ? "bg-accent-muted text-white font-medium"
                    : "text-slate-400 hover:text-white hover:bg-surface-hover"
                }`}
              >
                {section.title}
              </button>
            ))}
          </nav>
        </div>
      </aside>

      <div className="flex-1 overflow-y-auto" ref={contentRef}>
        <div className="sm:hidden px-4 pt-4 pb-2">
          <select
            value={active}
            onChange={(e) => {
              setActive(e.target.value);
              setHighlight(null);
            }}
            className="w-full bg-surface-hover text-white text-sm border border-edge rounded-md px-3 py-2"
          >
            {SECTIONS.map((section) => (
              <option key={section.id} value={section.id}>
                {section.title}
              </option>
            ))}
          </select>
        </div>

        <div className="max-w-3xl mx-auto px-6 py-8">
          <h1 className="text-2xl font-bold text-white mb-6">
            {activeSection.title}
          </h1>
          <div className={UI_DOCS_BODY_CLASS}>
            {highlight?.sectionId === activeSection.id
              ? highlightTreeMatches(activeSection.body, highlight.term, {
                  count: 0,
                  target: highlight.index,
                  key: 0,
                })
              : activeSection.body}
          </div>
        </div>
      </div>
    </div>
  );
};

export default DocsPage;
