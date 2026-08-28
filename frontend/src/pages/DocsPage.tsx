import { useHotkey } from "@tanstack/react-hotkeys";
import { useNavigate, useSearch } from "@tanstack/react-router";
import { useEffect, useRef, useState } from "react";
import { UI_DOCS_BODY_CLASS, UI_DOCS_NAV_TITLE_CLASS } from "../uiTypography";
import DocsSearchOverlay from "../features/docs/components/DocsSearchOverlay";
import MarkdownContent from "../features/docs/components/MarkdownContent";
import { highlightSearchResult } from "../features/docs/docsHighlight";
import {
  buildSearchResults,
  type Highlight,
  OVERVIEW_SECTION,
  type SearchResult,
  DOC_SECTIONS,
} from "../features/docs/docs";

const DocsPage = () => {
  const navigate = useNavigate({ from: "/docs" });
  const { section: sectionId } = useSearch({ from: "/docs" });
  const [searchOpen, setSearchOpen] = useState(false);
  const [query, setQuery] = useState("");
  const [highlightRequest, setHighlightRequest] = useState(0);
  const [selectedResultIndex, setSelectedResultIndex] = useState(0);
  const pendingHighlightRef = useRef<Highlight | null>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const markdownContentRef = useRef<HTMLDivElement>(null);
  const resultsContainerRef = useRef<HTMLDivElement>(null);

  const closeSearch = () => {
    setSearchOpen(false);
    setQuery("");
  };

  useHotkey(
    { key: "K" },
    () => {
      setSearchOpen(true);
    },
    { preventDefault: true },
  );
  useHotkey("Escape", closeSearch, { enabled: searchOpen });

  const selectSection = (sectionId: string) => {
    void navigate({
      search: (previousSearch) => ({ ...previousSearch, section: sectionId }),
    });
    pendingHighlightRef.current = null;
    contentRef.current?.scrollTo({ top: 0 });
  };

  const selectResult = (result: SearchResult) => {
    void navigate({
      search: (previousSearch) => ({
        ...previousSearch,
        section: result.section.id,
      }),
    });
    pendingHighlightRef.current = {
      sectionId: result.section.id,
      term: query,
      index: result.occurrenceIndex,
    };
    setHighlightRequest((request) => request + 1);
    closeSearch();
  };

  const searchResults = buildSearchResults(DOC_SECTIONS, query);

  useEffect(() => {
    const highlight = pendingHighlightRef.current;
    const contentElement = markdownContentRef.current;
    if (!contentElement || !highlight || highlight.sectionId !== sectionId) {
      return;
    }

    highlightSearchResult(contentElement, highlight);
    pendingHighlightRef.current = null;
  }, [highlightRequest, sectionId]);

  const activeSection =
    DOC_SECTIONS.find((section) => section.id === sectionId) ??
    OVERVIEW_SECTION;

  useEffect(() => {
    contentRef.current?.scrollTo({ top: 0 });
  }, [activeSection.id]);

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
              <kbd className="bg-surface-hover border border-[#3d4a5c] text-[#7dd3fc] text-xs font-mono px-2 py-0.5 rounded">
                k
              </kbd>
            </button>
          </div>

          <nav className="flex flex-col gap-0.5">
            {DOC_SECTIONS.map((section) => (
              <button
                key={section.id}
                onClick={() => {
                  selectSection(section.id);
                }}
                className={`text-left text-sm px-3 py-2 rounded-md transition ${
                  activeSection.id === section.id
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
            value={activeSection.id}
            onChange={(e) => {
              selectSection(e.target.value);
            }}
            className="w-full bg-surface-hover text-white text-sm border border-edge rounded-md px-3 py-2"
          >
            {DOC_SECTIONS.map((section) => (
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
          <div ref={markdownContentRef} className={UI_DOCS_BODY_CLASS}>
            <MarkdownContent
              markdown={activeSection.markdown}
              sectionId={activeSection.id}
            />
          </div>
        </div>
      </div>
    </div>
  );
};

export default DocsPage;
