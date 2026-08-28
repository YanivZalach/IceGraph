import { useHotkey } from "@tanstack/react-hotkeys";
import { useEffect, useRef, useState } from "react";
import { UI_DOCS_BODY_CLASS, UI_DOCS_NAV_TITLE_CLASS } from "../uiTypography";
import DocsSearchOverlay from "../features/docs/components/DocsSearchOverlay";
import MarkdownContent from "../features/docs/components/MarkdownContent";
import {
  buildSearchResults,
  type Highlight,
  OVERVIEW_SECTION,
  type SearchResult,
  SECTIONS,
} from "../features/docs/docs";

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

  useHotkey(
    { key: "K" },
    () => {
      setSearchOpen(true);
    },
    { preventDefault: true },
  );
  useHotkey("Escape", closeSearch, { enabled: searchOpen });

  const selectSection = (sectionId: string) => {
    setActive(sectionId);
    setHighlight(null);
    contentRef.current?.scrollTo({ top: 0 });
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

  const searchResults = buildSearchResults(SECTIONS, query);

  useEffect(() => {
    const contentElement = contentRef.current;
    if (!contentElement || !highlight) return;

    for (const previousMark of contentElement.querySelectorAll(
      "mark[data-docs-search-highlight]",
    )) {
      previousMark.replaceWith(...previousMark.childNodes);
    }
    contentElement.normalize();

    const treeWalker = document.createTreeWalker(
      contentElement,
      NodeFilter.SHOW_TEXT,
    );
    const lowerTerm = highlight.term.toLowerCase();
    let occurrenceIndex = 0;
    let textNode = treeWalker.nextNode();

    while (textNode) {
      const text = textNode.nodeValue;
      if (text === null) {
        textNode = treeWalker.nextNode();
        continue;
      }
      const lowerText = text.toLowerCase();
      let matchIndex = lowerText.indexOf(lowerTerm);

      while (matchIndex !== -1) {
        if (occurrenceIndex === highlight.index) {
          const range = document.createRange();
          range.setStart(textNode, matchIndex);
          range.setEnd(textNode, matchIndex + highlight.term.length);
          const mark = document.createElement("mark");
          mark.dataset.docsSearchHighlight = "true";
          mark.className =
            "bg-accent text-white px-1 rounded ring-2 ring-white";
          range.surroundContents(mark);
          mark.scrollIntoView({ behavior: "smooth", block: "center" });
          return;
        }

        occurrenceIndex += 1;
        matchIndex = lowerText.indexOf(
          lowerTerm,
          matchIndex + lowerTerm.length,
        );
      }

      textNode = treeWalker.nextNode();
    }
  }, [highlight, active]);

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
              <kbd className="bg-surface-hover border border-[#3d4a5c] text-[#7dd3fc] text-xs font-mono px-2 py-0.5 rounded">
                k
              </kbd>
            </button>
          </div>

          <nav className="flex flex-col gap-0.5">
            {SECTIONS.map((section) => (
              <button
                key={section.id}
                onClick={() => {
                  selectSection(section.id);
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
              selectSection(e.target.value);
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
