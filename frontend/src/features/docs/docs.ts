import { APP_VERSION, BASE_PATH } from "../../appConstants";
import clientCliMarkdown from "./content/client-cli.md?raw";
import codingAgentMarkdown from "./content/coding-agent.md?raw";
import fileTreeViewMarkdown from "./content/file-tree-view.md?raw";
import graphViewMarkdown from "./content/graph-view.md?raw";
import issuesPanelMarkdown from "./content/issues-panel.md?raw";
import keyboardShortcutsMarkdown from "./content/keyboard-shortcuts.md?raw";
import loadingTableMarkdown from "./content/loading-table.md?raw";
import metadataViewMarkdown from "./content/metadata-view.md?raw";
import overviewMarkdown from "./content/overview.md?raw";
import specsPanelMarkdown from "./content/specs-panel.md?raw";
import timelineViewMarkdown from "./content/timeline-view.md?raw";
import tipsMarkdown from "./content/tips.md?raw";

interface DocsSection {
  id: string;
  title: string;
  markdown: string;
}

export interface SearchResult {
  section: DocsSection;
  occurrenceIndex: number;
  totalInSection: number;
  snippet: string;
}

export interface Highlight {
  sectionId: string;
  term: string;
  index: number;
}

const pipInstallCommand =
  APP_VERSION === "dev"
    ? "pip install icegraph-client"
    : `pip install icegraph-client==${APP_VERSION.replace(/^v/, "")}`;

const resolvePlaceholders = (markdown: string): string =>
  markdown
    .replaceAll("{{APP_VERSION}}", APP_VERSION)
    .replaceAll("{{PIP_INSTALL_COMMAND}}", pipInstallCommand)
    .replaceAll("{{BASE_PATH}}", BASE_PATH);

export const OVERVIEW_SECTION: DocsSection = {
  id: "overview",
  title: "Overview",
  markdown: resolvePlaceholders(overviewMarkdown),
};

export const SECTIONS: DocsSection[] = [
  OVERVIEW_SECTION,
  {
    id: "claude-code",
    title: "Connect to Coding Agent",
    markdown: resolvePlaceholders(codingAgentMarkdown),
  },
  {
    id: "loading-a-table",
    title: "Loading a Table",
    markdown: resolvePlaceholders(loadingTableMarkdown),
  },
  {
    id: "timeline-view",
    title: "Timeline View",
    markdown: resolvePlaceholders(timelineViewMarkdown),
  },
  {
    id: "metadata-view",
    title: "Metadata View",
    markdown: resolvePlaceholders(metadataViewMarkdown),
  },
  {
    id: "filetree-view",
    title: "FileTree View",
    markdown: resolvePlaceholders(fileTreeViewMarkdown),
  },
  {
    id: "graph-view",
    title: "Graph View",
    markdown: resolvePlaceholders(graphViewMarkdown),
  },
  {
    id: "specs-panel",
    title: "Specs Panel",
    markdown: resolvePlaceholders(specsPanelMarkdown),
  },
  {
    id: "issues-panel",
    title: "Issues Panel",
    markdown: resolvePlaceholders(issuesPanelMarkdown),
  },
  {
    id: "keyboard-shortcuts",
    title: "Keyboard Shortcuts",
    markdown: resolvePlaceholders(keyboardShortcutsMarkdown),
  },
  {
    id: "tips",
    title: "Tips & Tricks",
    markdown: resolvePlaceholders(tipsMarkdown),
  },
  {
    id: "cli",
    title: "CLI & Python Client",
    markdown: resolvePlaceholders(clientCliMarkdown),
  },
];

const findAllIndices = (text: string, query: string): number[] => {
  const indices = [];
  const lowerText = text.toLowerCase();
  const lowerQuery = query.toLowerCase();
  let from = 0;
  let index = lowerText.indexOf(lowerQuery, from);

  while (index !== -1) {
    indices.push(index);
    from = index + lowerQuery.length;
    index = lowerText.indexOf(lowerQuery, from);
  }

  return indices;
};

const markdownToSearchText = (markdown: string): string =>
  markdown
    .replace(/```[^\n]*\n/g, "")
    .replace(/```/g, "")
    .replace(/\[([^\]]+)]\([^)]+\)/g, "$1")
    .replace(/[*#`]/g, "")
    .replace(/^\s*-\s+/gm, "")
    .replace(/\s+/g, " ")
    .trim();

export const buildSearchResults = (
  sections: DocsSection[],
  query: string,
): SearchResult[] => {
  if (!query) return [];

  return sections.flatMap((section) => {
    const content = markdownToSearchText(section.markdown);
    const contentMatches = findAllIndices(content, query);

    return contentMatches.map((matchIndex, occurrenceIndex) => {
      const snippetStart = Math.max(0, matchIndex - 40);

      return {
        section,
        snippet: content.substring(snippetStart, snippetStart + 140),
        occurrenceIndex,
        totalInSection: contentMatches.length,
      };
    });
  });
};
