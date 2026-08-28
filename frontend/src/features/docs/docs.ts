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

const createSection = (
  id: string,
  title: string,
  markdown: string,
): DocsSection => ({
  id,
  title,
  markdown: resolvePlaceholders(markdown),
});

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

/** Ordered documentation pages shown in the sidebar and searchable index. */
export const DOC_SECTIONS: DocsSection[] = [
  OVERVIEW_SECTION,
  createSection("claude-code", "Connect to Coding Agent", codingAgentMarkdown),
  createSection("loading-a-table", "Loading a Table", loadingTableMarkdown),
  createSection("timeline-view", "Timeline View", timelineViewMarkdown),
  createSection("metadata-view", "Metadata View", metadataViewMarkdown),
  createSection("filetree-view", "FileTree View", fileTreeViewMarkdown),
  createSection("graph-view", "Graph View", graphViewMarkdown),
  createSection("specs-panel", "Specs Panel", specsPanelMarkdown),
  createSection("issues-panel", "Issues Panel", issuesPanelMarkdown),
  createSection(
    "keyboard-shortcuts",
    "Keyboard Shortcuts",
    keyboardShortcutsMarkdown,
  ),
  createSection("tips", "Tips & Tricks", tipsMarkdown),
  createSection("cli", "CLI & Python Client", clientCliMarkdown),
];

const findAllIndices = (text: string, query: string): number[] => {
  const indices: number[] = [];
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

const markdownToSearchText = (markdown: string): string => {
  const withoutCodeBlocks = markdown
    .replace(/```[^\n]*\n/g, "")
    .replace(/```/g, "");
  const withoutLinks = withoutCodeBlocks.replace(/\[([^\]]+)]\([^)]+\)/g, "$1");
  const withoutMarkdownSyntax = withoutLinks
    .replace(/[*#`]/g, "")
    .replace(/^\s*-\s+/gm, "");

  return withoutMarkdownSyntax.replace(/\s+/g, " ").trim();
};

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
