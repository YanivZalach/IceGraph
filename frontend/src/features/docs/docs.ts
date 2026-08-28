import type { MDXComponents } from "mdx/types";
import type { ComponentType } from "react";
import { APP_VERSION } from "../../appConstants";
import { PIP_INSTALL_COMMAND } from "./docsContentValues";
import ClientCliContent, {
  searchText as clientCliText,
} from "./content/client-cli.mdx";
import CodingAgentContent, {
  searchText as codingAgentText,
} from "./content/coding-agent.mdx";
import FileTreeViewContent, {
  searchText as fileTreeViewText,
} from "./content/file-tree-view.mdx";
import GraphViewContent, {
  searchText as graphViewText,
} from "./content/graph-view.mdx";
import IssuesPanelContent, {
  searchText as issuesPanelText,
} from "./content/issues-panel.mdx";
import KeyboardShortcutsContent, {
  searchText as keyboardShortcutsText,
} from "./content/keyboard-shortcuts.mdx";
import LoadingTableContent, {
  searchText as loadingTableText,
} from "./content/loading-table.mdx";
import MetadataViewContent, {
  searchText as metadataViewText,
} from "./content/metadata-view.mdx";
import OverviewContent, {
  searchText as overviewText,
} from "./content/overview.mdx";
import SpecsPanelContent, {
  searchText as specsPanelText,
} from "./content/specs-panel.mdx";
import TimelineViewContent, {
  searchText as timelineViewText,
} from "./content/timeline-view.mdx";
import TipsContent, { searchText as tipsText } from "./content/tips.mdx";

interface DocsSection {
  id: string;
  title: string;
  Content: ComponentType<{ components?: MDXComponents }>;
  searchText: string;
}

const createSection = (
  id: string,
  title: string,
  Content: ComponentType<{ components?: MDXComponents }>,
  searchText: string,
): DocsSection => ({
  id,
  title,
  Content,
  searchText,
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

export const OVERVIEW_SECTION = createSection(
  "overview",
  "Overview",
  OverviewContent,
  overviewText.replace("Version", `Version ${APP_VERSION}`),
);

export const DOC_SECTIONS: DocsSection[] = [
  OVERVIEW_SECTION,
  createSection(
    "coding-agent",
    "Connect to Coding Agent",
    CodingAgentContent,
    codingAgentText,
  ),
  createSection(
    "loading-a-table",
    "Loading a Table",
    LoadingTableContent,
    loadingTableText,
  ),
  createSection(
    "timeline-view",
    "Timeline View",
    TimelineViewContent,
    timelineViewText,
  ),
  createSection(
    "metadata-view",
    "Metadata View",
    MetadataViewContent,
    metadataViewText,
  ),
  createSection(
    "filetree-view",
    "FileTree View",
    FileTreeViewContent,
    fileTreeViewText,
  ),
  createSection("graph-view", "Graph View", GraphViewContent, graphViewText),
  createSection(
    "specs-panel",
    "Specs Panel",
    SpecsPanelContent,
    specsPanelText,
  ),
  createSection(
    "issues-panel",
    "Issues Panel",
    IssuesPanelContent,
    issuesPanelText,
  ),
  createSection(
    "keyboard-shortcuts",
    "Keyboard Shortcuts",
    KeyboardShortcutsContent,
    keyboardShortcutsText,
  ),
  createSection("tips", "Tips & Tricks", TipsContent, tipsText),
  createSection(
    "cli",
    "CLI & Python Client",
    ClientCliContent,
    `${clientCliText} ${PIP_INSTALL_COMMAND}`,
  ),
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

export const buildSearchResults = (
  sections: DocsSection[],
  query: string,
): SearchResult[] => {
  if (!query) return [];

  return sections.flatMap((section) => {
    const content = section.searchText;
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
