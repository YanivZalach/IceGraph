import { APP_VERSION } from "../../appConstants";
import type { MDXComponents } from "mdx/types";
import type { ComponentType } from "react";
import ClientCliContent from "./content/client-cli.mdx";
import clientCliSource from "./content/client-cli.mdx?raw";
import CodingAgentContent from "./content/coding-agent.mdx";
import codingAgentSource from "./content/coding-agent.mdx?raw";
import FileTreeViewContent from "./content/file-tree-view.mdx";
import fileTreeViewSource from "./content/file-tree-view.mdx?raw";
import GraphViewContent from "./content/graph-view.mdx";
import graphViewSource from "./content/graph-view.mdx?raw";
import IssuesPanelContent from "./content/issues-panel.mdx";
import issuesPanelSource from "./content/issues-panel.mdx?raw";
import KeyboardShortcutsContent from "./content/keyboard-shortcuts.mdx";
import keyboardShortcutsSource from "./content/keyboard-shortcuts.mdx?raw";
import LoadingTableContent from "./content/loading-table.mdx";
import loadingTableSource from "./content/loading-table.mdx?raw";
import MetadataViewContent from "./content/metadata-view.mdx";
import metadataViewSource from "./content/metadata-view.mdx?raw";
import OverviewContent from "./content/overview.mdx";
import overviewSource from "./content/overview.mdx?raw";
import SpecsPanelContent from "./content/specs-panel.mdx";
import specsPanelSource from "./content/specs-panel.mdx?raw";
import TimelineViewContent from "./content/timeline-view.mdx";
import timelineViewSource from "./content/timeline-view.mdx?raw";
import TipsContent from "./content/tips.mdx";
import tipsSource from "./content/tips.mdx?raw";

interface DocsSection {
  id: string;
  title: string;
  Content: ComponentType<{ components?: MDXComponents }>;
  source: string;
}

const createSection = (
  id: string,
  title: string,
  Content: ComponentType<{ components?: MDXComponents }>,
  source: string,
): DocsSection => ({
  id,
  title,
  Content,
  source,
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

const resolveSearchComponents = (source: string): string =>
  source
    .replaceAll("<AppVersion />", APP_VERSION)
    .replaceAll("<PipInstallCommand />", pipInstallCommand)
    .replaceAll("<SkillDownloadLink>", "")
    .replaceAll("</SkillDownloadLink>", "")
    .replaceAll("<CriticalHeading>", "")
    .replaceAll("</CriticalHeading>", "")
    .replaceAll("<WarningHeading>", "")
    .replaceAll("</WarningHeading>", "");

export const OVERVIEW_SECTION = createSection(
  "overview",
  "Overview",
  OverviewContent,
  overviewSource,
);

/** Ordered documentation pages shown in the sidebar and searchable index. */
export const DOC_SECTIONS: DocsSection[] = [
  OVERVIEW_SECTION,
  createSection(
    "coding-agent",
    "Connect to Coding Agent",
    CodingAgentContent,
    codingAgentSource,
  ),
  createSection(
    "loading-a-table",
    "Loading a Table",
    LoadingTableContent,
    loadingTableSource,
  ),
  createSection(
    "timeline-view",
    "Timeline View",
    TimelineViewContent,
    timelineViewSource,
  ),
  createSection(
    "metadata-view",
    "Metadata View",
    MetadataViewContent,
    metadataViewSource,
  ),
  createSection(
    "filetree-view",
    "FileTree View",
    FileTreeViewContent,
    fileTreeViewSource,
  ),
  createSection("graph-view", "Graph View", GraphViewContent, graphViewSource),
  createSection(
    "specs-panel",
    "Specs Panel",
    SpecsPanelContent,
    specsPanelSource,
  ),
  createSection(
    "issues-panel",
    "Issues Panel",
    IssuesPanelContent,
    issuesPanelSource,
  ),
  createSection(
    "keyboard-shortcuts",
    "Keyboard Shortcuts",
    KeyboardShortcutsContent,
    keyboardShortcutsSource,
  ),
  createSection("tips", "Tips & Tricks", TipsContent, tipsSource),
  createSection(
    "cli",
    "CLI & Python Client",
    ClientCliContent,
    clientCliSource,
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

const mdxToSearchText = (source: string): string => {
  const withoutCodeBlocks = source
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
    const content = mdxToSearchText(resolveSearchComponents(section.source));
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
