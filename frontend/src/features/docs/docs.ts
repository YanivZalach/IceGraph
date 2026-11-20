import type { MDXComponents } from "mdx/types";
import type { ComponentType } from "react";
import { APP_VERSION } from "../../appConstants";
import { PIP_INSTALL_COMMAND } from "./docsContentValues";
import AiAssistantContent, {
  searchText as aiAssistantText,
} from "./content/ai-assistant.mdx";
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
import PartitionFilesContent, {
  searchText as partitionFilesText,
} from "./content/partition-files.mdx";
import PartitionFilesFileTreeContent, {
  searchText as partitionFilesFileTreeText,
} from "./content/partition-files-filetree.mdx";
import PartitionFilesGraphContent, {
  searchText as partitionFilesGraphText,
} from "./content/partition-files-graph.mdx";
import AiAssistantShortcuts, {
  searchText as aiAssistantShortcutsText,
} from "./content/shortcuts/ai-assistant.mdx";
import GraphViewShortcuts, {
  searchText as graphViewShortcutsText,
} from "./content/shortcuts/graph-view.mdx";
import MetadataViewShortcuts, {
  searchText as metadataViewShortcutsText,
} from "./content/shortcuts/metadata-view.mdx";
import PartitionFilesShortcuts, {
  searchText as partitionFilesShortcutsText,
} from "./content/shortcuts/partition-files.mdx";
import TimelineViewShortcuts, {
  searchText as timelineViewShortcutsText,
} from "./content/shortcuts/timeline-view.mdx";
import SpecsPanelContent, {
  searchText as specsPanelText,
} from "./content/specs-panel.mdx";
import TimelineViewContent, {
  searchText as timelineViewText,
} from "./content/timeline-view.mdx";
import TipsContent, { searchText as tipsText } from "./content/tips.mdx";

type MdxComponent = ComponentType<{ components?: MDXComponents }>;

export const DOC_GROUPS = [
  "Getting Started",
  "Table Views",
  "Partition Files",
  "General",
] as const;

type DocsGroup = (typeof DOC_GROUPS)[number];

interface DocsSection {
  id: string;
  title: string;
  group: DocsGroup;
  Content: MdxComponent;
  Shortcuts?: MdxComponent;
  searchText: string;
}

interface DocsSectionInput {
  id: string;
  title: string;
  group: DocsGroup;
  Content: MdxComponent;
  text: string;
  Shortcuts?: MdxComponent;
  shortcutsText?: string;
}

const createSection = ({
  text,
  shortcutsText,
  ...section
}: DocsSectionInput): DocsSection => ({
  ...section,
  searchText: shortcutsText === undefined ? text : `${text} ${shortcutsText}`,
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

export const OVERVIEW_SECTION = createSection({
  id: "overview",
  title: "Overview",
  group: "Getting Started",
  Content: OverviewContent,
  text: overviewText.replace("Version", `Version ${APP_VERSION}`),
});

export const DOC_SECTIONS: DocsSection[] = [
  OVERVIEW_SECTION,
  createSection({
    id: "coding-agent",
    title: "Connect to Coding Agent",
    group: "Getting Started",
    Content: CodingAgentContent,
    text: codingAgentText,
  }),
  createSection({
    id: "loading-a-table",
    title: "Loading a Table",
    group: "Getting Started",
    Content: LoadingTableContent,
    text: loadingTableText,
  }),
  createSection({
    id: "tips",
    title: "Tips & Tricks",
    group: "Getting Started",
    Content: TipsContent,
    text: tipsText,
  }),
  createSection({
    id: "timeline-view",
    title: "Timeline",
    group: "Table Views",
    Content: TimelineViewContent,
    text: timelineViewText,
    Shortcuts: TimelineViewShortcuts,
    shortcutsText: timelineViewShortcutsText,
  }),
  createSection({
    id: "metadata-view",
    title: "Metadata",
    group: "Table Views",
    Content: MetadataViewContent,
    text: metadataViewText,
    Shortcuts: MetadataViewShortcuts,
    shortcutsText: metadataViewShortcutsText,
  }),
  createSection({
    id: "filetree-view",
    title: "FileTree",
    group: "Table Views",
    Content: FileTreeViewContent,
    text: fileTreeViewText,
  }),
  createSection({
    id: "graph-view",
    title: "Graph",
    group: "Table Views",
    Content: GraphViewContent,
    text: graphViewText,
    Shortcuts: GraphViewShortcuts,
    shortcutsText: graphViewShortcutsText,
  }),
  createSection({
    id: "partition-files",
    title: "Overview",
    group: "Partition Files",
    Content: PartitionFilesContent,
    text: partitionFilesText,
    Shortcuts: PartitionFilesShortcuts,
    shortcutsText: partitionFilesShortcutsText,
  }),
  createSection({
    id: "partition-files-filetree",
    title: "FileTree Tab",
    group: "Partition Files",
    Content: PartitionFilesFileTreeContent,
    text: partitionFilesFileTreeText,
  }),
  createSection({
    id: "partition-files-graph",
    title: "Graph Tab",
    group: "Partition Files",
    Content: PartitionFilesGraphContent,
    text: partitionFilesGraphText,
  }),
  createSection({
    id: "specs-panel",
    title: "Specs Panel",
    group: "General",
    Content: SpecsPanelContent,
    text: specsPanelText,
  }),
  createSection({
    id: "issues-panel",
    title: "Issues Panel",
    group: "General",
    Content: IssuesPanelContent,
    text: issuesPanelText,
  }),
  createSection({
    id: "ai-assistant",
    title: "AI Assistant",
    group: "General",
    Content: AiAssistantContent,
    text: aiAssistantText,
    Shortcuts: AiAssistantShortcuts,
    shortcutsText: aiAssistantShortcutsText,
  }),
  createSection({
    id: "keyboard-shortcuts",
    title: "Keyboard Shortcuts",
    group: "General",
    Content: KeyboardShortcutsContent,
    text: keyboardShortcutsText,
  }),
  createSection({
    id: "cli",
    title: "CLI & Python Client",
    group: "General",
    Content: ClientCliContent,
    text: `${clientCliText} ${PIP_INSTALL_COMMAND}`,
  }),
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
