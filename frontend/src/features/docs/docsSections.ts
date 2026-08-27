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
import { PIP_INSTALL_COMMAND } from "./docsConstants";

export interface DocsSection {
  id: string;
  title: string;
  markdown: string;
}

const resolvePlaceholders = (markdown: string): string =>
  markdown
    .replaceAll("{{APP_VERSION}}", APP_VERSION)
    .replaceAll("{{PIP_INSTALL_COMMAND}}", PIP_INSTALL_COMMAND)
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
