import type { ReactElement } from "react";
import OverviewSection from "./components/OverviewSection";
import CodingAgentSection from "./components/CodingAgentSection";
import LoadingTableSection from "./components/LoadingTableSection";
import TimelineViewSection from "./components/TimelineViewSection";
import MetadataViewSection from "./components/MetadataViewSection";
import FileTreeViewSection from "./components/FileTreeViewSection";
import GraphViewSection from "./components/GraphViewSection";
import SpecsPanelSection from "./components/SpecsPanelSection";
import IssuesPanelSection from "./components/IssuesPanelSection";
import KeyboardShortcutsSection from "./components/KeyboardShortcutsSection";
import TipsSection from "./components/TipsSection";
import ClientCliSection from "./components/ClientCliSection";

export interface DocsSection {
  id: string;
  title: string;
  body: ReactElement;
}

export const OVERVIEW_SECTION: DocsSection = {
  id: "overview",
  title: "Overview",
  body: <OverviewSection />,
};

export const SECTIONS: DocsSection[] = [
  OVERVIEW_SECTION,
  {
    id: "claude-code",
    title: "Connect to Coding Agent",
    body: <CodingAgentSection />,
  },
  {
    id: "loading-a-table",
    title: "Loading a Table",
    body: <LoadingTableSection />,
  },
  {
    id: "timeline-view",
    title: "Timeline View",
    body: <TimelineViewSection />,
  },
  {
    id: "metadata-view",
    title: "Metadata View",
    body: <MetadataViewSection />,
  },
  {
    id: "filetree-view",
    title: "FileTree View",
    body: <FileTreeViewSection />,
  },
  { id: "graph-view", title: "Graph View", body: <GraphViewSection /> },
  { id: "specs-panel", title: "Specs Panel", body: <SpecsPanelSection /> },
  { id: "issues-panel", title: "Issues Panel", body: <IssuesPanelSection /> },
  {
    id: "keyboard-shortcuts",
    title: "Keyboard Shortcuts",
    body: <KeyboardShortcutsSection />,
  },
  { id: "tips", title: "Tips & Tricks", body: <TipsSection /> },
  { id: "cli", title: "CLI & Python Client", body: <ClientCliSection /> },
];
