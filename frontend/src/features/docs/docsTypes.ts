import type { DocsSection } from "./docsSections";

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
