// Expandable sections sit directly on top of each other, so the summary stays
// at the card colour while the opened body is recessed to the canvas colour.
// Without the contrast the next section header is hard to find once one is open.
export const METADATA_SUMMARY_CLASS =
  "cursor-pointer px-5 py-4 text-sm font-medium text-ink hover:bg-surface-hover";

export const METADATA_SECTION_BODY_CLASS = "border-t border-edge bg-canvas";
