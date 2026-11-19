import type { TimelineRow } from "../lib/timelineRow";

const BRANCH_ACCENTS = [
  { node: "border-violet-400", chip: "bg-violet-500/15 text-violet-400" },
  { node: "border-teal-400", chip: "bg-teal-500/15 text-teal-400" },
  { node: "border-rose-400", chip: "bg-rose-500/15 text-rose-400" },
  { node: "border-amber-400", chip: "bg-amber-500/15 text-amber-400" },
  { node: "border-emerald-400", chip: "bg-emerald-500/15 text-emerald-400" },
  { node: "border-fuchsia-400", chip: "bg-fuchsia-500/15 text-fuchsia-400" },
  { node: "border-orange-400", chip: "bg-orange-500/15 text-orange-400" },
  { node: "border-sky-400", chip: "bg-sky-500/15 text-sky-400" },
] as const;

const FALLBACK_ACCENT = BRANCH_ACCENTS[0];

export interface BranchAccent {
  node: string;
  chip: string;
}

const MAIN_ACCENT: BranchAccent = {
  node: "border-blue-400",
  chip: "bg-blue-500/15 text-blue-400",
};

/** main stays unmarked on single-branch tables and gets a fixed blue once other branches exist. */
export const branchAccentsByName = (
  rows: TimelineRow[],
): ReadonlyMap<string, BranchAccent> => {
  const accents = new Map<string, BranchAccent>();
  const assign = (name: string | null) => {
    if (name !== null && name !== "main" && !accents.has(name)) {
      accents.set(
        name,
        BRANCH_ACCENTS[accents.size % BRANCH_ACCENTS.length] ?? FALLBACK_ACCENT,
      );
    }
  };

  for (const row of rows) {
    assign(row.branchName);
    for (const segment of row.impact) {
      if (segment.kind === "ref" && segment.refType === "branch") {
        assign(segment.name);
      }
    }
  }

  const hasOtherBranches = accents.size > 0;
  if (hasOtherBranches) {
    accents.set("main", MAIN_ACCENT);
  }
  return accents;
};

interface Tone {
  textClass: string;
  backgroundClass: string;
  color: string;
}

const tone = (
  textClass: string,
  backgroundClass: string,
  color: string,
): Tone => ({ textClass, backgroundClass, color });

const DATA_ADDED_TONE = tone(
  "text-emerald-300",
  "bg-emerald-300",
  "var(--color-emerald-300)",
);
const DATA_REMOVED_TONE = tone(
  "text-rose-300",
  "bg-rose-300",
  "var(--color-rose-300)",
);
const DATA_REPLACED_TONE = tone(
  "text-amber-300",
  "bg-amber-300",
  "var(--color-amber-300)",
);
const UNPUBLISHED_TONE = tone(
  "text-sky-300",
  "bg-sky-300",
  "var(--color-sky-300)",
);
const MAINTENANCE_TONE = tone(
  "text-cyan-300",
  "bg-cyan-300",
  "var(--color-cyan-300)",
);
const DEFINITION_TONE = tone(
  "text-violet-300",
  "bg-violet-300",
  "var(--color-violet-300)",
);
const REFERENCE_TONE = tone(
  "text-indigo-300",
  "bg-indigo-300",
  "var(--color-indigo-300)",
);
const ROLLBACK_TONE = tone(
  "text-orange-300",
  "bg-orange-300",
  "var(--color-orange-300)",
);
const QUIET_TONE = tone(
  "text-slate-300",
  "bg-slate-300",
  "var(--color-slate-300)",
);
const DEFAULT_TONE = tone("text-ink", "bg-ink", "var(--color-ink)");

const TITLE_TONES: Record<string, Tone> = {
  "Data added": DATA_ADDED_TONE,
  "Data deleted": DATA_REMOVED_TONE,
  "Data overwritten": DATA_REPLACED_TONE,
  "Data not published": UNPUBLISHED_TONE,
  "Files compacted": MAINTENANCE_TONE,
  "Delete files compacted": MAINTENANCE_TONE,
  "Manifests rewritten": MAINTENANCE_TONE,
  "History trimmed": MAINTENANCE_TONE,
  "Schema changed": DEFINITION_TONE,
  "Partitioning changed": DEFINITION_TONE,
  "Sort order changed": DEFINITION_TONE,
  "Branch changed": REFERENCE_TONE,
  "Tag changed": REFERENCE_TONE,
  "Switched snapshots": REFERENCE_TONE,
  "Rolled back": ROLLBACK_TONE,
  "Settings changed": QUIET_TONE,
  "Metadata updated": QUIET_TONE,
  "Start of history": QUIET_TONE,
  "Data changed": QUIET_TONE,
  "No data changed": QUIET_TONE,
};

/**
 * Each kind of change reads in its own light tone, bright enough to stay readable on the card.
 * The list title and the details panel share it, so a row keeps its color when opened.
 */
export const eventToneFor = (row: TimelineRow): Tone => {
  if (row.movedToBranchName !== null) {
    return REFERENCE_TONE;
  }
  return TITLE_TONES[row.title] ?? DEFAULT_TONE;
};
