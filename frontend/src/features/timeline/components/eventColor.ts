import type { CommitCategory } from "../lib/classification/commitDescription";
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
    if (row.kind === "gap") {
      continue;
    }
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
  color: string;
}

const tone = (textClass: string, color: string): Tone => ({ textClass, color });

const TONE_BY_CATEGORY: Record<CommitCategory, Tone> = {
  "data-added": tone("text-emerald-300", "var(--color-emerald-300)"),
  "data-removed": tone("text-rose-300", "var(--color-rose-300)"),
  "data-replaced": tone("text-amber-300", "var(--color-amber-300)"),
  unpublished: tone("text-sky-300", "var(--color-sky-300)"),
  maintenance: tone("text-cyan-300", "var(--color-cyan-300)"),
  definition: tone("text-violet-300", "var(--color-violet-300)"),
  reference: tone("text-indigo-300", "var(--color-indigo-300)"),
  rollback: tone("text-orange-300", "var(--color-orange-300)"),
  quiet: tone("text-slate-300", "var(--color-slate-300)"),
};

const GAP_TONE = tone("text-amber-300", "var(--color-amber-300)");

/**
 * Each kind of change reads in its own light tone, bright enough to stay readable on the card.
 * The list title and the details panel share it, so a row keeps its color when opened.
 */
export const eventToneFor = (row: TimelineRow): Tone =>
  row.kind === "gap" ? GAP_TONE : TONE_BY_CATEGORY[row.category];
