import type { TimelineRow } from "../lib/timelineRow";

const WRITE_COLOR = "#1964B9";
const METADATA_COLOR = "#6437D2";
const BRANCH_COLOR = "#0EA5E9";

export const eventColorFor = (row: TimelineRow): string => {
  if (row.kind === "draft-write") {
    return BRANCH_COLOR;
  }
  if (row.kind === "published-write") {
    return row.branchName === "main" || row.branchName === null
      ? WRITE_COLOR
      : BRANCH_COLOR;
  }
  return METADATA_COLOR;
};

export const eventBorderClassFor = (row: TimelineRow): string => {
  const color = eventColorFor(row);
  if (color === WRITE_COLOR) {
    return "border-l-[#1964B9]";
  }
  if (color === BRANCH_COLOR) {
    return "border-l-[#0EA5E9]";
  }
  return "border-l-[#6437D2]";
};

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
