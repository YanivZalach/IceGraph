import type { SnapshotRefs } from "../api/nodeSchemas";
import type { SnapshotReference } from "../api/snapshotReferenceSchema";
import type { SnapshotsById } from "./classification/commitDescription";
import type { SequenceItem } from "./sequenceTimelineFiles";
import type { TimelineRow } from "./timelineRow";

const MAIN_BRANCH = "main";
const MAIN_LANE = 0;

export interface LaneLine {
  lane: number;
  branchName: string;
}

export interface LaneEdge {
  kind: "fork" | "merge";
  fromLane: number;
  toLane: number;
  branchName: string | null;
}

export interface RowLanes {
  through: LaneLine[];
  starting: LaneLine[];
  ending: LaneLine[];
  edges: LaneEdge[];
  nodeLane: number;
  isGap: boolean;
}

export interface BranchGraph {
  laneCount: number;
  branchNames: string[];
  lanesByFilePath: ReadonlyMap<string, RowLanes>;
}

type CommitItem = Extract<SequenceItem, { kind: "commit" }>;

const branchHeads = (refs: SnapshotRefs): Map<string, string> => {
  const heads = new Map<string, string>();
  for (const [name, ref] of Object.entries(refs)) {
    if (ref.type === "branch") {
      heads.set(name, ref["snapshot-id"]);
    }
  }
  return heads;
};

const isSameRef = (
  first: SnapshotReference | undefined,
  second: SnapshotReference | undefined,
): boolean =>
  first?.type === second?.type &&
  first?.["snapshot-id"] === second?.["snapshot-id"] &&
  first?.["min-snapshots-to-keep"] === second?.["min-snapshots-to-keep"] &&
  first?.["max-snapshot-age-ms"] === second?.["max-snapshot-age-ms"] &&
  first?.["max-ref-age-ms"] === second?.["max-ref-age-ms"];

const findChangedBranches = (
  refs: SnapshotRefs,
  previousRefs: SnapshotRefs,
): string[] =>
  [...new Set([...Object.keys(refs), ...Object.keys(previousRefs)])].filter(
    (name) => {
      const ref = refs[name];
      const previousRef = previousRefs[name];
      const isBranch = ref?.type === "branch" || previousRef?.type === "branch";
      return name !== MAIN_BRANCH && isBranch && !isSameRef(ref, previousRef);
    },
  );

const laneOf = (lanes: ReadonlyMap<string, number>, name: string): number =>
  name === MAIN_BRANCH ? MAIN_LANE : (lanes.get(name) ?? MAIN_LANE);

const findSourceLane = (
  snapshotId: string,
  previousHeads: ReadonlyMap<string, string>,
  previousLanes: ReadonlyMap<string, number>,
  snapshotsById: SnapshotsById,
): number | null => {
  const parentId = snapshotsById.get(snapshotId)?.parent_id ?? null;
  const candidates = [snapshotId, parentId];
  for (const candidateId of candidates) {
    if (candidateId === null) {
      continue;
    }
    if (previousHeads.get(MAIN_BRANCH) === candidateId) {
      return MAIN_LANE;
    }
    for (const [name, headId] of previousHeads) {
      const lane = previousLanes.get(name);
      if (headId === candidateId && lane !== undefined) {
        return lane;
      }
    }
  }
  return null;
};

const firstFreeLane = (taken: ReadonlySet<number>): number => {
  let lane = MAIN_LANE + 1;
  while (taken.has(lane)) {
    lane += 1;
  }
  return lane;
};

const toLines = (lanes: ReadonlyMap<string, number>): LaneLine[] => [
  { lane: MAIN_LANE, branchName: MAIN_BRANCH },
  ...[...lanes].map(([branchName, lane]) => ({ lane, branchName })),
];

const nodeLaneOf = (
  row: TimelineRow,
  lanes: ReadonlyMap<string, number>,
  changedBranches: string[],
): number => {
  if (row.kind === "published-write" && row.branchName !== null) {
    return laneOf(lanes, row.branchName);
  }
  const [onlyChanged] = changedBranches;
  return row.kind === "metadata-only" &&
    changedBranches.length === 1 &&
    onlyChanged !== undefined
    ? laneOf(lanes, onlyChanged)
    : MAIN_LANE;
};

interface LaneAdvance {
  openLanes: Map<string, number>;
  rowLanes: RowLanes;
}

const advanceLanes = (
  { file, predecessor }: CommitItem,
  row: TimelineRow,
  previousLanes: ReadonlyMap<string, number>,
  snapshotsById: SnapshotsById,
): LaneAdvance => {
  const heads = branchHeads(file.refs);
  const previousHeads =
    predecessor.kind === "file"
      ? branchHeads(predecessor.file.refs)
      : new Map<string, string>();
  const isComparable = predecessor.kind === "file";

  const ending: LaneLine[] = [];
  const through: LaneLine[] = [{ lane: MAIN_LANE, branchName: MAIN_BRANCH }];
  const lanes = new Map<string, number>();
  for (const [branchName, lane] of previousLanes) {
    if (heads.has(branchName)) {
      lanes.set(branchName, lane);
      through.push({ lane, branchName });
    } else {
      ending.push({ lane, branchName });
    }
  }

  const taken = new Set([...previousLanes.values(), MAIN_LANE]);
  const starting: LaneLine[] = [];
  const edges: LaneEdge[] = [];
  for (const [branchName, headId] of heads) {
    if (branchName === MAIN_BRANCH || lanes.has(branchName)) {
      continue;
    }
    const lane = firstFreeLane(taken);
    taken.add(lane);
    lanes.set(branchName, lane);
    starting.push({ lane, branchName });
    const sourceLane = isComparable
      ? findSourceLane(headId, previousHeads, previousLanes, snapshotsById)
      : null;
    if (sourceLane !== null) {
      edges.push({
        kind: "fork",
        fromLane: sourceLane,
        toLane: lane,
        branchName,
      });
    }
  }

  const changedBranches =
    predecessor.kind === "file"
      ? findChangedBranches(file.refs, predecessor.file.refs)
      : [];
  let nodeLane = nodeLaneOf(
    row,
    new Map([...previousLanes, ...lanes]),
    changedBranches,
  );

  if (row.kind === "re-point" && row.movedToBranchName !== null) {
    const fromLane = previousLanes.get(row.movedToBranchName);
    if (fromLane !== undefined) {
      edges.push({
        kind: "merge",
        fromLane,
        toLane: MAIN_LANE,
        branchName: row.movedToBranchName,
      });
    }
  }

  if (row.kind === "draft-write" && row.snapshotId !== null) {
    const sourceLane = isComparable
      ? findSourceLane(
          row.snapshotId,
          previousHeads,
          previousLanes,
          snapshotsById,
        )
      : null;
    nodeLane = sourceLane ?? MAIN_LANE;
  }

  return {
    openLanes: lanes,
    rowLanes: { through, starting, ending, edges, nodeLane, isGap: false },
  };
};

export const buildBranchLanes = (
  sequence: SequenceItem[],
  rowsOldestFirst: TimelineRow[],
  snapshotsById: SnapshotsById,
): BranchGraph => {
  const lanesByFilePath = new Map<string, RowLanes>();
  let openLanes = new Map<string, number>();
  let laneCount = 1;
  const branchNames = new Set<string>([MAIN_BRANCH]);

  const record = (filePath: string, rowLanes: RowLanes) => {
    lanesByFilePath.set(filePath, rowLanes);
    for (const line of [...rowLanes.through, ...rowLanes.starting]) {
      branchNames.add(line.branchName);
    }
    const lanes = [
      ...rowLanes.through,
      ...rowLanes.starting,
      ...rowLanes.ending,
    ].map((line) => line.lane);
    laneCount = Math.max(
      laneCount,
      rowLanes.nodeLane + 1,
      ...lanes.map((lane) => lane + 1),
    );
  };

  sequence.forEach((item, index) => {
    const row = rowsOldestFirst[index];
    if (row === undefined) {
      return;
    }
    if (item.kind === "gap") {
      record(row.filePath, {
        through: toLines(openLanes),
        starting: [],
        ending: [],
        edges: [],
        nodeLane: MAIN_LANE,
        isGap: true,
      });
      return;
    }

    const result = advanceLanes(item, row, openLanes, snapshotsById);
    openLanes = result.openLanes;
    record(row.filePath, result.rowLanes);
  });

  return { laneCount, branchNames: [...branchNames], lanesByFilePath };
};
