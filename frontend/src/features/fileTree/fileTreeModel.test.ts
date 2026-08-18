import assert from "node:assert/strict";
import test from "node:test";
import {
  buildFileTree,
  buildFileTreeGraphIndex,
  calculateFileStatistics,
  formatSnapshotVersion,
  getBranches,
  getDisplayedSnapshots,
  getSnapshotFileErrors,
  getSnapshotFiles,
  groupFilesByPartition,
} from "./fileTreeModel.ts";
import { fileTreeContextSchema } from "./fileTreeSchemas.ts";
import type { DataFileNode, DataFileType } from "./fileTreeTypes.ts";

const createDataFile = (
  id: string,
  type: DataFileType,
  sizeGb: number,
  rowCount: number,
  partition: string,
  snapshotId: string,
): DataFileNode => ({
  details: {
    earliest_appearing_snapshot_id: snapshotId,
    partition,
    row_count: rowCount,
    size_gb: sizeGb,
  },
  id,
  type,
});

void test("calculates partition statistics across every file type", () => {
  const files = [
    createDataFile("data", "data", 1, 10, "region=eu", "2"),
    createDataFile("position", "position_delete", 0.5, 2, "region=eu", "2"),
    createDataFile("equality", "equality_delete", 0.25, 1, "region=eu", "2"),
  ];
  const statistics = calculateFileStatistics(files);

  assert.equal(statistics.fileCount, 3);
  assert.equal(statistics.totalRowCount, 13);
  assert.equal(statistics.dataFileCount, 1);
  assert.equal(statistics.positionDeleteFileCount, 1);
  assert.equal(statistics.equalityDeleteFileCount, 1);
  assert.equal(statistics.totalSizeBytes, 1.75 * 1024 ** 3);
});

void test("commit scope includes only files first appearing in the snapshot", () => {
  const context = fileTreeContextSchema.parse({
    edges: [
      { from: "snapshot-path", to: "manifest-path" },
      { from: "manifest-path", to: "old-file" },
      { from: "manifest-path", to: "new-file" },
    ],
    metadata: null,
    nodes: [
      {
        details: { error: null, snapshot_id: "2" },
        id: "snapshot-path",
        type: "snapshot",
      },
      { details: {}, id: "manifest-path", type: "manifest" },
      createDataFile("old-file", "data", 1, 10, "region=eu", "1"),
      createDataFile("new-file", "data", 1, 10, "region=eu", "2"),
    ],
  });
  const graphIndex = buildFileTreeGraphIndex(context);
  const snapshot = graphIndex.snapshots[0];

  assert.deepEqual(
    getSnapshotFiles(snapshot, graphIndex, "snapshot").map(({ id }) => id),
    ["old-file", "new-file"],
  );
  assert.deepEqual(
    getSnapshotFiles(snapshot, graphIndex, "commit").map(({ id }) => id),
    ["new-file"],
  );
  assert.deepEqual(getSnapshotFileErrors(snapshot, graphIndex), []);
});

void test("tree folders roll statistics up from descendant partitions", () => {
  const files = [
    createDataFile("one", "data", 1, 10, "region=eu, day=1", "2"),
    createDataFile("two", "data", 2, 20, "region=eu, day=2", "2"),
  ];
  const tree = buildFileTree(groupFilesByPartition(files, ""));
  const regionFolder = tree[0];

  assert.ok(regionFolder);
  assert.equal(regionFolder.statistics.fileCount, 2);
  assert.equal(regionFolder.statistics.totalRowCount, 30);
  assert.equal(regionFolder.children.length, 2);
});

void test("snapshot labels expose the actual snapshot version", () => {
  const [snapshot] = buildFileTreeGraphIndex(
    fileTreeContextSchema.parse({
      edges: [],
      nodes: [
        {
          details: {
            operation_description: "Append",
            snapshot_id: "3791262493104209104",
            timestamp: "2026-08-15 13:45:59.674+03:00",
          },
          id: "snapshot-path",
          type: "snapshot",
        },
      ],
    }),
  ).snapshots;

  assert.ok(snapshot);
  assert.equal(
    formatSnapshotVersion(snapshot, true),
    "ID 3791262493104209104 · Append · 2026-08-15 13:45:59.674+03:00 · latest",
  );
});

void test("discovers historical branches across metadata versions", () => {
  const context = fileTreeContextSchema.parse({
    edges: [],
    metadata: {
      refs: { main: { "snapshot-id": "3", type: "branch" } },
    },
    nodes: [
      {
        details: {
          refs: { main: { "snapshot-id": "3", type: "branch" } },
          timestamp: "2026-08-15T13:46:00Z",
        },
        id: "metadata-3",
        type: "main_metadata",
      },
      {
        details: {
          refs: {
            main: { "snapshot-id": "2", type: "branch" },
            my_test_branch: { "snapshot-id": "2", type: "branch" },
          },
          timestamp: "2026-08-15T13:45:59Z",
        },
        id: "metadata-2",
        type: "metadata",
      },
      {
        details: { parent_id: "2", snapshot_id: "3", timestamp: 3 },
        id: "snapshot-3",
        type: "snapshot",
      },
      {
        details: { parent_id: "1", snapshot_id: "2", timestamp: 2 },
        id: "snapshot-2",
        type: "snapshot",
      },
      {
        details: { parent_id: null, snapshot_id: "1", timestamp: 1 },
        id: "snapshot-1",
        type: "snapshot",
      },
    ],
  });
  const branches = getBranches(context);

  assert.deepEqual(branches, [
    { headSnapshotId: "3", name: "main" },
    { headSnapshotId: "2", name: "my_test_branch" },
  ]);
  assert.deepEqual(
    getDisplayedSnapshots(
      buildFileTreeGraphIndex(context),
      branches,
      "my_test_branch",
    ).map((snapshot) => snapshot.details.snapshot_id),
    ["1", "2"],
  );
});
