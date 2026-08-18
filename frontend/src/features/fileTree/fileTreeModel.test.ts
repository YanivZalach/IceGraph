import assert from "node:assert/strict";
import test from "node:test";
import {
  buildFileTree,
  buildFileTreeGraphIndex,
  calculateFileStatistics,
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
