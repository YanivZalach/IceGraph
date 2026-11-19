import type { MetadataFileNode } from "../../api/nodeSchemas";
import { formatShortId } from "../format/formatShortId";
import type { DescribedChange } from "./describedChange";

type StatisticsFiles = MetadataFileNode["pointed_table_statistics_files"];

export type StatisticsChangeAction = "added" | "removed" | "replaced";

export interface StatisticsChange extends DescribedChange {
  action: StatisticsChangeAction;
}

const pathsBySnapshotId = (
  files: StatisticsFiles,
): ReadonlyMap<string, ReadonlySet<string>> => {
  const paths = new Map<string, Set<string>>();
  for (const file of files ?? []) {
    const snapshotPaths = paths.get(file["snapshot-id"]) ?? new Set<string>();
    snapshotPaths.add(file["statistics-path"]);
    paths.set(file["snapshot-id"], snapshotPaths);
  }

  return paths;
};

const statisticsChange = (
  kind: string,
  action: StatisticsChangeAction,
  snapshotId: string,
  path: string,
): StatisticsChange => ({
  action,
  impact: `${kind} statistics ${action} for snapshot ${formatShortId(snapshotId)}`,
  detail: `${kind} statistics ${action} for snapshot ${snapshotId}: ${path}`,
});

const describeStatisticsFilesChange = (
  kind: string,
  previousFiles: StatisticsFiles,
  currentFiles: StatisticsFiles,
): StatisticsChange[] => {
  const previousPaths = pathsBySnapshotId(previousFiles);
  const currentPaths = pathsBySnapshotId(currentFiles);
  const changes: StatisticsChange[] = [];

  for (const [snapshotId, paths] of currentPaths) {
    const earlierPaths = previousPaths.get(snapshotId) ?? new Set<string>();
    const addedPaths = [...paths].filter((path) => !earlierPaths.has(path));
    const removedPaths = [...earlierPaths].filter((path) => !paths.has(path));
    const isReplacement = addedPaths.length > 0 && removedPaths.length > 0;
    for (const path of addedPaths) {
      changes.push(
        statisticsChange(
          kind,
          isReplacement ? "replaced" : "added",
          snapshotId,
          path,
        ),
      );
    }
    if (!isReplacement) {
      for (const path of removedPaths) {
        changes.push(statisticsChange(kind, "removed", snapshotId, path));
      }
    }
  }

  for (const [snapshotId, paths] of previousPaths) {
    if (!currentPaths.has(snapshotId)) {
      for (const path of paths) {
        changes.push(statisticsChange(kind, "removed", snapshotId, path));
      }
    }
  }

  return changes;
};

export const describeStatisticsChanges = (
  previousFile: MetadataFileNode,
  currentFile: MetadataFileNode,
): StatisticsChange[] => [
  ...describeStatisticsFilesChange(
    "Table",
    previousFile.pointed_table_statistics_files,
    currentFile.pointed_table_statistics_files,
  ),
  ...describeStatisticsFilesChange(
    "Partition",
    previousFile.pointed_partition_statistics_files,
    currentFile.pointed_partition_statistics_files,
  ),
];
