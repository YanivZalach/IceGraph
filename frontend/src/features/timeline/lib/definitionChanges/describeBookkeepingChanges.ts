import type { MetadataFileNode } from "../../api/nodeSchemas";
import { FIELD_TITLES, formatRawValue } from "../details/describeFieldDiff";
import { diffMetadataFiles } from "../details/diffMetadataFiles";
import { formatShortId } from "../format/formatShortId";
import { sameTextChange, type DescribedChange } from "./describedChange";

type StatisticsFiles = MetadataFileNode["pointed_table_statistics_files"];

export const describeFormatVersionChange = (
  previousVersion: number | null | undefined,
  currentVersion: number | null | undefined,
): DescribedChange[] =>
  previousVersion == null ||
  currentVersion == null ||
  previousVersion === currentVersion
    ? []
    : [
        sameTextChange(
          `format version ${previousVersion.toString()} → ${currentVersion.toString()}`,
        ),
      ];

const statisticsKey = (file: NonNullable<StatisticsFiles>[number]): string =>
  `${file["snapshot-id"]} ${file["statistics-path"]}`;

const describeStatisticsSide = (
  kind: string,
  action: string,
  files: NonNullable<StatisticsFiles>,
): DescribedChange[] => {
  const [onlyFile] = files;
  if (onlyFile === undefined) {
    return [];
  }
  if (files.length > 1) {
    return [
      sameTextChange(
        `${files.length.toString()} ${kind} statistics files ${action}`,
      ),
    ];
  }
  const prefix = `${kind} statistics ${action} for snapshot`;
  return [
    {
      impact: `${prefix} ${formatShortId(onlyFile["snapshot-id"])}`,
      detail: `${prefix} ${onlyFile["snapshot-id"]}`,
    },
  ];
};

export const describeStatisticsChanges = (
  kind: "table" | "partition",
  previousFiles: StatisticsFiles,
  currentFiles: StatisticsFiles,
): DescribedChange[] => {
  const previousKeys = new Set((previousFiles ?? []).map(statisticsKey));
  const currentKeys = new Set((currentFiles ?? []).map(statisticsKey));
  const added = (currentFiles ?? []).filter(
    (file) => !previousKeys.has(statisticsKey(file)),
  );
  const removed = (previousFiles ?? []).filter(
    (file) => !currentKeys.has(statisticsKey(file)),
  );
  const removedSnapshotIds = new Set(
    removed.map((file) => file["snapshot-id"]),
  );
  const replaced = added.filter((file) =>
    removedSnapshotIds.has(file["snapshot-id"]),
  );
  const replacedSnapshotIds = new Set(
    replaced.map((file) => file["snapshot-id"]),
  );
  const isReplaced = (file: NonNullable<StatisticsFiles>[number]): boolean =>
    replacedSnapshotIds.has(file["snapshot-id"]);

  return [
    ...describeStatisticsSide(
      kind,
      "added",
      added.filter((file) => !isReplaced(file)),
    ),
    ...describeStatisticsSide(kind, "replaced", replaced),
    ...describeStatisticsSide(
      kind,
      "removed",
      removed.filter((file) => !isReplaced(file)),
    ),
  ];
};

const isScalar = (value: unknown): boolean =>
  value === null || typeof value !== "object";

export const describeRawFieldChanges = (
  previousFile: MetadataFileNode,
  currentFile: MetadataFileNode,
): DescribedChange[] =>
  diffMetadataFiles(previousFile, currentFile).map(({ key, before, after }) => {
    const title = FIELD_TITLES[key] ?? key;
    if (!isScalar(before) || !isScalar(after)) {
      return sameTextChange(`${title} changed`);
    }
    const detail = `${title} ${formatRawValue(before)} → ${formatRawValue(after)}`;
    const shortText = (value: unknown): string =>
      key === "snapshot_id" && typeof value === "string"
        ? formatShortId(value)
        : formatRawValue(value);
    return {
      impact: `${title} ${shortText(before)} → ${shortText(after)}`,
      detail,
    };
  });
