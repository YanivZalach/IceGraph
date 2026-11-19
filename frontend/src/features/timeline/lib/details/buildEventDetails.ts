import { UNRECOGNIZED_METADATA_TITLE } from "../classification/describeMetadataOnlyCommit";
import type { SnapshotNode } from "../../api/nodeSchemas";
import {
  describeFieldDiff,
  type DescribedFieldDiff,
} from "./describeFieldDiff";
import type { DetailRowData } from "./detailRowData";
import { diffMetadataFiles } from "./diffMetadataFiles";
import { buildEventNotices, type DetailNotice } from "./eventNotices";
import {
  buildMetadataFileData,
  refRows,
  type MetadataFileData,
} from "./metadataFileDetails";
import { formatDayMonthYearAndClock } from "../format/formatTimelineTime";
import { SNAPSHOT_UNAVAILABLE } from "../format/snapshotUnavailable";
import {
  groupSnapshotSummary,
  type SummaryEntry,
} from "./groupSnapshotSummary";
import {
  buildBeforeAfterRows,
  buildChangeCounts,
  type BeforeAfterRow,
  type ChangeCounts,
} from "./summaryTables";
import type { CommitRow, TimelineData } from "../timelineRow";

export interface EventDetailData {
  notices: DetailNotice[];
  actionLink: string | null;
  schemaChange: { before: number; after: number } | null;
  snapshotFilePath: string | null;
  topRows: DetailRowData[];
  thisChange: ChangeCounts;
  tableState: BeforeAfterRow[];
  engine: DetailRowData[];
  metadataFile: MetadataFileData;
  refs: DetailRowData[];
  properties: DetailRowData[];
  rawDiff: DescribedFieldDiff[];
  isRawDiffOpenByDefault: boolean;
}

const toRows = (entries: SummaryEntry[]): DetailRowData[] =>
  entries.map(([label, value]) => ({ label, value }));

const movedToText = (
  targetId: string,
  target: SnapshotNode | undefined,
): string => {
  if (target === undefined) {
    return `snapshot ${targetId} (${SNAPSHOT_UNAVAILABLE})`;
  }
  const operation = target.operation ?? "unknown";
  return `snapshot ${target.snapshot_id} from ${formatDayMonthYearAndClock(target.timestamp)} (${operation})`;
};

const snapshotRows = (
  row: CommitRow,
  snapshot: SnapshotNode | undefined,
): DetailRowData[] => {
  if (snapshot === undefined) {
    return row.snapshotId === null
      ? []
      : [
          {
            label: `Snapshot ID (${SNAPSHOT_UNAVAILABLE})`,
            value: row.snapshotId,
            isCopyable: true,
          },
        ];
  }

  const rows: DetailRowData[] = [
    { label: "Snapshot ID", value: snapshot.snapshot_id, isCopyable: true },
    { label: "Parent ID", value: snapshot.parent_id ?? "" },
    { label: "Operation", value: snapshot.operation ?? "" },
    { label: "Manifest List", value: snapshot.file_path, isCopyable: true },
  ];
  // Row lineage exists only on v3 tables; older versions leave it null.
  if (snapshot.first_row_id != null) {
    rows.push({ label: "First Row ID", value: String(snapshot.first_row_id) });
  }
  if (snapshot.added_rows != null) {
    rows.push({ label: "Added Rows", value: String(snapshot.added_rows) });
  }

  if (
    snapshot.operation_description !== null &&
    snapshot.operation_description !== snapshot.operation
  ) {
    rows.push({
      label: "Operation Description",
      value: snapshot.operation_description,
    });
  }

  return rows;
};

const eventRows = (
  row: CommitRow,
  repointTarget: SnapshotNode | undefined,
): DetailRowData[] => {
  const rows: DetailRowData[] = [];

  if (row.repointTargetId !== null) {
    rows.push({
      label: "Moved To",
      value: movedToText(row.repointTargetId, repointTarget),
    });
  }

  if (row.details.length > 0) {
    rows.push({ label: "Changes", value: row.details.join("\n") });
  }

  if (row.isDraftPublishedLater && row.publishedAtMs !== null) {
    rows.push({
      label: "Published",
      value: formatDayMonthYearAndClock(row.publishedAtMs),
    });
  }

  const isUnpublishedInRange =
    row.kind === "draft-write" &&
    !row.isDraftPublishedLater &&
    row.publishedAsSnapshotId === null;
  if (isUnpublishedInRange) {
    rows.push({ label: "Published", value: "not in the loaded range" });
  }

  if (row.publishedAsSnapshotId !== null) {
    rows.push({
      label: "Published As",
      value: row.publishedAsSnapshotId,
      isCopyable: true,
    });
  }

  return rows;
};

export const buildEventDetails = (
  timeline: TimelineData,
  row: CommitRow,
): EventDetailData => {
  const { snapshotsById, filesByPath } = timeline;

  const file = filesByPath.get(row.filePath);
  if (file === undefined) {
    throw new Error(`timeline row without its metadata file: ${row.filePath}`);
  }

  const snapshot =
    row.snapshotId === null ? undefined : snapshotsById.get(row.snapshotId);
  const summary =
    snapshot === undefined ? null : groupSnapshotSummary(snapshot.summary);
  const thisChange = buildChangeCounts(summary?.thisChange ?? []);

  const parentSnapshot =
    snapshot?.parent_id == null
      ? undefined
      : snapshotsById.get(snapshot.parent_id);

  const repointTarget =
    row.repointTargetId === null
      ? undefined
      : snapshotsById.get(row.repointTargetId);

  const actionLink = snapshot?.action_link ?? null;

  const previousFile =
    row.previousFilePath === null
      ? undefined
      : filesByPath.get(row.previousFilePath);
  const beforeSchema = previousFile?.current_schema_id;
  const afterSchema = file.current_schema_id;

  const branchRows: DetailRowData[] =
    row.branchName === null
      ? []
      : [{ label: "Branch Name", value: row.branchName }];

  const isChangeUnknown = row.kind === "unknown-change";
  const currentSnapshotRows: DetailRowData[] =
    isChangeUnknown && file.snapshot_id !== null
      ? [
          {
            label: "Current Snapshot ID",
            value: file.snapshot_id,
            isCopyable: true,
          },
        ]
      : [];

  return {
    notices: buildEventNotices(row, file, snapshot),
    actionLink: actionLink === "" ? null : actionLink,
    schemaChange:
      beforeSchema != null &&
      afterSchema !== null &&
      beforeSchema !== afterSchema
        ? { before: beforeSchema, after: afterSchema }
        : null,
    snapshotFilePath: snapshot?.file_path ?? null,
    topRows: [
      ...branchRows,
      ...currentSnapshotRows,
      ...snapshotRows(row, snapshot),
      ...eventRows(row, repointTarget),
    ],
    thisChange,
    tableState: buildBeforeAfterRows(
      summary?.tableAfter ?? [],
      parentSnapshot?.summary ?? null,
    ),
    engine: toRows(summary?.engine ?? []),
    metadataFile: buildMetadataFileData(file),
    refs: refRows(file),
    properties: toRows(Object.entries(file.properties)),
    rawDiff:
      previousFile === undefined
        ? []
        : diffMetadataFiles(previousFile, file).map(describeFieldDiff),
    isRawDiffOpenByDefault:
      row.kind === "metadata-only" && row.title === UNRECOGNIZED_METADATA_TITLE,
  };
};
