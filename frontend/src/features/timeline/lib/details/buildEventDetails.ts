import type { MetadataFileNode, SnapshotNode } from "../../api/nodeSchemas";
import { diffMetadataFiles, type FieldDiff } from "./diffMetadataFiles";
import {
  formatDayAndMonth,
  formatDayMonthYearAndClock,
} from "../format/formatTimelineTime";
import { formatShortId } from "../format/formatShortId";
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
import type { TimelineData, TimelineRow } from "../timelineRow";

export interface DetailRowData {
  label: string;
  value: string;
  isCopyable?: boolean;
}

export interface MetadataFileData {
  path: string;
  stats: DetailRowData[];
}

export interface DetailNotice {
  kind: "error" | "warning";
  text: string;
}

export interface EventDetailData {
  notices: DetailNotice[];
  actionLink: string | null;
  snapshotFilePath: string | null;
  topRows: DetailRowData[];
  thisChange: ChangeCounts;
  tableState: BeforeAfterRow[];
  engine: DetailRowData[];
  metadataFile: MetadataFileData;
  refs: DetailRowData[];
  properties: DetailRowData[];
  rawDiff: FieldDiff[];
}

const toRows = (entries: SummaryEntry[]): DetailRowData[] =>
  entries.map(([label, value]) => ({ label, value }));

const movedToText = (
  targetId: string,
  target: SnapshotNode | undefined,
): string => {
  if (target === undefined) {
    return `snapshot ${formatShortId(targetId)} (expired or not loaded)`;
  }
  const operation = target.operation ?? "unknown";
  return `snapshot ${formatShortId(target.snapshot_id)} from ${formatDayMonthYearAndClock(target.timestamp)} (${operation})`;
};

const snapshotRows = (
  row: TimelineRow,
  snapshot: SnapshotNode | undefined,
): DetailRowData[] => {
  if (snapshot === undefined) {
    return row.snapshotId === null
      ? []
      : [
          {
            label: "Snapshot ID (expired or not loaded)",
            value: row.snapshotId,
            isCopyable: true,
          },
        ];
  }

  const rows: DetailRowData[] = [
    { label: "Snapshot ID", value: snapshot.snapshot_id, isCopyable: true },
    { label: "Parent ID", value: snapshot.parent_id ?? "" },
    { label: "Operation", value: snapshot.operation ?? "" },
  ];

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
  row: TimelineRow,
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
      value: formatDayAndMonth(row.publishedAtMs),
    });
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

const fileStats = (file: MetadataFileNode): DetailRowData[] => [
  { label: "Schema ID", value: file.current_schema_id?.toString() ?? "" },
  {
    label: "Partition Spec ID",
    value: file.partition_spec_id?.toString() ?? "",
  },
  { label: "Sort Order ID", value: file.sort_order_id?.toString() ?? "" },
  {
    label: "Last Sequence Number",
    value: file.last_sequence_number?.toString() ?? "",
  },
];

const nodeNotices = (
  file: MetadataFileNode,
  snapshot: SnapshotNode | undefined,
): DetailNotice[] => {
  const candidates = [
    { kind: "error", text: file.error },
    { kind: "error", text: snapshot?.error },
    { kind: "warning", text: file.warning },
    { kind: "warning", text: snapshot?.warning },
  ] as const;

  const seenTexts = new Set<string>();
  const notices: DetailNotice[] = [];
  for (const { kind, text } of candidates) {
    if (text != null && text !== "" && !seenTexts.has(text)) {
      seenTexts.add(text);
      notices.push({ kind, text });
    }
  }
  return notices;
};

const refRows = (file: MetadataFileNode): DetailRowData[] =>
  Object.entries(file.refs).map(([name, ref]) => ({
    label: `${ref.type} ${name}`,
    value: ref["snapshot-id"],
    isCopyable: true,
  }));

export const buildEventDetails = (
  timeline: TimelineData,
  row: TimelineRow,
): EventDetailData => {
  const { rows, snapshotsById, filesByPath } = timeline;

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

  const rowIndex = rows.findIndex(
    (candidate) => candidate.filePath === row.filePath,
  );
  const rowBelow = rowIndex === -1 ? undefined : rows[rowIndex + 1];
  const previousFile =
    rowBelow === undefined ? undefined : filesByPath.get(rowBelow.filePath);

  const branchRows: DetailRowData[] =
    row.branchName === null
      ? []
      : [{ label: "Branch Name", value: row.branchName }];

  return {
    notices: nodeNotices(file, snapshot),
    actionLink: actionLink === "" ? null : actionLink,
    snapshotFilePath: snapshot?.file_path ?? null,
    topRows: [
      ...branchRows,
      ...snapshotRows(row, snapshot),
      ...eventRows(row, repointTarget),
    ],
    thisChange,
    tableState: buildBeforeAfterRows(
      summary?.tableAfter ?? [],
      parentSnapshot?.summary ?? null,
    ),
    engine: toRows(summary?.engine ?? []),
    metadataFile: { path: file.file_path, stats: fileStats(file) },
    refs: refRows(file),
    properties: toRows(Object.entries(file.properties)),
    rawDiff:
      previousFile === undefined ? [] : diffMetadataFiles(previousFile, file),
  };
};
