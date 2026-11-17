import { formatDurationMs } from "../format/formatDuration";
import type { MetadataFileNode, SnapshotNode } from "../../api/nodeSchemas";
import { diffMetadataFiles, type FieldDiff } from "./diffMetadataFiles";
import { formatDayMonthYearAndClock } from "../format/formatTimelineTime";
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
  /** Set when labels alone may collide, such as user-chosen ref names. */
  id?: string;
  label: string;
  value: string;
  isCopyable?: boolean;
}

export interface MetadataFileData {
  path: string;
  linkedPaths: DetailRowData[];
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
    return `snapshot ${targetId} (expired or not loaded)`;
  }
  const operation = target.operation ?? "unknown";
  return `snapshot ${target.snapshot_id} from ${formatDayMonthYearAndClock(target.timestamp)} (${operation})`;
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

const fileStats = (file: MetadataFileNode): DetailRowData[] => {
  const stats: DetailRowData[] = [
    ...(file.format_version == null
      ? []
      : [{ label: "Format Version", value: String(file.format_version) }]),
    { label: "Schema ID", value: file.current_schema_id?.toString() ?? "" },
    {
      label: "Partition Spec ID",
      value: file.partition_spec_id?.toString() ?? "",
    },
    { label: "Sort Order ID", value: file.sort_order_id?.toString() ?? "" },
  ];
  // v1 tables have no sequence numbers; the row is omitted rather than dashed.
  if (file.last_sequence_number !== null) {
    stats.push({
      label: "Last Sequence Number",
      value: String(file.last_sequence_number),
    });
  }
  return stats;
};

const nodeNotices = (
  file: MetadataFileNode,
  snapshot: SnapshotNode | undefined,
): DetailNotice[] => {
  const noticesOf = (
    kind: DetailNotice["kind"],
    texts: string[],
  ): DetailNotice[] => texts.map((text) => ({ kind, text }));
  const candidates = [
    ...noticesOf("error", file.errors),
    ...noticesOf("error", snapshot?.errors ?? []),
    ...noticesOf("warning", file.warnings),
    ...noticesOf("warning", snapshot?.warnings ?? []),
  ];

  const seenTexts = new Set<string>();
  const notices: DetailNotice[] = [];
  for (const { kind, text } of candidates) {
    if (text !== "" && !seenTexts.has(text)) {
      seenTexts.add(text);
      notices.push({ kind, text });
    }
  }
  return notices;
};

const gapNotices = (
  row: TimelineRow,
  hasReadablePrevious: boolean,
): DetailNotice[] => {
  const count = row.unreadableCommitsBefore;
  if (count === 0) {
    return [];
  }
  const eventsText =
    count === 1
      ? "1 event right before this one"
      : `${count.toString()} events right before this one`;
  const consequence = hasReadablePrevious
    ? "This event is compared with the last readable one, so the changes shown may include theirs."
    : "No earlier event could be read, so what this event changed cannot be shown.";
  const fileLines = row.unreadableFilesBefore.map((file) =>
    file.reasons.length === 0
      ? file.path
      : `${file.path}: ${file.reasons.join("; ")}`,
  );
  return [
    {
      kind: "warning",
      text: [
        `${eventsText} could not be read. ${consequence}`,
        ...fileLines,
      ].join("\n"),
    },
  ];
};

const statisticsPaths = (
  kind: string,
  files: MetadataFileNode["pointed_table_statistics_files"],
): DetailRowData[] =>
  (files ?? []).map((statisticsFile) => ({
    label: `${kind} statistics, snapshot ${statisticsFile["snapshot-id"]}`,
    value: statisticsFile["statistics-path"],
    isCopyable: true,
  }));

const linkedPaths = (file: MetadataFileNode): DetailRowData[] => [
  ...(file.previous_file === null
    ? []
    : [
        {
          label: "Previous metadata file",
          value: file.previous_file,
          isCopyable: true,
        },
      ]),
  ...statisticsPaths("Table", file.pointed_table_statistics_files),
  ...statisticsPaths("Partition", file.pointed_partition_statistics_files),
];

const REF_RETENTION_FIELDS = [
  { key: "min-snapshots-to-keep", label: "min snapshots to keep" },
  { key: "max-snapshot-age-ms", label: "max snapshot age" },
  { key: "max-ref-age-ms", label: "max ref age" },
] as const;

const refRows = (file: MetadataFileNode): DetailRowData[] =>
  Object.entries(file.refs).flatMap(([name, ref]) => [
    {
      id: JSON.stringify([ref.type, name]),
      label: `${ref.type} ${name}`,
      value: ref["snapshot-id"],
      isCopyable: true,
    },
    ...REF_RETENTION_FIELDS.flatMap(({ key, label }) => {
      const value = ref[key];
      if (value == null) {
        return [];
      }
      const text = String(value);
      return [
        {
          id: JSON.stringify([ref.type, name, key]),
          label: `${ref.type} ${name} ${label}`,
          value:
            key === "min-snapshots-to-keep"
              ? text
              : `${formatDurationMs(text)} (${text} ms)`,
        },
      ];
    }),
  ]);

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
    notices: [
      ...gapNotices(row, previousFile !== undefined),
      ...nodeNotices(file, snapshot),
    ],
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
    metadataFile: {
      path: file.file_path,
      linkedPaths: linkedPaths(file),
      stats: fileStats(file),
    },
    refs: refRows(file),
    properties: toRows(Object.entries(file.properties)),
    rawDiff:
      previousFile === undefined ? [] : diffMetadataFiles(previousFile, file),
  };
};
