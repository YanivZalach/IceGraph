import type {
  MetadataFileNode,
  SnapshotNode,
  SnapshotSummary,
} from "../../api/nodeSchemas";
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
  pairChangeCounts,
  type BeforeAfterRow,
  type ChangeCountRow,
} from "./summaryTables";
import type { TimelineRow } from "../timelineRow";

export interface DetailRowData {
  label: string;
  value: string;
  isCopyable?: boolean;
}

export interface ThisChangeData {
  counts: ChangeCountRow[];
  rest: DetailRowData[];
}

export interface MetadataFileData {
  path: string;
  stats: DetailRowData[];
}

/** Field order mirrors the panel's display order, but the JSX in EventDetails decides it. */
export interface EventDetailData {
  topRows: DetailRowData[];
  thisChange: ThisChangeData;
  tableState: BeforeAfterRow[];
  engine: DetailRowData[];
  metadataFile: MetadataFileData;
  refs: DetailRowData[];
  properties: DetailRowData[];
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
            label: "Snapshot ID",
            value: `${row.snapshotId} (unavailable)`,
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

const refRows = (file: MetadataFileNode): DetailRowData[] =>
  Object.entries(file.refs).map(([name, ref]) => ({
    label: `${ref.type} ${name}`,
    value: ref["snapshot-id"],
    isCopyable: true,
  }));

export const buildEventDetails = (
  row: TimelineRow,
  file: MetadataFileNode,
  snapshot: SnapshotNode | undefined,
  repointTarget: SnapshotNode | undefined,
  parentSummary: SnapshotSummary | null,
): EventDetailData => {
  const summary =
    snapshot === undefined ? null : groupSnapshotSummary(snapshot.summary);
  const { paired, rest } = pairChangeCounts(summary?.thisChange ?? []);

  const branchRows: DetailRowData[] =
    row.branchName === null
      ? []
      : [{ label: "Branch Name", value: row.branchName }];

  return {
    topRows: [
      ...branchRows,
      ...snapshotRows(row, snapshot),
      ...eventRows(row, repointTarget),
    ],
    thisChange: { counts: paired, rest: toRows(rest) },
    tableState: buildBeforeAfterRows(summary?.tableAfter ?? [], parentSummary),
    engine: toRows(summary?.engine ?? []),
    metadataFile: { path: file.file_path, stats: fileStats(file) },
    refs: refRows(file),
    properties: toRows(Object.entries(file.properties)),
  };
};
