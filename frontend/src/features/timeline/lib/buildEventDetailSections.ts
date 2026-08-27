import type { MetadataFileNode, SnapshotNode } from "../api/nodeSchemas";
import {
  formatDayAndMonth,
  formatDayMonthYearAndClock,
} from "./format/formatTimelineTime";
import { formatShortId } from "./format/formatShortId";
import { groupSnapshotSummary } from "./groupSnapshotSummary";
import type { TimelineRow } from "./timelineRow";

export interface DetailRowData {
  label: string;
  value: string;
  isCopyable?: boolean;
}

export interface DetailSection {
  /** Empty string = the untitled block at the top of the panel. */
  title: string;
  rows: DetailRowData[];
}

const toRows = (entries: [string, string][]): DetailRowData[] =>
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

const fileRows = (file: MetadataFileNode): DetailRowData[] => [
  { label: "Path", value: file.file_path, isCopyable: true },
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

export const buildEventDetailSections = (
  row: TimelineRow,
  file: MetadataFileNode,
  snapshot: SnapshotNode | undefined,
  repointTarget: SnapshotNode | undefined,
): DetailSection[] => {
  const summary =
    snapshot === undefined ? null : groupSnapshotSummary(snapshot.summary);

  const branchRows: DetailRowData[] =
    row.branchName === null
      ? []
      : [{ label: "Branch Name", value: row.branchName }];

  const sections: DetailSection[] = [
    {
      title: "",
      rows: [
        ...branchRows,
        ...snapshotRows(row, snapshot),
        ...eventRows(row, repointTarget),
      ],
    },
    { title: "This change", rows: toRows(summary?.thisChange ?? []) },
    { title: "Table after", rows: toRows(summary?.tableAfter ?? []) },
    { title: "Engine", rows: toRows(summary?.engine ?? []) },
    { title: "Metadata file", rows: fileRows(file) },
    { title: "Refs", rows: refRows(file) },
    { title: "Properties", rows: toRows(Object.entries(file.properties)) },
  ];

  return sections.filter((section) => section.rows.length > 0);
};
