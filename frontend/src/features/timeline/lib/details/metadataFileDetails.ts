import type { MetadataFileNode } from "../../api/nodeSchemas";
import { formatDurationMs } from "../format/formatDuration";
import type { DetailRowData } from "./detailRowData";

export interface MetadataFileData {
  path: string;
  linkedPaths: DetailRowData[];
  stats: DetailRowData[];
}

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

export const refRows = (file: MetadataFileNode): DetailRowData[] =>
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

export const buildMetadataFileData = (
  file: MetadataFileNode,
): MetadataFileData => ({
  path: file.file_path,
  linkedPaths: linkedPaths(file),
  stats: fileStats(file),
});
