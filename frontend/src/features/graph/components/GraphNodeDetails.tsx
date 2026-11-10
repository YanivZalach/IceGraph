import DataFileReadableMetricsTable from "../../../components/DataFileReadableMetricsTable";
import { PanelDetailRow } from "../../../components/PanelContent";
import ReadableMetricsSummary from "../../../components/ReadableMetricsSummary";
import { FileType } from "../../../graphConstants";
import { isEmptyValue } from "../../../shared/lib/isEmptyValue";
import PartitionDistributionTable from "../../table/components/PartitionDistributionTable";
import PartitionStatisticsTable from "../../table/components/PartitionStatisticsTable";
import PartitionValues, {
  type ManifestPartitionFilter,
} from "../../table/components/PartitionValues";
import type { GraphNodeDetails as Details } from "../graphViewSchemas";

const HIDDEN_DETAIL_KEYS = new Set([
  "type",
  "node_id",
  "errors",
  "warnings",
  "readable_metrics",
  "virtual_read",
  "sampled_partitions",
  "partition_distribution",
]);

const READABLE_METRICS_FILE_TYPES = new Set<string>([
  FileType.DATA,
  FileType.POSITION_DELETE,
  FileType.DELETION_VECTOR,
  FileType.EQUALITY_DELETE,
]);

const PARTITION_KEYS = new Set(["partition", "partitions"]);

const parseFileSizeBytes = (details: Details): number | null => {
  const rawFileSize = details.file_size_in_bytes;
  if (rawFileSize === undefined || rawFileSize === null) return null;
  const fileSizeBytes = Number(rawFileSize);
  return Number.isSafeInteger(fileSizeBytes) && fileSizeBytes >= 0
    ? fileSizeBytes
    : null;
};

interface GraphNodeDetailsProps {
  nodeId: string;
  details: Details;
  isRelaxedCollapse: boolean;
  partitionFilter: ManifestPartitionFilter;
}

const DetailRows = ({
  nodeId,
  details,
  isRelaxedCollapse,
  partitionFilter,
}: GraphNodeDetailsProps) =>
  Object.entries(details)
    .filter(([key]) => !HIDDEN_DETAIL_KEYS.has(key.toLowerCase()))
    .filter(([key, value]) => PARTITION_KEYS.has(key) || !isEmptyValue(value))
    .map(([key, value]) => {
      const rowKey = `${nodeId}:${key}`;
      if (key === "partition" && Array.isArray(details.partition)) {
        return (
          <PartitionValues key={rowKey} kind="file" value={details.partition} />
        );
      }
      if (key === "partitions" && Array.isArray(details.partitions)) {
        return (
          <PartitionValues
            key={rowKey}
            kind="manifest"
            value={details.partitions}
            partitionFilter={partitionFilter}
          />
        );
      }
      return (
        <PanelDetailRow
          key={rowKey}
          label={key}
          value={value}
          relaxedCollapse={isRelaxedCollapse}
        />
      );
    });

const PartitionStatistics = ({ details }: { details: Details }) => {
  if (details.type !== FileType.PARTITION_STATISTICS) return null;
  const partitionsCount = details.partitions_count;
  if (partitionsCount === undefined || partitionsCount === null) return null;

  return (
    <>
      {details.partition_distribution && (
        <PartitionDistributionTable
          partitionDistribution={details.partition_distribution}
        />
      )}
      {Array.isArray(details.sampled_partitions) && (
        <PartitionStatisticsTable
          partitionsCount={partitionsCount}
          sampledPartitions={details.sampled_partitions}
        />
      )}
    </>
  );
};

const ReadableMetrics = ({ details }: { details: Details }) => {
  const readableMetrics = details.readable_metrics;
  if (
    details.type === undefined ||
    !READABLE_METRICS_FILE_TYPES.has(details.type) ||
    !readableMetrics ||
    Object.keys(readableMetrics).length === 0
  ) {
    return null;
  }
  const totalFileSizeBytes = parseFileSizeBytes(details);

  return (
    <>
      <ReadableMetricsSummary
        readableMetrics={readableMetrics}
        sizeScope="file"
        totalFileSizeBytes={totalFileSizeBytes}
      />
      <DataFileReadableMetricsTable
        readableMetrics={readableMetrics}
        sizeScope="file"
        totalFileSizeBytes={totalFileSizeBytes}
      />
    </>
  );
};

const GraphNodeDetails = (props: GraphNodeDetailsProps) => (
  <>
    <DetailRows
      nodeId={props.nodeId}
      details={props.details}
      isRelaxedCollapse={props.isRelaxedCollapse}
      partitionFilter={props.partitionFilter}
    />
    <PartitionStatistics details={props.details} />
    <ReadableMetrics details={props.details} />
  </>
);

export default GraphNodeDetails;
