import type { TableMetadata } from "../../table/api/metadataSchemas";
import PartitionDistributionTable from "../../table/components/PartitionDistributionTable";
import PanelIssueNotice from "../../../components/PanelIssueNotice";
import { formatCount, integerText } from "../metadataPresentation";

interface MetadataPartitionStatisticsProps {
  metadata: TableMetadata;
}

const MetadataPartitionStatistics = ({
  metadata,
}: MetadataPartitionStatisticsProps) => {
  const statistics = metadata["current-partition-statistics"];
  if (!statistics) return null;

  return (
    <section
      aria-labelledby="metadata-partition-statistics-title"
      className="space-y-3"
    >
      <h2
        id="metadata-partition-statistics-title"
        className="text-base font-semibold text-ink"
      >
        Partition statistics
      </h2>
      <PanelIssueNotice type="error">{statistics.errors}</PanelIssueNotice>
      <PanelIssueNotice type="warning">{statistics.warnings}</PanelIssueNotice>
      <dl className="grid grid-cols-2 divide-x divide-edge rounded-xl border border-edge bg-surface">
        <div className="p-5">
          <dt className="text-xs text-slate-400">Partitions</dt>
          <dd className="mt-2 text-2xl font-semibold text-ink">
            {formatCount(integerText(statistics.partitions_count))}
          </dd>
        </div>
        <div className="p-5">
          <dt className="text-xs text-slate-400">Partitions with deletes</dt>
          <dd className="mt-2 text-2xl font-semibold text-ink">
            {formatCount(integerText(statistics.partitions_with_deletes))}
          </dd>
        </div>
      </dl>
      <PartitionDistributionTable
        partitionDistribution={statistics.partition_distribution}
      />
    </section>
  );
};

export default MetadataPartitionStatistics;
