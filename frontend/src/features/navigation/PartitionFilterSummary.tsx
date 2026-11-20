import { formatPartitionFilters } from "../partition/partitionFilters";
import { usePartitionRequest } from "../partition/usePartitionRequest";

const PartitionFilterSummary = () => {
  const { request } = usePartitionRequest();
  const filterText = formatPartitionFilters(request.filters);

  return (
    <div className="flex items-center gap-3 xl:gap-4">
      <div className="h-4 w-px shrink-0 bg-slate-700" />
      <span
        className="max-w-36 truncate font-mono text-xs text-slate-500"
        title={`Snapshot ${request.snapshotId}`}
      >
        snapshot {request.snapshotId}
      </span>
      <span
        className="shrink-0 whitespace-nowrap font-mono text-xs text-slate-300"
        title={filterText}
      >
        {filterText}
      </span>
    </div>
  );
};

export default PartitionFilterSummary;
