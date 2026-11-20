import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Navigate } from "@tanstack/react-router";
import GraphCollectionChecklist from "../../../components/GraphCollectionChecklist";
import { UI_BODY_MUTED_CLASS } from "../../../uiTypography";
import {
  partitionFilesProgressQueryOptions,
  partitionFilesQueryOptions,
  type PartitionFilesRequest,
} from "../api/partitionFilesQueries";
import {
  formatPartitionFilters,
  partitionFilterSearch,
} from "../partitionFilters";
import PartitionFilesError from "./PartitionFilesError";

interface PartitionPreparationProps {
  request: PartitionFilesRequest;
  onBack: () => void;
}

const PartitionPreparation = ({
  request,
  onBack,
}: PartitionPreparationProps) => {
  const queryClient = useQueryClient();
  const partitionFilesQuery = useQuery(
    partitionFilesQueryOptions(request, queryClient),
  );
  const progressQuery = useQuery(partitionFilesProgressQueryOptions(request));

  if (partitionFilesQuery.isSuccess)
    return (
      <Navigate
        to="/partition/filetree"
        search={{
          table: request.tableName,
          snapshot: request.snapshotId,
          ...partitionFilterSearch(request.filters),
        }}
      />
    );

  if (partitionFilesQuery.isError)
    return (
      <PartitionFilesError
        error={partitionFilesQuery.error}
        actionLabel="Back to partition filter"
        onAction={onBack}
      />
    );

  return (
    <div aria-busy="true">
      <p className="text-base font-semibold text-ink-bright">
        Collecting partition files
      </p>
      <p className={`${UI_BODY_MUTED_CLASS} mt-1 break-all`}>
        {request.tableName} · snapshot {request.snapshotId}
      </p>
      <p className={`${UI_BODY_MUTED_CLASS} mt-1 mb-6 break-all font-mono`}>
        {formatPartitionFilters(request.filters)}
      </p>
      <GraphCollectionChecklist stages={progressQuery.data ?? null} />
    </div>
  );
};

export default PartitionPreparation;
