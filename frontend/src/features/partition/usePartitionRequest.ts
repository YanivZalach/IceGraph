import { useNavigate, useSearch } from "@tanstack/react-router";
import type { PartitionFilesRequest } from "./api/partitionFilesQueries";
import {
  partitionFilterSearch,
  readPartitionFilters,
} from "./partitionFilters";

interface PartitionRequestState {
  request: PartitionFilesRequest;
  editFilter: () => void;
}

export const usePartitionRequest = (): PartitionRequestState => {
  const search = useSearch({ from: "/partition" });
  const navigate = useNavigate();
  const request = {
    tableName: search.table ?? "",
    snapshotId: search.snapshot ?? "",
    filters: readPartitionFilters(search),
  };

  return {
    request,
    editFilter: () => {
      void navigate({
        to: "/snapshots-selection",
        search: {
          table: request.tableName,
          partition_snapshot: request.snapshotId,
          ...partitionFilterSearch(request.filters),
        },
      });
    },
  };
};
