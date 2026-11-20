import { Suspense } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Outlet } from "@tanstack/react-router";
import GraphCollectionChecklist from "../components/GraphCollectionChecklist";
import PartitionAssistant from "../features/assistant/components/PartitionAssistant";
import PageLoader from "../components/PageLoader";
import SpecDetailsOverlay from "../features/specs/components/SpecDetailsOverlay";
import {
  partitionFilesProgressQueryOptions,
  partitionFilesQueryOptions,
} from "../features/partition/api/partitionFilesQueries";
import PartitionFilesError from "../features/partition/components/PartitionFilesError";
import {
  PartitionDataContext,
  buildPartitionData,
} from "../features/partition/partitionData";
import { formatPartitionFilters } from "../features/partition/partitionFilters";
import { usePartitionRequest } from "../features/partition/usePartitionRequest";
import { tableMetadataQueryOptions } from "../features/table/api/tableMetadataQueries";
import { UI_BODY_MUTED_CLASS } from "../uiTypography";

const PartitionLayout = () => {
  const queryClient = useQueryClient();
  const { request, editFilter } = usePartitionRequest();
  const partitionFilesQuery = useQuery(
    partitionFilesQueryOptions(request, queryClient),
  );
  const progressQuery = useQuery(partitionFilesProgressQueryOptions(request));
  const metadataQuery = useQuery(tableMetadataQueryOptions(request.tableName));

  if (
    request.tableName === "" ||
    request.snapshotId === "" ||
    request.filters.length === 0
  ) {
    return (
      <div className="flex flex-1 items-center justify-center bg-canvas p-6">
        <div className="w-full max-w-2xl">
          <PartitionFilesError
            error={
              new Error(
                "A table, a snapshot, and at least one partition field value are required.",
              )
            }
            actionLabel="Edit filter"
            onAction={editFilter}
          />
        </div>
      </div>
    );
  }

  const error = partitionFilesQuery.error ?? metadataQuery.error;
  if (error !== null) {
    return (
      <div className="flex flex-1 items-center justify-center bg-canvas p-6">
        <div className="w-full max-w-2xl">
          <PartitionFilesError
            error={error}
            actionLabel="Edit filter"
            onAction={editFilter}
          />
        </div>
      </div>
    );
  }

  if (
    partitionFilesQuery.data === undefined ||
    metadataQuery.data === undefined
  ) {
    return (
      <div className="flex flex-1 flex-col items-center justify-center bg-canvas">
        <div
          className="w-full max-w-md rounded-2xl border border-edge bg-surface/80 p-7 shadow-2xl shadow-black/20 backdrop-blur-sm"
          aria-busy="true"
        >
          <div className="mb-6">
            <p className="text-base font-semibold text-ink-bright">
              Collecting partition files
            </p>
            <p className={`${UI_BODY_MUTED_CLASS} mt-1 break-all`}>
              {request.tableName} · snapshot {request.snapshotId}
            </p>
            <p className={`${UI_BODY_MUTED_CLASS} mt-1 break-all font-mono`}>
              {formatPartitionFilters(request.filters)}
            </p>
          </div>
          <GraphCollectionChecklist stages={progressQuery.data ?? null} />
        </div>
      </div>
    );
  }

  return (
    <div className="relative flex flex-1 overflow-hidden">
      <PartitionDataContext.Provider
        value={buildPartitionData(partitionFilesQuery.data, metadataQuery.data)}
      >
        <Suspense fallback={<PageLoader />}>
          <Outlet />
        </Suspense>
      </PartitionDataContext.Provider>
      <SpecDetailsOverlay />
      <PartitionAssistant
        request={request}
        partitionFiles={partitionFilesQuery.data}
        metadata={metadataQuery.data}
      />
    </div>
  );
};

export default PartitionLayout;
