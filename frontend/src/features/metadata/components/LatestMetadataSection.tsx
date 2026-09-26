import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { tableMetadataQueryOptions } from "../../table/api/tableMetadataQueries";
import { useTableSpecs } from "../../specs/tableSpecs";
import LoadingIndicator from "../../../components/LoadingIndicator";
import MetadataOverview from "./MetadataOverview";
import MetadataDetails from "./MetadataDetails";

interface LatestMetadataSectionProps {
  tableName: string;
  afterSummary: ReactNode;
}

const LatestMetadataSection = ({
  tableName,
  afterSummary,
}: LatestMetadataSectionProps) => {
  const metadataQuery = useQuery(tableMetadataQueryOptions(tableName));
  const { openSpec } = useTableSpecs();
  const metadata = metadataQuery.data;
  const metadataLoader = metadataQuery.isPending && (
    <div className="flex justify-center py-10">
      <LoadingIndicator
        title="Loading metadata"
        description={`Reading the latest metadata file for ${tableName}.`}
      />
    </div>
  );

  return (
    <>
      {metadata ? (
        <MetadataOverview
          metadata={metadata}
          tableName={tableName}
          versionTitle="Latest metadata"
        />
      ) : (
        metadataLoader
      )}
      {afterSummary}
      {metadata ? (
        <MetadataDetails metadata={metadata} onOpenSpec={openSpec} />
      ) : metadataQuery.isError ? (
        <div className="rounded-xl border border-red-800 bg-red-950/50 p-5 text-red-400">
          <h2 className="font-bold">Failed to Load Metadata</h2>
          <p className="mt-2 text-sm">{metadataQuery.error.message}</p>
        </div>
      ) : (
        <div aria-hidden="true">{metadataLoader}</div>
      )}
    </>
  );
};
export default LatestMetadataSection;
