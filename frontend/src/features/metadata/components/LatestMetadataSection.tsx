import { useQuery } from "@tanstack/react-query";
import { tableMetadataQueryOptions } from "../../table/api/tableMetadataQueries";
import { useTableSpecs } from "../../specs/tableSpecs";
import LoadingIndicator from "../../../components/LoadingIndicator";
import MetadataOverview from "./MetadataOverview";

interface LatestMetadataSectionProps {
  tableName: string;
}

const LatestMetadataSection = ({ tableName }: LatestMetadataSectionProps) => {
  const metadataQuery = useQuery(tableMetadataQueryOptions(tableName));
  const { openSpec } = useTableSpecs();

  if (metadataQuery.isPending)
    return (
      <div className="flex justify-center py-10">
        <LoadingIndicator
          title="Loading metadata"
          description={`Reading the latest metadata file for ${tableName}.`}
        />
      </div>
    );

  if (metadataQuery.isError)
    return (
      <div className="rounded-xl border border-red-800 bg-red-950/50 p-5 text-red-400">
        <h2 className="font-bold">Failed to Load Metadata</h2>
        <p className="mt-2 text-sm">{metadataQuery.error.message}</p>
      </div>
    );

  return (
    <MetadataOverview
      metadata={metadataQuery.data}
      tableName={tableName}
      versionTitle="Latest metadata"
      onOpenSpec={openSpec}
    />
  );
};
export default LatestMetadataSection;
