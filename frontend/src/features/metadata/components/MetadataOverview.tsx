import type { TableMetadata } from "../../table/api/metadataSchemas";
import MetadataVersion from "./MetadataVersion";
import MetadataSummary from "./MetadataSummary";

interface MetadataOverviewProps {
  metadata: TableMetadata;
  tableName: string | undefined;
  versionTitle: string;
}

const MetadataOverview = ({
  metadata,
  tableName,
  versionTitle,
}: MetadataOverviewProps) => (
  <>
    <header>
      <p className="text-xs tracking-widest text-slate-400 uppercase">
        Table metadata
      </p>
      <h1 className="mt-1 break-all text-2xl font-semibold text-ink">
        {metadata["table-name"] ?? tableName}
      </h1>
    </header>
    <MetadataVersion
      title={versionTitle}
      path={metadata.metadata_file_path}
      updatedAt={metadata["last-updated-ms"]}
    />
    <MetadataSummary metadata={metadata} />
  </>
);
export default MetadataOverview;
