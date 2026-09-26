import type { TableMetadata } from "../../table/api/metadataSchemas";
import HelpTerm from "../../../shared/components/HelpTerm";
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
}: MetadataOverviewProps) => {
  const description = metadata.properties?.["icegraph.description"];

  return (
    <>
      <header>
        <p className="text-xs tracking-widest text-slate-400 uppercase">
          Table metadata
        </p>
        <h1 className="mt-1 break-all text-2xl font-semibold text-ink">
          {metadata["table-name"] ?? tableName}
        </h1>
        <section className="mt-4" aria-label="Table description">
          <h2 className="text-sm font-semibold text-ink">
            Table description{" ("}
            <HelpTerm label="How to add it">
              Set the table property with Spark SQL:
              <code className="mt-2 block whitespace-pre-wrap break-words font-mono">
                {
                  "ALTER TABLE my.table\nSET TBLPROPERTIES (\n  'icegraph.description' = 'Customer events'\n);"
                }
              </code>
            </HelpTerm>
            {")"}
          </h2>
          <p className="mt-1 whitespace-pre-wrap break-words text-sm text-slate-400">
            {typeof description === "string" && description.trim()
              ? description
              : "No table description recorded."}
          </p>
        </section>
      </header>
      <MetadataVersion
        title={versionTitle}
        path={metadata.metadata_file_path}
        updatedAt={metadata["last-updated-ms"]}
      />
      <MetadataSummary metadata={metadata} />
    </>
  );
};
export default MetadataOverview;
