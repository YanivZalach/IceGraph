import { useSearch } from "@tanstack/react-router";
import { useTableSpecs } from "../features/specs/tableSpecs";
import { useMetadataKeyboardScroll } from "../features/metadata/useMetadataKeyboardScroll";
import MetadataOverview from "../features/metadata/components/MetadataOverview";

const MetadataPage = () => {
  const search = useSearch({ from: "/table/metadata" });
  const { graphQuery, openSpec, detailsOpen, issuesOpen } = useTableSpecs();
  useMetadataKeyboardScroll(detailsOpen || issuesOpen);
  if (!graphQuery.data) return null;
  return (
    <div className="min-w-0 flex-1 bg-canvas">
      <main className="mx-auto flex max-w-5xl flex-col gap-7 px-5 py-8 md:px-8">
        <MetadataOverview
          metadata={graphQuery.data.metadata}
          tableName={search.table}
          versionTitle="Latest metadata in the selected range"
          onOpenSpec={openSpec}
        />
      </main>
    </div>
  );
};
export default MetadataPage;
