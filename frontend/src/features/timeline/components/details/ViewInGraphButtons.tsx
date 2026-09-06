import { useViewInGraph } from "../../../../hooks/useViewInGraph";
import GraphLinkButton from "./GraphLinkButton";

interface ViewInGraphButtonsProps {
  snapshotFilePath: string | null;
  metadataFilePath: string;
}

const ViewInGraphButtons = ({
  snapshotFilePath,
  metadataFilePath,
}: ViewInGraphButtonsProps) => {
  const { viewInGraph } = useViewInGraph();

  return (
    <div className="flex items-center justify-center gap-2 border-b border-edge pb-4">
      {snapshotFilePath !== null && (
        <GraphLinkButton
          label="Snapshot"
          onOpen={(event) => {
            viewInGraph(event, snapshotFilePath);
          }}
        />
      )}
      <GraphLinkButton
        label="Metadata"
        onOpen={(event) => {
          viewInGraph(event, metadataFilePath);
        }}
      />
    </div>
  );
};

export default ViewInGraphButtons;
