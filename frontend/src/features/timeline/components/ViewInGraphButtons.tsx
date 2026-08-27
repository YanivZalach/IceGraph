import { useViewInGraph } from "../../../hooks/useViewInGraph";
import GraphLinkButton from "./GraphLinkButton";

interface ViewInGraphButtonsProps {
  snapshotFilePath: string | null;
  metadataFilePath: string;
}

const ViewInGraphButtons = ({
  snapshotFilePath,
  metadataFilePath,
}: ViewInGraphButtonsProps) => {
  const { viewInGraph, duplicatingNodeId, canViewInGraph } = useViewInGraph();

  if (!canViewInGraph) {
    return null;
  }

  return (
    <div className="flex items-center justify-center gap-2 border-b border-edge pb-4">
      {snapshotFilePath !== null && (
        <GraphLinkButton
          label="Snapshot"
          isLoading={duplicatingNodeId === snapshotFilePath}
          isDisabled={duplicatingNodeId !== null}
          onOpen={(event) => void viewInGraph(event, snapshotFilePath)}
        />
      )}
      <GraphLinkButton
        label="Metadata"
        isLoading={duplicatingNodeId === metadataFilePath}
        isDisabled={duplicatingNodeId !== null}
        onOpen={(event) => void viewInGraph(event, metadataFilePath)}
      />
    </div>
  );
};

export default ViewInGraphButtons;
