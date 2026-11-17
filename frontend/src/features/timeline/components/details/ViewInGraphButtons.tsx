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
    <div className="flex flex-col items-center gap-2 border-b border-edge pb-4">
      <span className="text-xs text-slate-400">View in graph</span>
      <div className="flex items-center justify-center gap-2">
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
    </div>
  );
};

export default ViewInGraphButtons;
