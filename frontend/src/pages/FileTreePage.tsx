import { useOutletContext } from "react-router-dom";
import PanelIssueNotice from "../components/PanelIssueNotice";
import FileTreeView from "../features/fileTree/FileTreeView";
import { fileTreeContextSchema } from "../features/fileTree/schemas";

const FileTreePage = () => {
  const rawGraphData: unknown = useOutletContext();
  const graphDataResult = fileTreeContextSchema.safeParse(rawGraphData);
  if (!graphDataResult.success) {
    const validationErrors = graphDataResult.error.issues
      .map(({ message, path }) => `${path.join(".") || "data"}: ${message}`)
      .join("\n");
    return (
      <div className="h-graph bg-canvas p-6">
        <PanelIssueNotice type="error">
          {`File tree data is invalid.\n${validationErrors}`}
        </PanelIssueNotice>
      </div>
    );
  }

  return <FileTreeView graphData={graphDataResult.data} />;
};

export default FileTreePage;
