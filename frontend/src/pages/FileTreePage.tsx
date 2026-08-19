import { useOutletContext } from "react-router-dom";
import FileTreeView from "../features/fileTree/FileTreeView";
import { fileTreeContextSchema } from "../features/fileTree/schemas";

const FileTreePage = () => {
  const rawGraphData: unknown = useOutletContext();
  const graphData = fileTreeContextSchema.parse(rawGraphData);

  return <FileTreeView graphData={graphData} />;
};

export default FileTreePage;
