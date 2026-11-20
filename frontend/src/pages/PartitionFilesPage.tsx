import PanelIssueNotice from "../components/PanelIssueNotice";
import { fileTreeContextSchema } from "../features/fileTree/schemas";
import PartitionFilesView from "../features/partition/PartitionFilesView";
import { usePartitionData } from "../features/partition/partitionData";

const PartitionFilesPage = () => {
  const { graphData, summary } = usePartitionData();
  const fileTreeContextResult = fileTreeContextSchema.safeParse(graphData);
  if (!fileTreeContextResult.success) {
    const validationErrors = fileTreeContextResult.error.issues.map(
      ({ message, path }) => `${path.join(".") || "data"}: ${message}`,
    );
    return (
      <div className="h-graph w-full bg-canvas p-6">
        <PanelIssueNotice type="error">
          {["Partition files data is invalid.", ...validationErrors]}
        </PanelIssueNotice>
      </div>
    );
  }

  return (
    <PartitionFilesView
      fileTreeContext={fileTreeContextResult.data}
      summary={summary}
    />
  );
};

export default PartitionFilesPage;
