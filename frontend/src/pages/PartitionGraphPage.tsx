import PanelIssueNotice from "../components/PanelIssueNotice";
import GraphView from "../features/graph/GraphView";
import { graphViewDataSchema } from "../features/graph/graphViewSchemas";
import { buildFileGraph } from "../features/graph/model/graphModel";
import { usePartitionData } from "../features/partition/partitionData";

const PartitionGraphPage = () => {
  const { graphData, summary } = usePartitionData();
  const graphDataResult = graphViewDataSchema.safeParse(graphData);
  if (!graphDataResult.success) {
    const validationErrors = graphDataResult.error.issues.map(
      ({ message, path }) => `${path.join(".") || "data"}: ${message}`,
    );
    return (
      <div className="h-graph w-full bg-canvas p-6">
        <PanelIssueNotice type="error">
          {["Partition graph data is invalid.", ...validationErrors]}
        </PanelIssueNotice>
      </div>
    );
  }

  return (
    <GraphView
      fileGraph={buildFileGraph(graphDataResult.data)}
      tableName={summary.table_name}
    />
  );
};

export default PartitionGraphPage;
