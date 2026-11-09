import PanelIssueNotice from "../components/PanelIssueNotice";
import GraphView from "../features/graph/GraphView";
import { graphViewDataSchema } from "../features/graph/graphViewSchemas";
import { buildFileGraph } from "../features/graph/model/graphModel";
import { useTableGraphData } from "../features/table/tableGraphData";

const GraphPage = () => {
  const rawGraphData: unknown = useTableGraphData();
  const graphDataResult = graphViewDataSchema.safeParse(rawGraphData);
  if (!graphDataResult.success) {
    const validationErrors = graphDataResult.error.issues.map(
      ({ message, path }) => `${path.join(".") || "data"}: ${message}`,
    );
    return (
      <div className="h-graph bg-canvas p-6">
        <PanelIssueNotice type="error">
          {["Graph data is invalid.", ...validationErrors]}
        </PanelIssueNotice>
      </div>
    );
  }

  return <GraphView fileGraph={buildFileGraph(graphDataResult.data)} />;
};

export default GraphPage;
