import { useQuery, useQueryClient } from "@tanstack/react-query";
import { Navigate } from "@tanstack/react-router";
import {
  graphProgressQueryOptions,
  graphQueryOptions,
} from "../api/graphQueries";
import type { GraphRequestParameters } from "../api/graphCache";
import GraphCollectionChecklist from "../../../components/GraphCollectionChecklist";
import { UI_BODY_MUTED_CLASS } from "../../../uiTypography";

interface GraphPreparationProps {
  parameters: GraphRequestParameters;
  onBack: () => void;
}

const GraphPreparation = ({ parameters, onBack }: GraphPreparationProps) => {
  const queryClient = useQueryClient();
  const graphQuery = useQuery(graphQueryOptions(parameters, queryClient));
  const progressQuery = useQuery(graphProgressQueryOptions(parameters));
  const stages = progressQuery.data ?? null;

  if (graphQuery.isSuccess)
    return (
      <Navigate
        to="/table/timeline"
        search={{
          table: parameters.tableName,
          ...(parameters.startSnapshotId === ""
            ? {}
            : { start_snapshot_id: parameters.startSnapshotId }),
          ...(parameters.endSnapshotId === ""
            ? {}
            : { end_snapshot_id: parameters.endSnapshotId }),
        }}
      />
    );

  if (graphQuery.isError)
    return (
      <div className="rounded-xl border border-red-800 bg-red-950/50 p-6 text-red-400">
        <h2 className="font-bold">Failed to Prepare Graph</h2>
        {stages && (
          <div className="my-5 rounded-lg border border-red-900/50 bg-canvas/40 p-4">
            <GraphCollectionChecklist stages={stages} />
          </div>
        )}
        <p className="mt-2 text-sm break-words">{graphQuery.error.message}</p>
        <button
          type="button"
          onClick={onBack}
          className="mt-5 cursor-pointer rounded-lg bg-accent px-5 py-2.5 text-sm font-bold text-white transition hover:bg-accent-dark"
        >
          Back to snapshot selection
        </button>
      </div>
    );

  return (
    <div aria-busy="true">
      <p className="text-base font-semibold text-ink-bright">
        Preparing table graph
      </p>
      <p className={`${UI_BODY_MUTED_CLASS} mt-1 mb-6 break-all`}>
        {parameters.tableName}
      </p>
      <GraphCollectionChecklist stages={stages} />
    </div>
  );
};
export default GraphPreparation;
