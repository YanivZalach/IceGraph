import { useQuery } from "@tanstack/react-query";
import { useSearch } from "@tanstack/react-router";
import { useTableSpecs } from "../../specs/tableSpecs";
import { assistantStatusQueryOptions } from "../api/assistantApi";
import { createGraphContextSource } from "../assistantContext";
import { getConversationKey } from "../assistantConversation";
import AssistantChat from "./AssistantChat";

const Assistant = () => {
  const search = useSearch({ strict: false });
  const { graphQuery } = useTableSpecs();
  const statusQuery = useQuery(assistantStatusQueryOptions());

  if (statusQuery.data?.enabled !== true || graphQuery.data === undefined) {
    return null;
  }

  const parameters = {
    tableName: typeof search.table === "string" ? search.table : "",
    startSnapshotId:
      typeof search.start_snapshot_id === "string"
        ? search.start_snapshot_id
        : "",
    endSnapshotId:
      typeof search.end_snapshot_id === "string" ? search.end_snapshot_id : "",
  };

  const conversationKey = getConversationKey(parameters);

  return (
    <AssistantChat
      key={conversationKey}
      conversationKey={conversationKey}
      contextSource={createGraphContextSource(graphQuery.data)}
      maxContextCharacters={statusQuery.data.max_context_characters}
      parameters={parameters}
    />
  );
};

export default Assistant;
