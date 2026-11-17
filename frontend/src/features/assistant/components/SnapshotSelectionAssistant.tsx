import { useInfiniteQuery, useQuery } from "@tanstack/react-query";
import { snapshotMapQueryOptions } from "../../snapshots/api/snapshotQueries";
import type { SnapshotRange } from "../../snapshots/snapshotRange";
import { tableMetadataQueryOptions } from "../../table/api/tableMetadataQueries";
import { assistantStatusQueryOptions } from "../api/assistantApi";
import { createSnapshotSelectionContextSource } from "../assistantContext";
import { getSnapshotSelectionConversationKey } from "../assistantConversation";
import AssistantChat from "./AssistantChat";

interface SnapshotSelectionAssistantProps {
  tableName: string;
  range: SnapshotRange;
}

const SnapshotSelectionAssistant = ({
  tableName,
  range,
}: SnapshotSelectionAssistantProps) => {
  const statusQuery = useQuery(assistantStatusQueryOptions());
  const metadataQuery = useQuery(tableMetadataQueryOptions(tableName));
  const snapshotQuery = useInfiniteQuery(snapshotMapQueryOptions(tableName));

  if (
    statusQuery.data?.enabled !== true ||
    (metadataQuery.data === undefined && snapshotQuery.data === undefined)
  ) {
    return null;
  }

  const conversationKey = getSnapshotSelectionConversationKey(tableName);

  return (
    <AssistantChat
      key={conversationKey}
      conversationKey={conversationKey}
      contextSource={createSnapshotSelectionContextSource(
        metadataQuery.data,
        snapshotQuery.data?.pages,
      )}
      maxContextCharacters={statusQuery.data.max_context_characters}
      parameters={{
        tableName,
        startSnapshotId: range.startSnapshotId,
        endSnapshotId: range.endSnapshotId,
      }}
    />
  );
};

export default SnapshotSelectionAssistant;
