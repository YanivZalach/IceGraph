import { useQuery } from "@tanstack/react-query";
import type { PartitionFiles } from "../../partition/api/partitionFilesSchemas";
import type { PartitionFilesRequest } from "../../partition/api/partitionFilesQueries";
import type { TableMetadata } from "../../table/api/metadataSchemas";
import { assistantStatusQueryOptions } from "../api/assistantApi";
import { createPartitionContextSource } from "../assistantContext";
import { getPartitionConversationKey } from "../assistantConversation";
import AssistantChat from "./AssistantChat";

interface PartitionAssistantProps {
  request: PartitionFilesRequest;
  partitionFiles: PartitionFiles;
  metadata: TableMetadata;
}

const PartitionAssistant = ({
  request,
  partitionFiles,
  metadata,
}: PartitionAssistantProps) => {
  const statusQuery = useQuery(assistantStatusQueryOptions());

  if (statusQuery.data?.enabled !== true) return null;

  const conversationKey = getPartitionConversationKey(
    request.tableName,
    request.snapshotId,
    request.filters,
  );

  return (
    <AssistantChat
      key={conversationKey}
      conversationKey={conversationKey}
      contextSource={createPartitionContextSource(partitionFiles, metadata)}
      maxContextCharacters={statusQuery.data.max_context_characters}
      parameters={{
        tableName: request.tableName,
        startSnapshotId: "",
        endSnapshotId: request.snapshotId,
      }}
    />
  );
};

export default PartitionAssistant;
