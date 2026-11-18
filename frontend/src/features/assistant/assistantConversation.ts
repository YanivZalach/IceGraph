import { z } from "zod";
import type { GraphRequestParameters } from "../table/api/graphCache";
import {
  assistantMessageSchema,
  type AssistantMessage,
} from "./api/assistantApi";

const CONVERSATION_KEY_PREFIX = "assistant-conversation:v1:";
const MAX_SAVED_MESSAGES = 50;
const MAX_SENT_MESSAGES = 12;

const conversationSchema = z.array(assistantMessageSchema);

export const getConversationKey = (
  parameters: GraphRequestParameters,
): string =>
  `${CONVERSATION_KEY_PREFIX}${JSON.stringify([
    parameters.tableName,
    parameters.startSnapshotId,
    parameters.endSnapshotId,
  ])}`;

// The selected range changes while the user picks it, so this page keeps one
// conversation per table. Its 2-item array never equals the 3-item key above.
export const getSnapshotSelectionConversationKey = (
  tableName: string,
): string =>
  `${CONVERSATION_KEY_PREFIX}${JSON.stringify([tableName, "snapshot-selection"])}`;

export const readConversation = (
  conversationKey: string,
): AssistantMessage[] => {
  try {
    const savedConversation = localStorage.getItem(conversationKey);
    if (savedConversation === null) return [];
    const parsedConversation = conversationSchema.safeParse(
      JSON.parse(savedConversation),
    );
    return parsedConversation.success ? parsedConversation.data : [];
  } catch (storageError) {
    console.warn("Failed to read the assistant conversation", storageError);
    return [];
  }
};

export const saveConversation = (
  conversationKey: string,
  messages: AssistantMessage[],
): void => {
  try {
    if (messages.length === 0) {
      localStorage.removeItem(conversationKey);
      return;
    }
    localStorage.setItem(
      conversationKey,
      JSON.stringify(messages.slice(-MAX_SAVED_MESSAGES)),
    );
  } catch (storageError) {
    console.warn("Failed to save the assistant conversation", storageError);
  }
};

// Questions and answers are saved in pairs, so an even tail starts with a question.
export const selectMessagesToSend = (
  messages: AssistantMessage[],
): AssistantMessage[] => messages.slice(-MAX_SENT_MESSAGES);
