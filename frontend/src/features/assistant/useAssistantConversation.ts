import { useEffect, useRef, useState } from "react";
import { useMutation } from "@tanstack/react-query";
import type { GraphRequestParameters } from "../table/api/graphCache";
import { askAssistant, type AssistantMessage } from "./api/assistantApi";
import type { AssistantPage } from "./assistantContext";
import {
  readConversation,
  saveConversation,
  selectMessagesToSend,
} from "./assistantConversation";

const UNDO_CLEAR_TIMEOUT_MS = 5000;

export interface AssistantExchange {
  question: string;
  answer: string;
}

export interface AssistantAsk {
  question: string;
  context: string;
  page: AssistantPage;
  shouldSendHistory: boolean;
}

export interface AssistantConversation {
  messages: AssistantMessage[];
  pendingExchange: AssistantExchange | null;
  isAnswering: boolean;
  errorMessage: string | null;
  historyCharacters: number;
  historyExchangeCount: number;
  canUndoClear: boolean;
  announcement: string;
  ask: (assistantAsk: AssistantAsk) => void;
  clear: () => void;
  undoClear: () => void;
  load: () => void;
}

const countCharacters = (messages: AssistantMessage[]): number =>
  messages.reduce(
    (characters, message) => characters + message.content.length,
    0,
  );

export const useAssistantConversation = (
  conversationKey: string,
  parameters: GraphRequestParameters,
): AssistantConversation => {
  const [messages, setMessages] = useState<AssistantMessage[]>([]);
  const [hasLoaded, setHasLoaded] = useState(false);
  const [announcement, setAnnouncement] = useState("");
  const [pendingExchange, setPendingExchange] =
    useState<AssistantExchange | null>(null);
  const [clearedMessages, setClearedMessages] = useState<
    AssistantMessage[] | null
  >(null);
  const abortControllerRef = useRef<AbortController | null>(null);
  const undoTimeoutRef = useRef<number | null>(null);
  const historyMessages = selectMessagesToSend(messages);

  useEffect(
    () => () => {
      abortControllerRef.current?.abort();
      if (undoTimeoutRef.current !== null)
        window.clearTimeout(undoTimeoutRef.current);
    },
    [],
  );

  const cancelUndoClear = (): void => {
    if (undoTimeoutRef.current !== null)
      window.clearTimeout(undoTimeoutRef.current);
    undoTimeoutRef.current = null;
    setClearedMessages(null);
  };

  const answerMutation = useMutation({
    mutationFn: async ({
      question,
      context,
      page,
      shouldSendHistory,
    }: AssistantAsk) => {
      const abortController = new AbortController();
      abortControllerRef.current = abortController;
      const answer = await askAssistant(
        {
          parameters,
          page,
          context,
          messages: [
            ...(shouldSendHistory ? historyMessages : []),
            { role: "user", content: question },
          ],
        },
        (partialAnswer) => {
          if (abortController.signal.aborted) return;
          setPendingExchange({ question, answer: partialAnswer });
        },
        abortController.signal,
      );
      abortController.signal.throwIfAborted();
      return answer;
    },
    onSuccess: (answer, { question }) => {
      const answeredMessages: AssistantMessage[] = [
        ...messages,
        { role: "user", content: question },
        { role: "assistant", content: answer },
      ];
      setMessages(answeredMessages);
      saveConversation(conversationKey, answeredMessages);
      setPendingExchange(null);
      setAnnouncement(answer);
    },
    onError: (error) => {
      if (error instanceof DOMException && error.name === "AbortError") return;
      setAnnouncement(error.message);
    },
  });

  const ask = (assistantAsk: AssistantAsk): void => {
    if (answerMutation.isPending) return;
    cancelUndoClear();
    setPendingExchange({ question: assistantAsk.question, answer: "" });
    setAnnouncement("Answering");
    answerMutation.mutate(assistantAsk);
  };

  const clear = (): void => {
    abortControllerRef.current?.abort();
    answerMutation.reset();
    setPendingExchange(null);
    setAnnouncement("");
    cancelUndoClear();
    if (messages.length > 0) {
      setClearedMessages(messages);
      undoTimeoutRef.current = window.setTimeout(() => {
        undoTimeoutRef.current = null;
        setClearedMessages(null);
      }, UNDO_CLEAR_TIMEOUT_MS);
    }
    setMessages([]);
    saveConversation(conversationKey, []);
  };

  const undoClear = (): void => {
    if (clearedMessages === null) return;
    setMessages(clearedMessages);
    saveConversation(conversationKey, clearedMessages);
    cancelUndoClear();
  };

  const load = (): void => {
    if (hasLoaded) return;
    setMessages(readConversation(conversationKey));
    setHasLoaded(true);
  };

  return {
    messages,
    pendingExchange,
    isAnswering: answerMutation.isPending,
    errorMessage: answerMutation.error?.message ?? null,
    historyCharacters: countCharacters(historyMessages),
    historyExchangeCount: historyMessages.length / 2,
    canUndoClear: clearedMessages !== null,
    announcement,
    ask,
    clear,
    undoClear,
    load,
  };
};
