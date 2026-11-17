import { useEffect, useRef } from "react";
import type { AssistantConversation } from "../useAssistantConversation";
import AssistantAnswer from "./AssistantAnswer";

interface AssistantMessagesProps {
  conversation: AssistantConversation;
  suggestedQuestions: readonly string[];
  canAsk: boolean;
  onAsk: (question: string) => void;
}

const FOLLOW_BOTTOM_THRESHOLD_PX = 48;

const Question = ({ content }: { content: string }) => (
  <div className="max-w-[85%] self-end rounded-2xl rounded-br-sm bg-accent-muted px-3.5 py-2 text-sm leading-6 whitespace-pre-wrap break-words text-ink-bright">
    {content}
  </div>
);

const AssistantMessages = ({
  conversation,
  suggestedQuestions,
  canAsk,
  onAsk,
}: AssistantMessagesProps) => {
  const { messages, pendingExchange, isAnswering, errorMessage } = conversation;
  const listRef = useRef<HTMLDivElement>(null);
  const isFollowingBottomRef = useRef(true);
  const pendingAnswer = pendingExchange?.answer ?? "";

  useEffect(() => {
    const list = listRef.current;
    if (list !== null && isFollowingBottomRef.current) {
      list.scrollTop = list.scrollHeight;
    }
  }, [messages, pendingExchange, errorMessage]);

  const handleScroll = (): void => {
    const list = listRef.current;
    if (list === null) return;
    isFollowingBottomRef.current =
      list.scrollHeight - list.scrollTop - list.clientHeight <
      FOLLOW_BOTTOM_THRESHOLD_PX;
  };

  if (messages.length === 0 && pendingExchange === null) {
    return (
      <div className="flex min-h-0 flex-1 flex-col gap-1.5 overflow-y-auto p-4">
        <p className="mt-auto mb-2 text-[13px] leading-5 text-slate-400">
          Ask about this table. The context chips below choose which parts of
          the table's loaded metadata are sent to the AI model with your
          question.
        </p>
        {suggestedQuestions.map((question) => (
          <button
            key={question}
            type="button"
            className="cursor-pointer rounded-lg border border-edge px-3 py-2 text-left text-[13px] leading-5 text-slate-300 transition-colors focus-visible:outline-2 focus-visible:outline-accent hover:border-edge-hover hover:bg-surface-hover hover:text-ink disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:border-edge disabled:hover:bg-transparent disabled:hover:text-slate-300"
            onClick={() => {
              onAsk(question);
            }}
            disabled={!canAsk}
          >
            {question}
          </button>
        ))}
      </div>
    );
  }

  return (
    <div
      ref={listRef}
      className="flex min-h-0 flex-1 flex-col gap-5 overflow-y-auto px-4 py-4"
      onScroll={handleScroll}
      aria-live="polite"
      aria-busy={isAnswering}
    >
      {messages.map((message, index) =>
        message.role === "user" ? (
          <Question key={index} content={message.content} />
        ) : (
          <AssistantAnswer key={index} content={message.content} />
        ),
      )}
      {pendingExchange !== null && (
        <Question content={pendingExchange.question} />
      )}
      {pendingAnswer !== "" && <AssistantAnswer content={pendingAnswer} />}
      {isAnswering && pendingAnswer === "" && (
        <p
          className="text-[13px] text-slate-500 motion-safe:animate-pulse"
          role="status"
        >
          Thinking...
        </p>
      )}
      {errorMessage !== null && pendingExchange !== null && (
        <div className="rounded-lg border border-red-900/50 bg-red-950/20 px-3 py-2 text-[13px] leading-5 text-red-300">
          <p className="break-words">{errorMessage}</p>
          <button
            type="button"
            className="mt-1 cursor-pointer text-xs font-bold text-red-200 underline transition hover:text-white disabled:cursor-not-allowed disabled:opacity-50"
            onClick={() => {
              onAsk(pendingExchange.question);
            }}
            disabled={!canAsk}
          >
            Retry
          </button>
        </div>
      )}
    </div>
  );
};

export default AssistantMessages;
