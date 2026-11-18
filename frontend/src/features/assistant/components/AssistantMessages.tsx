import { useEffect, useLayoutEffect, useRef, useState } from "react";
import type { AssistantConversation } from "../useAssistantConversation";
import AssistantAnswer from "./AssistantAnswer";
import AssistantThinkingStatus from "./AssistantThinkingStatus";

interface AssistantMessagesProps {
  conversation: AssistantConversation;
  suggestedQuestions: readonly string[];
  canAsk: boolean;
  onAsk: (question: string) => void;
}

const FOLLOW_BOTTOM_THRESHOLD_PX = 48;
const LOAD_EARLIER_THRESHOLD_PX = 48;
// Even, so a question and its answer stay together.
const MESSAGES_PER_STEP = 6;

const Question = ({ content }: { content: string }) => (
  <div className="max-w-[85%] self-end rounded-2xl rounded-br-sm bg-accent-muted px-3.5 py-2 text-sm leading-6 whitespace-pre-wrap break-words text-ink-bright">
    {content}
  </div>
);

// Mounted only while there are messages, so Clear and Undo start again from the latest step.
const MessageList = ({
  conversation,
  canAsk,
  onAsk,
}: Omit<AssistantMessagesProps, "suggestedQuestions">) => {
  const { messages, pendingExchange, isAnswering, errorMessage } = conversation;
  const listRef = useRef<HTMLDivElement>(null);
  const isFollowingBottomRef = useRef(true);
  const lastScrollTopRef = useRef(0);
  const scrollHeightBeforeRevealRef = useRef<number | null>(null);
  const [hiddenCount, setHiddenCount] = useState(() =>
    Math.max(0, messages.length - MESSAGES_PER_STEP),
  );
  const pendingAnswer = pendingExchange?.answer ?? "";

  useEffect(() => {
    const list = listRef.current;
    if (list !== null && isFollowingBottomRef.current) {
      list.scrollTop = list.scrollHeight;
      lastScrollTopRef.current = list.scrollTop;
    }
  }, [messages, pendingExchange, errorMessage]);

  useLayoutEffect(() => {
    const list = listRef.current;
    const scrollHeightBeforeReveal = scrollHeightBeforeRevealRef.current;
    if (list === null || scrollHeightBeforeReveal === null) return;
    list.scrollTop += list.scrollHeight - scrollHeightBeforeReveal;
    lastScrollTopRef.current = list.scrollTop;
    scrollHeightBeforeRevealRef.current = null;
  }, [hiddenCount]);

  const showEarlierMessages = (): void => {
    scrollHeightBeforeRevealRef.current = listRef.current?.scrollHeight ?? null;
    setHiddenCount((count) => Math.max(0, count - MESSAGES_PER_STEP));
  };

  const handleScroll = (): void => {
    const list = listRef.current;
    if (list === null) return;
    isFollowingBottomRef.current =
      list.scrollHeight - list.scrollTop - list.clientHeight <
      FOLLOW_BOTTOM_THRESHOLD_PX;
    const isScrollingUp = list.scrollTop < lastScrollTopRef.current;
    lastScrollTopRef.current = list.scrollTop;
    if (
      isScrollingUp &&
      hiddenCount > 0 &&
      list.scrollTop < LOAD_EARLIER_THRESHOLD_PX &&
      scrollHeightBeforeRevealRef.current === null
    ) {
      showEarlierMessages();
    }
  };

  return (
    <div
      ref={listRef}
      className="flex min-h-0 flex-1 flex-col gap-5 overflow-y-auto px-4 py-4"
      onScroll={handleScroll}
    >
      {hiddenCount > 0 && (
        <button
          type="button"
          className="cursor-pointer self-center rounded-full px-3 py-1 text-xs text-slate-400 transition-colors focus-visible:outline-2 focus-visible:outline-accent hover:bg-surface-hover hover:text-ink"
          onClick={showEarlierMessages}
        >
          Show earlier messages
        </button>
      )}
      {messages
        .slice(hiddenCount)
        .map((message, index) =>
          message.role === "user" ? (
            <Question key={hiddenCount + index} content={message.content} />
          ) : (
            <AssistantAnswer
              key={hiddenCount + index}
              content={message.content}
            />
          ),
        )}
      {pendingExchange !== null && (
        <Question content={pendingExchange.question} />
      )}
      {pendingAnswer !== "" && <AssistantAnswer content={pendingAnswer} />}
      {isAnswering && pendingAnswer === "" && <AssistantThinkingStatus />}
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

const AssistantMessages = ({
  conversation,
  suggestedQuestions,
  canAsk,
  onAsk,
}: AssistantMessagesProps) => {
  const { messages, pendingExchange, announcement } = conversation;
  // Mounted with the panel, before the first question, so its changes are announced.
  const announcer = (
    <p className="sr-only" role="status" aria-live="polite">
      {announcement}
    </p>
  );

  if (messages.length === 0 && pendingExchange === null) {
    return (
      <>
        {announcer}
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
      </>
    );
  }

  return (
    <>
      {announcer}
      <MessageList conversation={conversation} canAsk={canAsk} onAsk={onAsk} />
    </>
  );
};

export default AssistantMessages;
