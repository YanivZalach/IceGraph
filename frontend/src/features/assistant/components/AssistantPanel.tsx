import type { KeyboardEvent } from "react";
import type { AssistantContextOption } from "../assistantContext";
import type { AssistantConversation } from "../useAssistantConversation";
import AssistantComposer from "./AssistantComposer";
import AssistantMessages from "./AssistantMessages";

interface AssistantPanelProps {
  tableName: string;
  conversation: AssistantConversation;
  contextOptions: AssistantContextOption[];
  selectedOptionIds: readonly string[];
  suggestedQuestions: readonly string[];
  shouldSendHistory: boolean;
  usedCharacters: number;
  maxCharacters: number;
  draft: string;
  onDraftChange: (draft: string) => void;
  onAsk: (question: string) => void;
  onToggleOption: (optionId: string) => void;
  onToggleHistory: () => void;
  onClose: () => void;
}

const HEADER_BUTTON_CLASS =
  "flex h-7 cursor-pointer items-center justify-center rounded-md px-2 text-xs font-medium text-slate-400 transition-colors focus-visible:outline-2 focus-visible:outline-accent hover:bg-surface-hover hover:text-ink disabled:cursor-not-allowed disabled:opacity-40 disabled:hover:bg-transparent disabled:hover:text-slate-400";

const AssistantPanel = ({
  tableName,
  conversation,
  contextOptions,
  selectedOptionIds,
  suggestedQuestions,
  shouldSendHistory,
  usedCharacters,
  maxCharacters,
  draft,
  onDraftChange,
  onAsk,
  onToggleOption,
  onToggleHistory,
  onClose,
}: AssistantPanelProps) => {
  const isConversationEmpty =
    conversation.messages.length === 0 && conversation.pendingExchange === null;
  const canAsk = !conversation.isAnswering && usedCharacters <= maxCharacters;

  // Keys typed in the assistant must not reach the view shortcuts behind it.
  // data-owns-keyboard covers the capture-phase listeners that run before this.
  const handleKeyDown = (event: KeyboardEvent<HTMLElement>): void => {
    event.stopPropagation();
    if (event.key === "Escape") onClose();
  };

  return (
    <section
      className="fixed bottom-20 left-5 z-[1150] flex h-[34rem] max-h-[calc(100dvh-var(--spacing-nav)-7rem)] w-[30rem] max-w-[calc(100vw-2.5rem)] flex-col overflow-y-auto rounded-xl border border-edge bg-surface shadow-2xl shadow-black/40"
      aria-label="AI assistant"
      data-owns-keyboard
      onKeyDown={handleKeyDown}
    >
      <header className="flex h-11 shrink-0 items-center gap-1 border-b border-edge pr-2 pl-4">
        <p className="flex min-w-0 flex-1 items-baseline gap-2">
          <span className="shrink-0 text-sm font-semibold text-ink-bright">
            Ask AI
          </span>
          <span
            className="truncate font-mono text-xs text-slate-500"
            title={tableName}
          >
            {tableName}
          </span>
        </p>
        {conversation.canUndoClear ? (
          <button
            type="button"
            className={HEADER_BUTTON_CLASS}
            title="Restore the cleared conversation"
            onClick={conversation.undoClear}
          >
            Undo
          </button>
        ) : (
          <button
            type="button"
            className={HEADER_BUTTON_CLASS}
            title="Delete this conversation"
            onClick={conversation.clear}
            disabled={isConversationEmpty}
          >
            Clear
          </button>
        )}
        <button
          type="button"
          className={HEADER_BUTTON_CLASS}
          title="Close"
          aria-label="Close the AI assistant"
          onClick={onClose}
        >
          <svg
            className="size-3.5"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2.2"
            strokeLinecap="round"
            aria-hidden="true"
          >
            <path d="M6 6l12 12M18 6L6 18" />
          </svg>
        </button>
      </header>
      <AssistantMessages
        conversation={conversation}
        suggestedQuestions={suggestedQuestions}
        canAsk={canAsk}
        onAsk={onAsk}
      />
      <AssistantComposer
        contextOptions={contextOptions}
        selectedOptionIds={selectedOptionIds}
        historyExchangeCount={conversation.historyExchangeCount}
        shouldSendHistory={shouldSendHistory}
        isAnswering={conversation.isAnswering}
        usedCharacters={usedCharacters}
        maxCharacters={maxCharacters}
        draft={draft}
        onDraftChange={onDraftChange}
        onAsk={onAsk}
        onToggleOption={onToggleOption}
        onToggleHistory={onToggleHistory}
      />
    </section>
  );
};

export default AssistantPanel;
