import type { KeyboardEvent, MouseEvent } from "react";
import { cn } from "../../../shared/lib/cn";
import type { AssistantContextOption } from "../assistantContext";

interface AssistantComposerProps {
  contextOptions: AssistantContextOption[];
  selectedOptionIds: readonly string[];
  historyExchangeCount: number;
  shouldSendHistory: boolean;
  isAnswering: boolean;
  usedCharacters: number;
  maxCharacters: number;
  draft: string;
  onDraftChange: (draft: string) => void;
  onAsk: (question: string) => void;
  onToggleOption: (optionId: string) => void;
  onToggleHistory: () => void;
}

const contextChipClass = (isSelected: boolean): string =>
  cn(
    "flex h-6 cursor-pointer items-center gap-1 rounded-full border px-2.5 text-xs transition-colors focus-visible:outline-2 focus-visible:outline-offset-1 focus-visible:outline-accent",
    isSelected
      ? "border-accent/50 bg-accent-muted text-ink-bright"
      : "border-edge text-slate-400 hover:border-edge-hover hover:text-ink",
  );

const keepTextareaFocus = (event: MouseEvent<HTMLButtonElement>): void => {
  event.preventDefault();
};

const SendIcon = ({ isAnswering }: { isAnswering: boolean }) => (
  <svg
    className={cn("size-4", isAnswering && "motion-safe:animate-pulse")}
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="2.2"
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
  >
    {isAnswering ? (
      <path d="M6 12h.01M12 12h.01M18 12h.01" />
    ) : (
      <path d="M12 19V5m-6 6 6-6 6 6" />
    )}
  </svg>
);

const AssistantComposer = ({
  contextOptions,
  selectedOptionIds,
  historyExchangeCount,
  shouldSendHistory,
  isAnswering,
  usedCharacters,
  maxCharacters,
  draft,
  onDraftChange,
  onAsk,
  onToggleOption,
  onToggleHistory,
}: AssistantComposerProps) => {
  const question = draft.trim();
  const totalCharacters = usedCharacters + question.length;
  const isOverLimit = totalCharacters > maxCharacters;
  const canSend = question !== "" && !isAnswering && !isOverLimit;

  const send = (): void => {
    if (!canSend) return;
    onAsk(question);
    onDraftChange("");
  };

  const handleKeyDown = (event: KeyboardEvent<HTMLTextAreaElement>): void => {
    if (event.key !== "Enter" || event.shiftKey) return;
    if (event.nativeEvent.isComposing) return;
    event.preventDefault();
    send();
  };

  return (
    <div className="shrink-0 space-y-2.5 border-t border-edge p-3">
      <div
        className="flex flex-wrap items-center gap-1.5"
        role="group"
        aria-label="Context sent to the AI model"
      >
        <span className="mr-0.5 text-[11px] font-medium text-slate-500">
          Context
        </span>
        {contextOptions.map((option) => {
          const isSelected = selectedOptionIds.includes(option.id);
          return (
            <button
              key={option.id}
              type="button"
              className={contextChipClass(isSelected)}
              aria-pressed={isSelected}
              onMouseDown={keepTextareaFocus}
              onClick={() => {
                onToggleOption(option.id);
              }}
            >
              {option.label}
              {option.itemCount !== null && (
                <span className="tabular-nums opacity-60">
                  {option.itemCount.toLocaleString()}
                </span>
              )}
            </button>
          );
        })}
        {historyExchangeCount > 0 && (
          <button
            type="button"
            className={contextChipClass(shouldSendHistory)}
            aria-pressed={shouldSendHistory}
            title="Send the latest questions and answers of this conversation"
            onMouseDown={keepTextareaFocus}
            onClick={onToggleHistory}
          >
            History
            <span className="tabular-nums opacity-60">
              {historyExchangeCount.toLocaleString()}
            </span>
          </button>
        )}
      </div>
      <div className="rounded-lg border border-edge bg-surface-deep transition-colors focus-within:border-accent/60">
        <textarea
          className="block w-full resize-none bg-transparent px-3 pt-2.5 pb-1 text-sm leading-6 text-ink placeholder-slate-500 focus:outline-none"
          rows={2}
          value={draft}
          placeholder="Ask about this table..."
          aria-label="Question for the AI assistant"
          autoFocus
          onChange={(event) => {
            onDraftChange(event.target.value);
          }}
          onKeyDown={handleKeyDown}
        />
        <div className="flex items-center gap-3 pr-1.5 pb-1.5 pl-3">
          <p
            className={cn(
              "min-w-0 flex-1 text-[11px] leading-4 tabular-nums",
              isOverLimit ? "text-rose-400" : "text-slate-500",
            )}
          >
            {totalCharacters.toLocaleString()} /{" "}
            {maxCharacters.toLocaleString()} characters
            {isOverLimit && ". Select less context or clear the conversation."}
          </p>
          <button
            type="button"
            className="flex size-7 shrink-0 cursor-pointer items-center justify-center rounded-md bg-accent text-white transition-colors focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent hover:bg-accent-dark disabled:cursor-not-allowed disabled:bg-surface-hover disabled:text-slate-500"
            aria-label={isAnswering ? "Answering..." : "Send"}
            title={isAnswering ? "Answering..." : "Send"}
            onClick={send}
            disabled={!canSend}
          >
            <SendIcon isAnswering={isAnswering} />
          </button>
        </div>
      </div>
    </div>
  );
};

export default AssistantComposer;
