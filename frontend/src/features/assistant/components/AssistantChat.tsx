import { Suspense, lazy, useEffect, useState } from "react";
import { cn } from "../../../shared/lib/cn";
import type { GraphRequestParameters } from "../../table/api/graphCache";
import type { AssistantContextSource } from "../assistantContext";
import { useAssistantConversation } from "../useAssistantConversation";
import type AssistantPanelComponent from "./AssistantPanel";

// React holds back a lazy component that suspended for about 300 ms, so the
// preloaded panel renders directly instead of through lazy.
let loadedAssistantPanel: typeof AssistantPanelComponent | null = null;
const loadAssistantPanel = () =>
  import("./AssistantPanel").then((panelModule) => {
    loadedAssistantPanel = panelModule.default;
    return panelModule;
  });
const LazyAssistantPanel = lazy(loadAssistantPanel);
const PANEL_PRELOAD_FALLBACK_DELAY_MS = 1000;

const preloadAssistantPanel = (): void => {
  loadAssistantPanel().catch(() => undefined);
};

interface AssistantChatProps {
  conversationKey: string;
  contextSource: AssistantContextSource;
  maxContextCharacters: number;
  parameters: GraphRequestParameters;
}

const AssistantIcon = ({ isOpen }: { isOpen: boolean }) => (
  <svg
    className="size-6"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="1.8"
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
  >
    {isOpen ? (
      <path d="M6 9l6 6 6-6" />
    ) : (
      <>
        <path d="M11 3l1.9 4.9L17.8 9.8l-4.9 1.9L11 16.6l-1.9-4.9L4.2 9.8l4.9-1.9L11 3z" />
        <path d="M18.5 15.5l.8 2 2 .8-2 .8-.8 2-.8-2-2-.8 2-.8.8-2z" />
      </>
    )}
  </svg>
);

const AssistantChat = ({
  conversationKey,
  contextSource,
  maxContextCharacters,
  parameters,
}: AssistantChatProps) => {
  const [isOpen, setIsOpen] = useState(false);
  const [draft, setDraft] = useState("");
  const [selectedOptionIds, setSelectedOptionIds] = useState(
    contextSource.defaultOptionIds,
  );
  const [shouldSendHistory, setShouldSendHistory] = useState(true);
  const conversation = useAssistantConversation(conversationKey, parameters);

  useEffect(() => {
    if (typeof window.requestIdleCallback === "function") {
      const idleCallbackId = window.requestIdleCallback(preloadAssistantPanel);
      return () => {
        window.cancelIdleCallback(idleCallbackId);
      };
    }
    const timeoutId = window.setTimeout(
      preloadAssistantPanel,
      PANEL_PRELOAD_FALLBACK_DELAY_MS,
    );
    return () => {
      window.clearTimeout(timeoutId);
    };
  }, []);
  const AssistantPanel = loadedAssistantPanel ?? LazyAssistantPanel;
  const contextOptions = contextSource.options;
  const context = contextSource.buildContext(selectedOptionIds);

  const handleToggleOption = (optionId: string): void => {
    setSelectedOptionIds((currentOptionIds) =>
      currentOptionIds.includes(optionId)
        ? currentOptionIds.filter((selectedId) => selectedId !== optionId)
        : [...currentOptionIds, optionId],
    );
  };

  return (
    <>
      {isOpen && (
        <Suspense fallback={null}>
          <AssistantPanel
            tableName={parameters.tableName}
            conversation={conversation}
            contextOptions={contextOptions}
            selectedOptionIds={selectedOptionIds}
            suggestedQuestions={contextSource.suggestedQuestions}
            shouldSendHistory={shouldSendHistory}
            usedCharacters={
              context.length +
              (shouldSendHistory ? conversation.historyCharacters : 0)
            }
            maxCharacters={maxContextCharacters}
            draft={draft}
            onDraftChange={setDraft}
            onAsk={(question) => {
              conversation.ask({
                question,
                context,
                page: contextSource.page,
                shouldSendHistory,
              });
            }}
            onToggleOption={handleToggleOption}
            onToggleHistory={() => {
              setShouldSendHistory(!shouldSendHistory);
            }}
            onClose={() => {
              setIsOpen(false);
            }}
          />
        </Suspense>
      )}
      <button
        type="button"
        className={cn(
          "fixed bottom-5 left-5 z-[1150] flex size-12 cursor-pointer items-center justify-center rounded-full border border-accent bg-accent text-white shadow-lg shadow-black/30 transition-colors hover:bg-accent-dark focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-accent",
          conversation.isAnswering && !isOpen && "animate-pulse",
        )}
        title={isOpen ? "Close the AI assistant" : "Ask AI about this table"}
        aria-label={
          isOpen ? "Close the AI assistant" : "Ask AI about this table"
        }
        aria-expanded={isOpen}
        data-owns-keyboard={isOpen ? "" : undefined}
        onClick={() => {
          if (!isOpen) conversation.load();
          setIsOpen(!isOpen);
        }}
      >
        <AssistantIcon isOpen={isOpen} />
      </button>
    </>
  );
};

export default AssistantChat;
