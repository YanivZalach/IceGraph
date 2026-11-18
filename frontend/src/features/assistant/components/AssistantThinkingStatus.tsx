import { useEffect, useState } from "react";

const THINKING_WORDS = [
  "Thinking",
  "Iceing",
  "Graphing",
  "Snapshotting",
  "Manifesting",
  "Partitioning",
  "Compacting",
  "Committing",
  "Freezing",
  "Chilling",
  "Pruning",
  "Branching",
];
const THINKING_WORD_INTERVAL_MS = 1500;

// Any other word, so the same word never shows twice in a row.
const pickNextWordIndex = (wordIndex: number): number =>
  (wordIndex + 1 + Math.floor(Math.random() * (THINKING_WORDS.length - 1))) %
  THINKING_WORDS.length;

const AssistantThinkingStatus = () => {
  const [wordIndex, setWordIndex] = useState(() =>
    Math.floor(Math.random() * THINKING_WORDS.length),
  );

  useEffect(() => {
    const intervalId = window.setInterval(() => {
      setWordIndex(pickNextWordIndex);
    }, THINKING_WORD_INTERVAL_MS);
    return () => {
      window.clearInterval(intervalId);
    };
  }, []);

  return (
    // Visual only: the panel's announcer already tells screen readers an answer is coming.
    <p
      className="text-[13px] text-slate-500 motion-safe:animate-pulse"
      aria-hidden="true"
    >
      {THINKING_WORDS[wordIndex]}...
    </p>
  );
};

export default AssistantThinkingStatus;
