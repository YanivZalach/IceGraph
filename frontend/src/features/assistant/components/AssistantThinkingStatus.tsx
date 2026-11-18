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
const THINKING_WORD_INTERVAL_MS = 3000;

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
    <p
      className="text-[13px] text-slate-500 motion-safe:animate-pulse"
      aria-hidden="true"
    >
      {THINKING_WORDS[wordIndex]}...
    </p>
  );
};

export default AssistantThinkingStatus;
