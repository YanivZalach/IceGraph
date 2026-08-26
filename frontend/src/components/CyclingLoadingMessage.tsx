import { useEffect, useState } from "react";

const MESSAGES = [
  "Talking to the catalog…",
  "Listing snapshots…",
  "Reading table metadata…",
  "Checking snapshot history…",
  "Warming up the connection…",
  "Fetching table state…",
  "Walking the metadata tree…",
  "Gathering snapshot details…",
  "Almost there…",
  "Querying Spark Connect…",
  "Resolving table references…",
  "Sorting through history…",
  "Loading table timeline…",
  "Preparing snapshot list…",
  "Syncing with the catalog…",
];

const MIN_CYCLE_INTERVAL_MS = 1500;
const MAX_CYCLE_INTERVAL_MS = 3000;

const randomCycleIntervalMs = () =>
  MIN_CYCLE_INTERVAL_MS +
  Math.random() * (MAX_CYCLE_INTERVAL_MS - MIN_CYCLE_INTERVAL_MS);

const CyclingLoadingMessage = () => {
  const [index, setIndex] = useState(0);

  useEffect(() => {
    const timeout = setTimeout(() => {
      setIndex((current) => (current + 1) % MESSAGES.length);
    }, randomCycleIntervalMs());

    return () => {
      clearTimeout(timeout);
    };
  }, [index]);

  return <p className="text-lg">{MESSAGES[index]}</p>;
};

export default CyclingLoadingMessage;
