import { useHotkey } from "@tanstack/react-hotkeys";
import { Link, useNavigate } from "@tanstack/react-router";
import { navTabClass } from "./navTabClass";

const PARTITION_TABS = [
  { to: "/partition/filetree", label: "FileTree" },
  { to: "/partition/graph", label: "Graph" },
] as const;

type PartitionTabPath = (typeof PARTITION_TABS)[number]["to"];

const GRAPH_ONLY_KEYS = new Set([
  "select_node_id",
  "lineage_node_id",
  "lineage_partition",
  "graph_mode",
  "graph_search",
]);

const tabSearch = (
  previous: Record<string, unknown>,
  destination: PartitionTabPath,
): Record<string, unknown> =>
  Object.fromEntries(
    Object.entries(previous).filter(
      ([key]) =>
        (destination === "/partition/filetree" ||
          !key.startsWith("filetree_")) &&
        (destination === "/partition/graph" || !GRAPH_ONLY_KEYS.has(key)),
    ),
  );

const PartitionTabs = () => {
  const navigate = useNavigate();
  const goTo = (destination: PartitionTabPath): void => {
    void navigate({
      to: destination,
      search: (previous) => tabSearch(previous, destination),
    });
  };
  useHotkey("1", () => {
    goTo("/partition/filetree");
  });
  useHotkey("2", () => {
    goTo("/partition/graph");
  });

  return (
    <div className="flex shrink-0 items-center gap-3 xl:gap-4">
      {PARTITION_TABS.map(({ to, label }) => (
        <Link
          key={to}
          to={to}
          search={(previous) => tabSearch(previous, to)}
          activeProps={{ className: navTabClass(true) }}
          inactiveProps={{ className: navTabClass(false) }}
        >
          {label}
        </Link>
      ))}
    </div>
  );
};

export default PartitionTabs;
