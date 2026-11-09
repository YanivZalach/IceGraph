import { useHotkey } from "@tanstack/react-hotkeys";
import { Link, useNavigate } from "@tanstack/react-router";
import { navTabClass } from "./navTabClass";

const ANALYZE_TABS = [
  { to: "/table/timeline", label: "Timeline" },
  { to: "/table/metadata", label: "Metadata" },
  { to: "/table/filetree", label: "FileTree" },
  { to: "/table/graph", label: "Graph" },
] as const;

type AnalyzeTabPath = (typeof ANALYZE_TABS)[number]["to"];

const GRAPH_SEARCH_KEYS = new Set([
  "select_node_id",
  "lineage_node_id",
  "graph_mode",
]);

const isSearchKeyKept = (key: string, destination: AnalyzeTabPath): boolean =>
  (destination === "/table/filetree" || !key.startsWith("filetree_")) &&
  (destination === "/table/graph" || !GRAPH_SEARCH_KEYS.has(key));

const tabSearch = (
  previous: Record<string, unknown>,
  destination: AnalyzeTabPath,
): Record<string, unknown> =>
  Object.fromEntries(
    Object.entries(previous).filter(([key]) =>
      isSearchKeyKept(key, destination),
    ),
  );

const AnalyzeTabs = () => {
  const navigate = useNavigate();
  const goTo = (destination: AnalyzeTabPath): void => {
    void navigate({
      to: destination,
      search: (previous) => tabSearch(previous, destination),
    });
  };
  useHotkey("1", () => {
    goTo("/table/timeline");
  });
  useHotkey("2", () => {
    goTo("/table/metadata");
  });
  useHotkey("3", () => {
    goTo("/table/filetree");
  });
  useHotkey("4", () => {
    goTo("/table/graph");
  });

  return (
    <div className="flex shrink-0 items-center gap-3 xl:gap-4">
      {ANALYZE_TABS.map(({ to, label }) => (
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
export default AnalyzeTabs;
