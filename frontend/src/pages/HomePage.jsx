import { useNavigate } from "@tanstack/react-router";
import logo from "../assets/icegraph.png";
import TableSpotlightPanel from "../features/catalog/TableSpotlightPanel";
import { rememberRecentTable } from "../features/catalog/recentTables";
import { IS_MOCK, MOCK_TABLE } from "../appConstants";
import { NAV_HEIGHT_REM } from "../layoutConstants";
import { UI_BODY_MUTED_CLASS, UI_FOOTER_TEXT_CLASS } from "../uiTypography";

export default function HomePage() {
  const navigate = useNavigate();

  function openTable(name) {
    const selectedTableName = IS_MOCK ? MOCK_TABLE : name.trim();
    if (selectedTableName === "") return;
    rememberRecentTable(selectedTableName);
    navigate({
      to: "/snapshots-selection",
      search: { table: selectedTableName },
    });
  }

  return (
    <div
      className="relative flex min-h-[34rem] flex-col"
      style={{ height: `calc(100dvh - ${NAV_HEIGHT_REM}rem)` }}
    >
      <div
        className="pointer-events-none absolute inset-0 bg-home-glow"
        aria-hidden="true"
      />
      <main className="relative flex min-h-0 flex-1 flex-col items-center justify-center px-6 py-6">
        <div className="flex max-h-full min-h-0 w-full max-w-3xl flex-col rounded-2xl border border-edge bg-surface p-6 shadow-2xl shadow-black/40">
          <div className="mb-5 flex shrink-0 items-center gap-3">
            <img
              src={logo}
              alt="IceGraph"
              className="h-12 w-12 shrink-0 object-contain"
            />
            <div className="min-w-0">
              <h1 className="text-xl font-bold text-ink">IceGraph</h1>
              <p className={UI_BODY_MUTED_CLASS}>
                Search the catalog, or type a full table name, to explore its
                metadata graph.
              </p>
            </div>
          </div>

          <TableSpotlightPanel
            inputId="home-table-search"
            onOpenTable={openTable}
            className="min-h-0"
            resultsClassName="h-[26rem] min-h-28 shrink"
          />
        </div>
      </main>

      <footer className={`${UI_FOOTER_TEXT_CLASS} relative shrink-0`}>
        IceGraph, open source Apache Iceberg Metadata Visualizer
      </footer>
    </div>
  );
}
