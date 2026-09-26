import { Link, useRouterState, useSearch } from "@tanstack/react-router";
import logo from "../../assets/icegraph.png";
import { useTableSpecs } from "../specs/tableSpecs";
import { cn } from "../../shared/lib/cn";
import TablePicker from "./TablePicker";
import AnalyzeTabs from "./AnalyzeTabs";
import { navTabClass } from "./navTabClass";

const actionClass = (isActive = false): string =>
  cn(
    "shrink-0 cursor-pointer whitespace-nowrap rounded-md border px-3 py-1 text-sm font-medium transition",
    isActive
      ? "border-accent bg-accent text-white"
      : "border-slate-600 text-slate-400 hover:border-slate-400 hover:text-white",
  );

const issuesClass = (isOpen: boolean, hasErrors: boolean): string =>
  cn(
    "shrink-0 cursor-pointer whitespace-nowrap rounded-md border px-3 py-1 text-sm font-bold transition",
    hasErrors
      ? isOpen
        ? "border-red-600 bg-red-600 text-white"
        : "border-red-900/50 text-red-500 hover:bg-red-950/30"
      : isOpen
        ? "border-amber-600 bg-amber-600 text-white"
        : "border-amber-900/50 text-amber-500 hover:bg-amber-950/30",
  );

const NavBar = () => {
  const pathname = useRouterState({
    select: (state) => state.location.pathname,
  });
  const search = useSearch({ strict: false });
  const {
    detailsOpen,
    setDetailsOpen,
    specsMetadata,
    graphQuery,
    rebuildGraph,
    errors,
    warnings,
    issuesOpen,
    setIssuesOpen,
  } = useTableSpecs();
  const isTablePage = pathname.startsWith("/table/");
  const isTableContext = isTablePage || pathname === "/snapshots-selection";
  const tableName = typeof search.table === "string" ? search.table : "";
  const errorCount = Object.keys(errors).length;
  const issueCount = errorCount + Object.keys(warnings).length;
  const isRecompileDisabled = graphQuery.isFetching || tableName === "";
  const isSpecsDisabled = specsMetadata === undefined && !detailsOpen;

  return (
    <nav className="sticky top-0 z-[1200] h-16 shrink-0 bg-surface text-white shadow-lg">
      <div className="flex h-full items-center gap-3 px-4 xl:gap-4 xl:px-6">
        <Link
          to="/"
          target={isTableContext ? "_blank" : "_self"}
          rel="noopener noreferrer"
          className="-ml-1 flex shrink-0 items-center gap-2 rounded-md px-1 select-none transition hover:bg-surface-hover"
          title="IceGraph home"
        >
          <img
            src={logo}
            alt=""
            className="pointer-events-none h-10 w-10 object-contain"
            aria-hidden="true"
          />
          <span
            className={cn(
              "text-lg font-bold tracking-tight",
              isTableContext && "hidden xl:inline",
            )}
          >
            IceGraph
          </span>
        </Link>

        {!isTableContext && (
          <Link
            to="/docs"
            activeProps={{ className: navTabClass(true) }}
            inactiveProps={{ className: navTabClass(false) }}
          >
            Docs
          </Link>
        )}

        {isTableContext && (
          <div className="flex min-w-0 flex-1 items-center gap-3 xl:gap-4">
            {tableName !== "" && (
              <TablePicker key={pathname} tableName={tableName} />
            )}
            {isTablePage && (
              <>
                <div className="h-4 w-px shrink-0 bg-slate-700" />
                <AnalyzeTabs />
              </>
            )}
            {isTablePage && issueCount > 0 && (
              <button
                type="button"
                onClick={() => {
                  setIssuesOpen((isOpen) => !isOpen);
                }}
                className={issuesClass(issuesOpen, errorCount > 0)}
              >
                Issues ({issueCount})
              </button>
            )}
            {tableName !== "" && (
              <button
                type="button"
                className={cn(
                  actionClass(detailsOpen),
                  isSpecsDisabled && "cursor-not-allowed opacity-50",
                )}
                title={
                  isSpecsDisabled
                    ? "Available once the table metadata loads"
                    : undefined
                }
                onClick={() => {
                  setDetailsOpen(!detailsOpen);
                }}
                disabled={isSpecsDisabled}
              >
                Specs
              </button>
            )}

            <div className="ml-auto flex shrink-0 items-center gap-3">
              {isTablePage && (
                <button
                  type="button"
                  className={cn(
                    actionClass(),
                    isRecompileDisabled && "cursor-not-allowed opacity-50",
                  )}
                  title="Discard the cached graph and recompile it from Iceberg metadata"
                  onClick={() => void rebuildGraph()}
                  disabled={isRecompileDisabled}
                >
                  {graphQuery.isFetching ? "Recompiling..." : "Recompile graph"}
                </button>
              )}
              <Link
                to="/docs"
                target="_blank"
                rel="noopener noreferrer"
                className={actionClass()}
              >
                Docs
              </Link>
            </div>
          </div>
        )}
      </div>
    </nav>
  );
};
export default NavBar;
