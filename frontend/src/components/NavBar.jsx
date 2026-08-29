import { useEffect, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Link,
  useLocation,
  useMatchRoute,
  useNavigate,
  useSearch,
} from "@tanstack/react-router";
import logo from "../assets/icegraph.png";
import CatalogTableList from "./CatalogTableList";
import { useTableSpecs } from "../context/TableSpecsContext";
import { catalogQueryOptions } from "../features/catalog/api/catalogQueries";
import {
  BASE_PATH,
  IS_MOCK,
  MOCK_HOME,
  MOCK_HOME_ROUTE,
  MOCK_TABLE,
  MOCK_TABLE_SEARCH,
} from "../appConstants";
import {
  UI_ERROR_TEXT_SPACED_CLASS,
  UI_FORM_LABEL_CLASS,
  UI_LINK_BUTTON_CLASS,
  UI_PRIMARY_BUTTON_SM_CLASS,
  UI_TABLE_NAME_BUTTON_CLASS,
  UI_TEXT_INPUT_CLASS,
} from "../uiTypography";

const tabClass = ({ isActive }) =>
  `text-sm font-medium px-1 py-0.5 border-b-2 transition ${
    isActive
      ? "border-accent text-white"
      : "border-transparent text-slate-400 hover:text-white hover:border-slate-500"
  }`;

const mobileTabClass = ({ isActive }) =>
  `text-sm font-medium px-3 py-2 rounded-md transition text-left ${
    isActive
      ? "bg-accent-muted text-white"
      : "text-slate-400 hover:text-white hover:bg-surface-hover"
  }`;

const getTabSearch = (search, destination) => {
  if (destination === "/table/filetree") return search;
  return Object.fromEntries(
    Object.entries(search).filter(([key]) => !key.startsWith("filetree_")),
  );
};

const TabLink = ({ to, children, mobile }) => (
  <Link
    to={to}
    search={(previous) => getTabSearch(previous, to)}
    activeProps={{
      className: (mobile ? mobileTabClass : tabClass)({ isActive: true }),
    }}
    inactiveProps={{
      className: (mobile ? mobileTabClass : tabClass)({ isActive: false }),
    }}
  >
    {children}
  </Link>
);

export default function NavBar() {
  const location = useLocation();
  const search = useSearch({ strict: false });
  const navigate = useNavigate();
  const matchRoute = useMatchRoute();
  const isTablePage = Boolean(matchRoute({ to: "/table", fuzzy: true }));
  const tableName = search.table ?? null;
  const {
    detailsOpen,
    setDetailsOpen,
    graphQuery,
    rebuildGraph,
    errors,
    warnings,
    issuesOpen,
    setIssuesOpen,
  } = useTableSpecs();
  const [menuOpen, setMenuOpen] = useState(false);
  const [tablePickerOpen, setTablePickerOpen] = useState(false);
  const [pickerTableName, setPickerTableName] = useState("");
  const [isCatalogListOpen, setIsCatalogListOpen] = useState(false);
  const [catalogFilter, setCatalogFilter] = useState("");
  const catalogQuery = useQuery(catalogQueryOptions());
  const navRef = useRef(null);
  const tablePickerRef = useRef(null);
  const catalogTables = isCatalogListOpen
    ? (catalogQuery.data?.tables ?? null)
    : null;
  const includeNoneIcebergCatalogs =
    catalogQuery.data?.include_none_iceberg_catalogs ?? false;
  const catalogError =
    isCatalogListOpen && catalogQuery.isError
      ? catalogQuery.error.message
      : null;

  useEffect(() => {
    if (!menuOpen && !tablePickerOpen) return;
    const handler = (e) => {
      if (menuOpen && navRef.current && !navRef.current.contains(e.target))
        setMenuOpen(false);
      if (
        tablePickerOpen &&
        tablePickerRef.current &&
        !tablePickerRef.current.contains(e.target)
      )
        setTablePickerOpen(false);
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [menuOpen, tablePickerOpen]);

  useEffect(() => {
    setMenuOpen(false);
    setTablePickerOpen(false);
  }, [location.pathname, location.searchStr]);

  useEffect(() => {
    if (!tablePickerOpen) return;
    const handleKey = (e) => {
      if (e.key === "Escape") setTablePickerOpen(false);
    };
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [tablePickerOpen]);

  useEffect(() => {
    if (!isTablePage) return;
    const handleKey = (e) => {
      if (e.ctrlKey || e.metaKey || e.altKey) return;
      if (
        ["INPUT", "TEXTAREA", "SELECT"].includes(e.target.tagName) ||
        e.target.isContentEditable
      )
        return;
      const tabs = ["timeline", "metadata", "filetree", "graph"];
      const idx = parseInt(e.key) - 1;
      if (idx >= 0 && idx < tabs.length) {
        e.preventDefault();
        const destination = `/table/${tabs[idx]}`;
        navigate({
          to: destination,
          search: (previous) => getTabSearch(previous, destination),
        });
      }
    };
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [isTablePage, navigate]);

  function openTablePicker() {
    setPickerTableName(tableName || "");
    setIsCatalogListOpen(false);
    setCatalogFilter("");
    setTablePickerOpen(true);
  }

  function fetchCatalogTables() {
    setIsCatalogListOpen(true);
    setCatalogFilter("");
    void catalogQuery.refetch();
  }

  function changeTable(newName) {
    const trimmed = newName.trim();
    if (!trimmed) return;

    const tableForHistory = IS_MOCK ? MOCK_TABLE : trimmed;
    const savedHistory = localStorage.getItem("tableHistory");
    const history = savedHistory ? JSON.parse(savedHistory) : [];
    const updatedHistory = [...new Set([tableForHistory, ...history])].slice(
      0,
      5,
    );
    localStorage.setItem("tableHistory", JSON.stringify(updatedHistory));

    const tableParam = encodeURIComponent(IS_MOCK ? MOCK_TABLE : trimmed);
    let targetUrl;
    if (IS_MOCK) {
      const tab =
        location.pathname.match(/\/table\/([^/]+)/)?.[1] || "timeline";
      targetUrl = `${BASE_PATH}${MOCK_HOME_ROUTE.replace("/table/timeline", `/table/${tab}`)}`;
    } else {
      targetUrl = `${BASE_PATH}/snapshots-selection?table=${tableParam}`;
    }

    window.open(targetUrl, "_blank", "noopener,noreferrer");

    setTablePickerOpen(false);
    setMenuOpen(false);
  }

  function handleTablePickerSubmit(e) {
    e.preventDefault();
    changeTable(pickerTableName);
  }

  const tableNameButtonClass = UI_TABLE_NAME_BUTTON_CLASS;

  const tablePickerPanel = (
    <form onSubmit={handleTablePickerSubmit} className="flex flex-col gap-3">
      <div>
        <div className="flex items-center justify-between mb-1.5">
          <label className={UI_FORM_LABEL_CLASS}>Change table</label>
          <button
            type="button"
            onClick={fetchCatalogTables}
            disabled={catalogQuery.isFetching}
            className={UI_LINK_BUTTON_CLASS}
          >
            {catalogQuery.isFetching
              ? catalogQuery.data
                ? "Refreshing…"
                : "Loading…"
              : "Browse catalog"}
          </button>
        </div>
        <input
          type="text"
          required
          value={pickerTableName}
          onChange={(e) => setPickerTableName(e.target.value)}
          placeholder="default.my_table"
          className={UI_TEXT_INPUT_CLASS}
          autoFocus
        />
        {catalogError && (
          <p className={UI_ERROR_TEXT_SPACED_CLASS}>{catalogError}</p>
        )}
        <CatalogTableList
          tables={catalogTables}
          selectedName={pickerTableName}
          onSelect={setPickerTableName}
          filter={catalogFilter}
          onFilterChange={setCatalogFilter}
          listClassName="max-h-40"
          includeNoneIcebergCatalogs={includeNoneIcebergCatalogs}
        />
      </div>
      <button type="submit" className={UI_PRIMARY_BUTTON_SM_CLASS}>
        Continue
      </button>
    </form>
  );

  return (
    <nav
      ref={navRef}
      className="h-16 bg-surface text-white shadow-lg shrink-0 sticky top-0 z-[1200]"
    >
      <div className="px-4 sm:px-6 py-3 flex items-center gap-4">
        <Link
          to="/docs"
          target={isTablePage ? "_blank" : undefined}
          rel={isTablePage ? "noopener noreferrer" : undefined}
          className="flex items-center gap-2 select-none shrink-0 rounded-md px-1 -ml-1 hover:bg-surface-hover transition"
          title="IceGraph documentation"
        >
          <img
            src={logo}
            alt=""
            className="h-10 w-10 object-contain pointer-events-none"
            aria-hidden="true"
          />
          <span className="text-lg font-bold tracking-tight">IceGraph</span>
        </Link>

        {!isTablePage && (
          <>
            <Link
              to={IS_MOCK ? "/table/timeline" : "/"}
              search={IS_MOCK ? MOCK_TABLE_SEARCH : undefined}
              activeOptions={{ exact: true }}
              activeProps={{ className: tabClass({ isActive: true }) }}
              inactiveProps={{ className: tabClass({ isActive: false }) }}
            >
              Home
            </Link>
            <Link
              to="/docs"
              activeProps={{ className: tabClass({ isActive: true }) }}
              inactiveProps={{ className: tabClass({ isActive: false }) }}
            >
              Docs
            </Link>
          </>
        )}

        {isTablePage && (
          <div className="hidden md:flex items-center gap-4 flex-1">
            {tableName && (
              <div className="relative" ref={tablePickerRef}>
                <button
                  type="button"
                  onClick={() =>
                    tablePickerOpen
                      ? setTablePickerOpen(false)
                      : openTablePicker()
                  }
                  className={tableNameButtonClass}
                  title="Change table"
                  aria-expanded={tablePickerOpen}
                >
                  {tableName}
                </button>
                {tablePickerOpen && (
                  <div className="absolute top-full left-0 mt-2 w-80 p-4 rounded-lg border border-edge bg-surface shadow-xl z-[70]">
                    {tablePickerPanel}
                  </div>
                )}
              </div>
            )}

            <div className="w-px h-4 bg-slate-700" />

            <TabLink to="/table/timeline">Timeline</TabLink>
            <TabLink to="/table/metadata">Metadata</TabLink>
            <TabLink to="/table/filetree">FileTree</TabLink>
            <TabLink to="/table/graph">Graph</TabLink>

            {((errors && Object.keys(errors).length > 0) ||
              (warnings && Object.keys(warnings).length > 0)) && (
              <button
                onClick={() => setIssuesOpen((p) => !p)}
                className={`text-sm font-bold px-3 py-1 rounded-md transition border ${
                  issuesOpen
                    ? Object.keys(errors || {}).length > 0
                      ? "bg-red-600 border-red-600 text-white"
                      : "bg-amber-600 border-amber-600 text-white"
                    : Object.keys(errors || {}).length > 0
                      ? "border-red-900/50 text-red-500 hover:bg-red-950/30"
                      : "border-amber-900/50 text-amber-500 hover:bg-amber-950/30"
                }`}
              >
                Issues (
                {Object.keys(errors || {}).length +
                  Object.keys(warnings || {}).length}
                )
              </button>
            )}

            <button
              className={`text-sm font-medium px-3 py-1 rounded-md border transition ${
                detailsOpen
                  ? "bg-accent border-accent text-white"
                  : "border-slate-600 text-slate-400 hover:border-slate-400 hover:text-white"
              }`}
              onClick={() => setDetailsOpen((p) => !p)}
            >
              Specs
            </button>

            <div className="ml-auto flex items-center gap-3">
              <button
                className={`text-sm font-medium border border-slate-600 px-3 py-1 rounded-md transition ${
                  graphQuery.isFetching || !tableName
                    ? "opacity-50 cursor-not-allowed text-slate-500"
                    : "text-slate-400 hover:text-white hover:border-slate-400"
                }`}
                title="Discard the cached graph and rebuild it from Iceberg metadata"
                onClick={() => void rebuildGraph()}
                disabled={graphQuery.isFetching || !tableName}
              >
                {graphQuery.isFetching ? "Rebuilding..." : "Rebuild graph"}
              </button>

              <div className="w-px h-4 bg-slate-700" />

              <Link
                to="/docs"
                target="_blank"
                rel="noopener noreferrer"
                className="text-sm font-medium text-slate-400 hover:text-white border border-slate-600 hover:border-slate-400 px-3 py-1 rounded-md transition"
              >
                Docs
              </Link>

              <button
                className="text-sm font-medium text-slate-400 hover:text-white border border-slate-600 hover:border-slate-400 px-3 py-1 rounded-md transition"
                onClick={() => window.open(IS_MOCK ? MOCK_HOME : "/", "_blank")}
              >
                ← Home
              </button>
            </div>
          </div>
        )}

        {isTablePage && (
          <button
            className="md:hidden ml-auto flex flex-col justify-center items-center w-8 h-8 gap-1.5 rounded transition hover:bg-surface-hover cursor-pointer"
            onClick={() => setMenuOpen((p) => !p)}
            aria-label="Toggle menu"
          >
            <span
              className={`block w-5 h-0.5 bg-slate-400 transition-all origin-center ${menuOpen ? "rotate-45 translate-y-2" : ""}`}
            />
            <span
              className={`block w-5 h-0.5 bg-slate-400 transition-all ${menuOpen ? "opacity-0" : ""}`}
            />
            <span
              className={`block w-5 h-0.5 bg-slate-400 transition-all origin-center ${menuOpen ? "-rotate-45 -translate-y-2" : ""}`}
            />
          </button>
        )}
      </div>

      {isTablePage && menuOpen && (
        <div className="md:hidden border-t border-edge px-4 py-3 flex flex-col gap-1 bg-surface absolute top-16 left-0 w-full z-[60] shadow-xl">
          {tableName && (
            <div ref={tablePickerRef}>
              <button
                type="button"
                onClick={() =>
                  tablePickerOpen
                    ? setTablePickerOpen(false)
                    : openTablePicker()
                }
                className={`${tableNameButtonClass} w-full text-left`}
                title="Change table"
                aria-expanded={tablePickerOpen}
              >
                {tableName}
              </button>
              {tablePickerOpen && (
                <div className="mt-2 p-4 rounded-lg border border-edge bg-surface-hover">
                  {tablePickerPanel}
                </div>
              )}
            </div>
          )}

          <div className="h-px bg-edge my-1" />

          <TabLink to="/table/timeline" mobile>
            Timeline
          </TabLink>
          <TabLink to="/table/metadata" mobile>
            Metadata
          </TabLink>
          <TabLink to="/table/filetree" mobile>
            FileTree
          </TabLink>
          <TabLink to="/table/graph" mobile>
            Graph
          </TabLink>

          {((errors && Object.keys(errors).length > 0) ||
            (warnings && Object.keys(warnings).length > 0)) && (
            <button
              onClick={() => {
                setIssuesOpen((p) => !p);
                setMenuOpen(false);
              }}
              className={`text-sm font-bold px-3 py-2 rounded-md transition text-left ${
                issuesOpen
                  ? Object.keys(errors || {}).length > 0
                    ? "bg-red-600 text-white"
                    : "bg-amber-600 text-white"
                  : Object.keys(errors || {}).length > 0
                    ? "text-red-500 hover:bg-red-950/30"
                    : "text-amber-500 hover:bg-amber-950/30"
              }`}
            >
              Issues (
              {Object.keys(errors || {}).length +
                Object.keys(warnings || {}).length}
              )
            </button>
          )}

          <div className="h-px bg-edge my-1" />

          <button
            className={`text-sm font-medium px-3 py-2 rounded-md border transition text-left ${
              detailsOpen
                ? "bg-accent border-accent text-white"
                : "border-slate-600 text-slate-400 hover:border-slate-400 hover:text-white"
            }`}
            onClick={() => {
              setDetailsOpen((p) => !p);
              setMenuOpen(false);
            }}
          >
            Specs
          </button>

          <button
            className={`text-sm font-medium border border-slate-600 px-3 py-2 rounded-md transition text-left ${
              graphQuery.isFetching || !tableName
                ? "opacity-50 cursor-not-allowed text-slate-500"
                : "text-slate-400 hover:text-white hover:border-slate-400"
            }`}
            title="Discard the cached graph and rebuild it from Iceberg metadata"
            onClick={() => {
              void rebuildGraph();
              setMenuOpen(false);
            }}
            disabled={graphQuery.isFetching || !tableName}
          >
            {graphQuery.isFetching ? "Rebuilding..." : "Rebuild graph"}
          </button>

          <Link
            to="/docs"
            target="_blank"
            rel="noopener noreferrer"
            onClick={() => setMenuOpen(false)}
            className="text-sm font-medium text-slate-400 hover:text-white border border-slate-600 hover:border-slate-400 px-3 py-2 rounded-md transition text-left"
          >
            Docs
          </Link>

          <button
            className="text-sm font-medium text-slate-400 hover:text-white border border-slate-600 hover:border-slate-400 px-3 py-2 rounded-md transition text-left"
            onClick={() => {
              window.open(IS_MOCK ? MOCK_HOME : "/", "_blank");
              setMenuOpen(false);
            }}
          >
            ← Home
          </button>
        </div>
      )}
    </nav>
  );
}
