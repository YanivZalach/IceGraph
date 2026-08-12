import { useEffect, useState } from "react";
import { useNavigate } from "@tanstack/react-router";
import logo from "../assets/icegraph.png";
import CatalogTableList from "../components/CatalogTableList";
import JSONbig from "json-bigint";
import {
  UI_BODY_MUTED_CLASS,
  UI_ERROR_TEXT_SPACED_CLASS,
  UI_FOOTER_TEXT_CLASS,
  UI_FORM_LABEL_CLASS,
  UI_LINK_BUTTON_CLASS,
  UI_PRIMARY_BUTTON_CLASS,
  UI_TEXT_INPUT_LG_CLASS,
} from "../uiTypography";

export default function HomePage() {
  const [tableName, setTableName] = useState("");
  const [history, setHistory] = useState([]);
  const [catalogTables, setCatalogTables] = useState(null);
  const [includeNoneIcebergCatalogs, setIncludeNoneIcebergCatalogs] =
    useState(false);
  const [catalogFilter, setCatalogFilter] = useState("");
  const [catalogLoading, setCatalogLoading] = useState(false);
  const [catalogError, setCatalogError] = useState(null);
  const navigate = useNavigate();

  useEffect(() => {
    const savedHistory = localStorage.getItem("tableHistory");
    if (savedHistory) {
      setHistory(JSONbig({ storeAsString: true }).parse(savedHistory));
    }
  }, []);

  function handleSubmit(e) {
    e.preventDefault();

    const updatedHistory = [...new Set([tableName, ...history])].slice(0, 5);
    setHistory(updatedHistory);
    localStorage.setItem("tableHistory", JSON.stringify(updatedHistory));

    navigate({ to: "/snapshots-selection", search: { table: tableName } });
  }

  async function fetchCatalogTables() {
    setCatalogLoading(true);
    setCatalogError(null);
    setCatalogFilter("");

    try {
      const res = await fetch("/api/v1/tables");
      const data = await res.json();
      if (!res.ok || data.error)
        throw new Error(data.error || "Failed to fetch tables");
      setCatalogTables(data.tables ?? []);
      setIncludeNoneIcebergCatalogs(
        Boolean(data.include_none_iceberg_catalogs),
      );
    } catch (e) {
      setCatalogError(e.message);
      setCatalogTables(null);
    } finally {
      setCatalogLoading(false);
    }
  }

  function selectCatalogTable(name) {
    setTableName(name);
  }

  return (
    <div className="flex-1 flex flex-col">
      <main className="flex-1 flex items-center justify-center p-8">
        <div className="bg-surface rounded-2xl shadow-xl p-10 w-full max-w-lg border border-edge">
          <div className="flex flex-col items-center mb-6">
            <img
              src={logo}
              alt="IceGraph"
              className="h-20 w-20 object-contain mb-3"
            />
            <h1 className="text-2xl font-bold text-ink">IceGraph</h1>
          </div>

          <h2 className="text-xl font-bold text-ink mb-1">Visualize a Table</h2>
          <p className={`${UI_BODY_MUTED_CLASS} mb-7`}>
            Enter an Iceberg table name to explore its metadata graph.
          </p>

          <form onSubmit={handleSubmit} className="flex flex-col gap-5">
            <div>
              <div className="flex items-center justify-between mb-1.5">
                <label className={UI_FORM_LABEL_CLASS}>Table Name</label>
                <button
                  type="button"
                  onClick={fetchCatalogTables}
                  disabled={catalogLoading}
                  className={UI_LINK_BUTTON_CLASS}
                >
                  {catalogLoading ? "Loading…" : "Browse catalog"}
                </button>
              </div>
              <input
                list="table-history"
                type="text"
                required
                value={tableName}
                onChange={(e) => setTableName(e.target.value)}
                placeholder="default.my_table"
                className={UI_TEXT_INPUT_LG_CLASS}
              />
              <datalist id="table-history">
                {history.map((item) => (
                  <option key={item} value={item} />
                ))}
              </datalist>

              {catalogError && (
                <p className={UI_ERROR_TEXT_SPACED_CLASS}>{catalogError}</p>
              )}

              <CatalogTableList
                tables={catalogTables}
                selectedName={tableName}
                onSelect={selectCatalogTable}
                filter={catalogFilter}
                onFilterChange={setCatalogFilter}
                includeNoneIcebergCatalogs={includeNoneIcebergCatalogs}
              />
            </div>

            <button type="submit" className={`${UI_PRIMARY_BUTTON_CLASS} mt-1`}>
              Continue
            </button>
          </form>
        </div>
      </main>

      <footer className={UI_FOOTER_TEXT_CLASS}>
        IceGraph — Apache Iceberg Metadata Visualizer
      </footer>
    </div>
  );
}
