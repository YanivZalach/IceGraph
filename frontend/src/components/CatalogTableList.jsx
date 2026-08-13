import { UI_FILTER_INPUT_CLASS, UI_HELPER_TEXT_CLASS } from "../uiTypography";

export default function CatalogTableList({
  tables,
  selectedName,
  onSelect,
  filter,
  onFilterChange,
  listClassName = "max-h-48",
  includeNoneIcebergCatalogs = false,
}) {
  const filteredTables =
    tables?.filter((name) =>
      name.toLowerCase().includes(filter.trim().toLowerCase()),
    ) ?? [];

  if (!tables) return null;

  if (tables.length === 0) {
    return (
      <p className={`mt-2 ${UI_HELPER_TEXT_CLASS}`}>
        No tables found in the catalog.
      </p>
    );
  }

  return (
    <div className="mt-2 border border-edge rounded-lg overflow-hidden">
      <div className="px-3 py-2 border-b border-edge bg-surface-hover">
        <input
          type="text"
          value={filter}
          onChange={(e) => onFilterChange(e.target.value)}
          placeholder="Filter tables…"
          className={UI_FILTER_INPUT_CLASS}
        />
      </div>
      {includeNoneIcebergCatalogs && (
        <p className={`px-3 py-2 border-b border-edge ${UI_HELPER_TEXT_CLASS}`}>
          Includes non-Iceberg catalogs, so non-Iceberg tables may also appear.
        </p>
      )}
      {filteredTables.length === 0 ? (
        <p className={`px-3 py-2 ${UI_HELPER_TEXT_CLASS}`}>
          No tables match your filter.
        </p>
      ) : (
        <ul className={`overflow-y-auto divide-y divide-edge ${listClassName}`}>
          {filteredTables.map((name) => (
            <li key={name}>
              <button
                type="button"
                onClick={() => onSelect(name)}
                className={`w-full text-left px-3 py-2 text-sm font-mono transition ${
                  selectedName === name
                    ? "bg-accent-muted text-ink"
                    : "text-slate-300 hover:bg-surface-hover hover:text-ink"
                }`}
              >
                {name}
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
