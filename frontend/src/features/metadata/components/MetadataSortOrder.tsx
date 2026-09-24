import type { SortOrder } from "../../table/api/metadataSchemas";
import type { SpecSelection } from "../../specs/tableSpecs";
import SortFieldTable from "../../specs/components/SortFieldTable";
import { plainSpecRows } from "../../specs/specFieldRows";

interface MetadataSortOrderProps {
  sortOrder: SortOrder | undefined;
  columnNames: ReadonlyMap<string, string>;
  onOpenSpec: (selection: SpecSelection) => void;
}

const MetadataSortOrder = ({
  sortOrder,
  columnNames,
  onOpenSpec,
}: MetadataSortOrderProps) => {
  const sortFields = sortOrder?.fields;
  const sortSummary = !sortFields
    ? "Sort fields are unavailable."
    : sortFields.length === 0
      ? "No sort order configured. Row order is not guaranteed."
      : "This is the table's preferred write order. Writers may leave files unsorted, and query results are not guaranteed to follow it.";
  return (
    <div className="overflow-hidden rounded-xl border border-edge bg-surface">
      <div className="p-5">
        <div className="flex items-center justify-between gap-3">
          <h3 className="text-sm font-semibold text-ink">Write sort order</h3>
          {sortOrder && (
            <button
              type="button"
              aria-haspopup="dialog"
              className="cursor-pointer text-xs text-accent-text hover:underline"
              onClick={() => {
                onOpenSpec({ kind: "order", id: sortOrder["order-id"] });
              }}
            >
              Order {String(sortOrder["order-id"])} ↗
            </button>
          )}
        </div>
        <p className="mt-3 text-sm text-slate-400">{sortSummary}</p>
      </div>
      {sortFields && sortFields.length > 0 && (
        <div className="border-t border-edge">
          <SortFieldTable
            rows={plainSpecRows(sortFields)}
            columnNames={columnNames}
          />
        </div>
      )}
    </div>
  );
};
export default MetadataSortOrder;
