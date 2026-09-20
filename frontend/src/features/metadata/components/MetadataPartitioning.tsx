import type { PartitionSpec } from "../../table/api/metadataSchemas";
import type { SpecSelection } from "../../table/tableSpecs";
import PartitionFieldTable from "../../specs/components/PartitionFieldTable";
import { plainSpecRows } from "../../specs/specFieldRows";

interface MetadataPartitioningProps {
  partitionSpec: PartitionSpec | undefined;
  columnNames: ReadonlyMap<string, string>;
  onOpenSpec: (selection: SpecSelection) => void;
}

const MetadataPartitioning = ({
  partitionSpec,
  columnNames,
  onOpenSpec,
}: MetadataPartitioningProps) => {
  const partitionFields = partitionSpec?.fields;
  const activeFields = partitionFields?.filter(
    (field) => field.transform !== "void",
  );
  const partitionSummary = !partitionFields
    ? "Partition fields are unavailable."
    : activeFields?.length === 0
      ? "No active partition fields. New data is not grouped by column values."
      : "Data is stored in separate sections (partitions) based on these columns. Filtering a query on them lets the engine read only the matching sections instead of the whole table.";
  return (
    <div className="overflow-hidden rounded-xl border border-edge bg-surface">
      <div className="p-5">
        <div className="flex items-center justify-between gap-3">
          <h3 className="text-sm font-semibold text-ink">Partitioning</h3>
          {partitionSpec && (
            <button
              type="button"
              aria-haspopup="dialog"
              className="cursor-pointer text-xs text-accent-text hover:underline"
              onClick={() => {
                onOpenSpec({ kind: "partition", id: partitionSpec["spec-id"] });
              }}
            >
              Spec {String(partitionSpec["spec-id"])} ↗
            </button>
          )}
        </div>
        <p className="mt-3 text-sm text-slate-400">{partitionSummary}</p>
      </div>
      {partitionFields && partitionFields.length > 0 && (
        <div className="border-t border-edge">
          <PartitionFieldTable
            rows={plainSpecRows(partitionFields)}
            columnNames={columnNames}
          />
        </div>
      )}
    </div>
  );
};
export default MetadataPartitioning;
