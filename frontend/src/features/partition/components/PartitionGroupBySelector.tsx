import { cn } from "../../../shared/lib/cn";

interface PartitionGroupBySelectorProps {
  fieldNames: string[];
  groupByFields: string[];
  onChange: (groupByFields: string[]) => void;
}

const CHIP_CLASS =
  "flex cursor-pointer items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-medium transition";

const PartitionGroupBySelector = ({
  fieldNames,
  groupByFields,
  onChange,
}: PartitionGroupBySelectorProps) => (
  <div
    role="group"
    aria-label="Group files by partition fields"
    className="flex flex-wrap items-center gap-1.5"
  >
    <span className="mr-1 text-xs font-bold uppercase tracking-wide text-slate-500">
      Group by
    </span>
    <button
      type="button"
      aria-pressed={groupByFields.length === 0}
      onClick={() => {
        onChange([]);
      }}
      className={cn(
        CHIP_CLASS,
        groupByFields.length === 0
          ? "border-accent bg-accent-muted text-white"
          : "border-edge text-slate-400 hover:border-edge-hover hover:text-ink",
      )}
    >
      None
    </button>
    {fieldNames.map((fieldName) => {
      const position = groupByFields.indexOf(fieldName);
      const isSelected = position !== -1;
      return (
        <button
          key={fieldName}
          type="button"
          aria-pressed={isSelected}
          title={
            isSelected
              ? "Remove from grouping"
              : "Add as the next grouping level"
          }
          onClick={() => {
            onChange(
              isSelected
                ? groupByFields.filter((name) => name !== fieldName)
                : [...groupByFields, fieldName],
            );
          }}
          className={cn(
            CHIP_CLASS,
            isSelected
              ? "border-accent bg-accent-muted text-white"
              : "border-edge text-slate-400 hover:border-edge-hover hover:text-ink",
          )}
        >
          {isSelected && (
            <span className="flex size-4 items-center justify-center rounded-full bg-accent text-[10px] font-bold text-white">
              {position + 1}
            </span>
          )}
          <span className="font-mono">{fieldName}</span>
        </button>
      );
    })}
  </div>
);

export default PartitionGroupBySelector;
