import Chip from "../Chip";

interface ImpactCountProps {
  added: bigint;
  removed: bigint;
  unit: "rows" | "files";
}

const numberFormatter = new Intl.NumberFormat("en-US");

const SINGULAR_UNIT = { rows: "row", files: "file" } as const;

const ImpactCount = ({ added, removed, unit }: ImpactCountProps) => (
  <span className="inline-flex items-center gap-2 whitespace-nowrap">
    {added > 0n && (
      <Chip text={`+${numberFormatter.format(added)}`} tone="added" />
    )}
    {removed > 0n && (
      <Chip text={`−${numberFormatter.format(removed)}`} tone="removed" />
    )}
    {added + removed === 1n ? SINGULAR_UNIT[unit] : unit}
  </span>
);

export default ImpactCount;
