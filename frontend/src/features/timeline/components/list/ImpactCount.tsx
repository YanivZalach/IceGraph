interface ImpactCountProps {
  added: number;
  removed: number;
  unit: "rows" | "files";
}

const numberFormatter = new Intl.NumberFormat("en-US");

const SINGULAR_UNIT = { rows: "row", files: "file" } as const;

const ImpactCount = ({ added, removed, unit }: ImpactCountProps) => (
  <>
    {added > 0 && (
      <span className="text-green-400">+{numberFormatter.format(added)} </span>
    )}
    {removed > 0 && (
      <span className="text-red-400">−{numberFormatter.format(removed)} </span>
    )}
    {added + removed === 1 ? SINGULAR_UNIT[unit] : unit}
  </>
);

export default ImpactCount;
