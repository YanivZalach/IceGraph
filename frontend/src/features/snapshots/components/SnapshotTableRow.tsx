import type { ReactNode } from "react";
import { cn } from "../../../shared/lib/cn";
import SnapshotRail, { type SnapshotRowMarking } from "./SnapshotRail";

interface SnapshotTableRowProps {
  label: string;
  marking: SnapshotRowMarking;
  onSelect: () => void;
  children: ReactNode;
}

const SnapshotTableRow = ({
  label,
  marking,
  onSelect,
  children,
}: SnapshotTableRowProps) => (
  <tr
    onClick={onSelect}
    className={cn(
      "cursor-pointer transition-colors has-[button:focus-visible]:outline-2 has-[button:focus-visible]:-outline-offset-2 has-[button:focus-visible]:outline-accent",
      marking.isEdge
        ? "bg-accent-muted/50 shadow-[inset_3px_0_0_var(--color-accent)]"
        : marking.isInRange
          ? "bg-accent/5 hover:bg-accent/10"
          : "hover:bg-surface-hover",
    )}
  >
    <td className="relative w-10 min-w-10">
      <SnapshotRail marking={marking} hasNode />
      <button
        type="button"
        aria-pressed={marking.isEdge}
        aria-label={label}
        onClick={(event) => {
          event.stopPropagation();
          onSelect();
        }}
        className="absolute inset-0 cursor-pointer focus:outline-none"
      />
    </td>
    {children}
  </tr>
);

export default SnapshotTableRow;
