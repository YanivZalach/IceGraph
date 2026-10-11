import type { TableOptionGroup } from "./tableOptions";
import { cn } from "../../shared/lib/cn";

interface TableOptionIconProps {
  group: TableOptionGroup;
  className?: string;
}

const ICON_PATHS: Record<TableOptionGroup, string[]> = {
  Recent: ["M12 7v5l3 2", "M3.5 12a8.5 8.5 0 1 0 17 0a8.5 8.5 0 1 0 -17 0"],
  Tables: ["M4 5h16v14H4z", "M4 10h16", "M10 10v9"],
  Matches: ["M4 5h16v14H4z", "M4 10h16", "M10 10v9"],
  Typed: ["M5 12h13", "M13 7l5 5-5 5"],
};

const TableOptionIcon = ({ group, className }: TableOptionIconProps) => (
  <svg
    viewBox="0 0 24 24"
    className={cn("h-4 w-4 shrink-0", className)}
    fill="none"
    stroke="currentColor"
    strokeWidth={1.75}
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
  >
    {ICON_PATHS[group].map((path) => (
      <path key={path} d={path} />
    ))}
  </svg>
);

export default TableOptionIcon;
