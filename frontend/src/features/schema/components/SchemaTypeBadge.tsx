import type { ReactNode } from "react";
import { cn } from "../../../shared/lib/cn";

type SchemaTypeBadgeKind = "primitive" | "struct" | "list" | "map";

const BADGE_STYLE: Record<SchemaTypeBadgeKind, string> = {
  primitive: "bg-accent-muted text-accent",
  struct: "bg-violet-900/30 text-violet-400",
  list: "bg-amber-900/40 text-amber-400",
  map: "bg-emerald-900/40 text-emerald-400",
};

interface SchemaTypeBadgeProps {
  kind: SchemaTypeBadgeKind;
  children: ReactNode;
  className?: string | undefined;
}

const SchemaTypeBadge = ({
  kind,
  children,
  className,
}: SchemaTypeBadgeProps) => (
  <span
    className={cn(
      "w-fit rounded px-2 py-0.5 font-mono text-xs",
      BADGE_STYLE[kind],
      className,
    )}
  >
    {children}
  </span>
);

export default SchemaTypeBadge;
