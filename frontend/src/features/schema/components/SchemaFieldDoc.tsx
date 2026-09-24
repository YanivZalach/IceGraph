import type { ReactNode } from "react";
import { cn } from "../../../shared/lib/cn";

interface SchemaFieldDocProps {
  children: ReactNode;
  className?: string;
}

const SchemaFieldDoc = ({ children, className }: SchemaFieldDocProps) => (
  <p className={cn("flex max-w-md gap-2 text-xs", className)}>
    <code className="shrink-0 font-mono text-slate-500">doc:</code>
    <span className="min-w-0 font-sans break-words text-slate-400">
      {children}
    </span>
  </p>
);

export default SchemaFieldDoc;
