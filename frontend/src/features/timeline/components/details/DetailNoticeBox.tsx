import type { ReactNode } from "react";
import { cn } from "../../../../shared/lib/cn";

interface DetailNoticeBoxProps {
  kind: "error" | "warning";
  children: ReactNode;
}

const DetailNoticeBox = ({ kind, children }: DetailNoticeBoxProps) => (
  <p
    className={cn(
      "rounded border px-3 py-2 text-xs break-words whitespace-pre-line",
      kind === "error"
        ? "border-red-500/40 bg-red-500/10 text-red-300"
        : "border-amber-500/40 bg-amber-500/10 text-amber-300",
    )}
  >
    {children}
  </p>
);

export default DetailNoticeBox;
