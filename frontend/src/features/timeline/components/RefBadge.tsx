import { GitBranch, Tag } from "lucide-react";

interface RefBadgeProps {
  name: string;
  type: "branch" | "tag";
}

const RefBadge = ({ name, type }: RefBadgeProps) => (
  <span className="flex shrink-0 items-center gap-1 rounded-md border border-edge px-1.5 py-0.5 text-xs text-slate-300">
    {type === "branch" ? (
      <GitBranch className="size-3" />
    ) : (
      <Tag className="size-3" />
    )}
    {name}
  </span>
);

export default RefBadge;
