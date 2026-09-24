import type { ReactNode } from "react";

interface SchemaCollectionMemberProps {
  id: ReactNode;
  name: "element" | "key" | "value";
  requiredness?: ReactNode;
}

const SchemaCollectionMember = ({
  id,
  name,
  requiredness,
}: SchemaCollectionMemberProps) => (
  <div className="flex flex-wrap items-center gap-2 font-mono">
    <span className="min-w-7 text-right text-sm text-slate-500">{id}</span>
    <span className="rounded border border-edge bg-canvas px-1.5 py-0.5 text-xs font-medium text-slate-300">
      {name}
    </span>
    {requiredness !== undefined && (
      <span className="text-xs text-slate-400">{requiredness}</span>
    )}
  </div>
);

export default SchemaCollectionMember;
