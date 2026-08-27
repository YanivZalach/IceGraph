import type { ReactNode } from "react";
import Key from "./Key";

interface ShortcutRowProps {
  keys: string[];
  desc: ReactNode;
}

const ShortcutRow = ({ keys, desc }: ShortcutRowProps) => (
  <div className="flex items-center gap-3 py-1.5 border-b border-[#1e2736]">
    <div className="flex items-center gap-1 shrink-0 min-w-[6rem]">
      {keys.map((keyLabel, index) => (
        <span key={keyLabel} className="flex items-center gap-1">
          {index > 0 && <span className="text-slate-600 text-xs">/</span>}
          <Key k={keyLabel} />
        </span>
      ))}
    </div>
    <span className="text-slate-300 text-sm">{desc}</span>
  </div>
);

export default ShortcutRow;
