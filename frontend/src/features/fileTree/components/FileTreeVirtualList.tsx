import { useRef } from "react";
import type { ReactNode } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { FileTreeRow } from "../fileTreeRows";

interface FileTreeVirtualListProps {
  renderRow: (row: FileTreeRow) => ReactNode;
  rows: FileTreeRow[];
}

const FileTreeVirtualList = ({ renderRow, rows }: FileTreeVirtualListProps) => {
  // TanStack Virtual exposes mutable functions that React Compiler cannot safely cache: https://react.dev/reference/react-compiler/directives/use-no-memo
  "use no memo";

  const scrollElementRef = useRef<HTMLDivElement>(null);
  const rowVirtualizer = useVirtualizer({
    count: rows.length,
    estimateSize: () => 62,
    getItemKey: (index) => rows[index]?.id ?? index,
    getScrollElement: () => scrollElementRef.current,
    overscan: 10,
  });

  return (
    <div
      ref={scrollElementRef}
      data-testid="file-tree-content-scroll"
      className="min-h-0 flex-1 overflow-y-auto"
    >
      <div
        role="list"
        aria-label="Data files by partition"
        className="relative w-full"
        // Runtime virtual-list measurements cannot be represented by static Tailwind classes: https://tanstack.com/virtual/latest/docs/framework/react/react-virtual
        style={{ height: `${String(rowVirtualizer.getTotalSize())}px` }}
      >
        {rowVirtualizer.getVirtualItems().map((virtualRow) => {
          const row = rows[virtualRow.index];
          if (row === undefined) return null;
          return (
            <div
              key={virtualRow.key}
              ref={rowVirtualizer.measureElement}
              role="none"
              data-index={virtualRow.index}
              className="absolute top-0 left-0 w-full pb-2"
              // TanStack Virtual supplies each row's runtime position: https://tanstack.com/virtual/latest/docs/framework/react/react-virtual
              style={{
                transform: `translateY(${String(virtualRow.start)}px)`,
              }}
            >
              {renderRow(row)}
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default FileTreeVirtualList;
