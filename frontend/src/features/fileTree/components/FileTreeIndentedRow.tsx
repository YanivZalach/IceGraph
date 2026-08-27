import type { ReactNode } from "react";

interface FileTreeIndentedRowProps {
  children: ReactNode;
  depth: number;
}

const FileTreeIndentedRow = ({ children, depth }: FileTreeIndentedRowProps) => (
  <div role="none" className="flex w-full">
    {Array.from({ length: depth }, (_, index) => (
      <span key={index} aria-hidden="true" className="w-4 shrink-0" />
    ))}
    <div role="none" className="min-w-0 flex-1">
      {children}
    </div>
  </div>
);

export default FileTreeIndentedRow;
