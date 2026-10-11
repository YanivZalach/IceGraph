import { useRef, type KeyboardEvent } from "react";
import { createPortal } from "react-dom";
import TableSpotlightPanel from "../catalog/TableSpotlightPanel";

interface TableSpotlightProps {
  onOpenTable: (tableName: string) => void;
  onClose: () => void;
}

const FOCUSABLE_SELECTOR = "input, button:not([disabled])";

const TableSpotlight = ({ onOpenTable, onClose }: TableSpotlightProps) => {
  const dialogRef = useRef<HTMLDivElement>(null);

  const keepFocusInside = (event: KeyboardEvent<HTMLDivElement>): void => {
    const focusable = Array.from(
      dialogRef.current?.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR) ??
        [],
    );
    const first = focusable.at(0);
    const last = focusable.at(-1);
    if (first === undefined || last === undefined) return;
    const isLeaving = event.shiftKey
      ? document.activeElement === first
      : document.activeElement === last;
    if (!isLeaving) return;
    event.preventDefault();
    (event.shiftKey ? last : first).focus();
  };

  const handleKeyDown = (event: KeyboardEvent<HTMLDivElement>): void => {
    event.stopPropagation();
    if (event.nativeEvent.isComposing) return;
    if (event.key === "Escape") {
      event.preventDefault();
      onClose();
    } else if (event.key === "Tab") {
      keepFocusInside(event);
    }
  };

  return createPortal(
    <div
      className="fixed inset-0 z-[10000] flex items-start justify-center bg-black/40 px-4 pt-[12vh] backdrop-blur-sm"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-label="Switch table"
        className="w-full max-w-3xl shadow-2xl"
        onKeyDown={handleKeyDown}
        onKeyUp={(event) => {
          event.stopPropagation();
        }}
      >
        <TableSpotlightPanel
          inputId="table-spotlight-search"
          onOpenTable={onOpenTable}
          onClose={onClose}
          resultsClassName="h-[min(55vh,26rem)]"
        />
      </div>
    </div>,
    document.body,
  );
};

export default TableSpotlight;
