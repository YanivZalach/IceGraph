import { useEffect, useRef } from "react";
import { useHotkey } from "@tanstack/react-hotkeys";
import { specSelectionLabel, useTableSpecs } from "../tableSpecs";
import { parseIcebergSchema } from "../../schema/schemaModel";
import {
  integerText,
  schemaColumnNames,
} from "../../metadata/metadataPresentation";
import {
  UI_DIALOG_TITLE_CLASS,
  UI_MONO_MUTED_CLASS,
} from "../../../uiTypography";
import SpecHistory from "./SpecHistory";
import SpecDetailPanel from "./SpecDetailPanel";

const FOCUSABLE_SELECTOR = 'button:not([disabled]), [href], [tabindex="0"]';

const SpecDetailsOverlay = () => {
  const {
    detailsOpen,
    setDetailsOpen,
    selectionDetail,
    unresolvedSelection,
    clearSpecSelection,
    openSpec,
    specView,
    setSpecView,
    specsMetadata,
    issuesOpen,
  } = useTableSpecs();
  const dialogRef = useRef<HTMLDivElement>(null);
  const isOpen = detailsOpen && specsMetadata !== undefined;

  useEffect(() => {
    if (!isOpen) return;
    const previousFocus = document.activeElement;
    const dialog = dialogRef.current;
    const focusable = (): HTMLElement[] => [
      ...(dialog?.querySelectorAll<HTMLElement>(FOCUSABLE_SELECTOR) ?? []),
    ];
    focusable()[0]?.focus();
    const trapFocus = (event: KeyboardEvent): void => {
      if (event.key !== "Tab") return;
      const elements = focusable();
      const first = elements[0];
      const last = elements[elements.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last?.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first?.focus();
      }
    };
    dialog?.addEventListener("keydown", trapFocus);
    return () => {
      dialog?.removeEventListener("keydown", trapFocus);
      if (previousFocus instanceof HTMLElement && previousFocus.isConnected)
        previousFocus.focus({ preventScroll: true });
    };
  }, [isOpen]);

  useHotkey(
    "Escape",
    () => {
      setDetailsOpen(false);
    },
    { conflictBehavior: "allow", enabled: detailsOpen && !issuesOpen },
  );

  if (!isOpen) return null;
  const currentSchema = specsMetadata.schemas?.find(
    (schema) =>
      integerText(schema["schema-id"]) ===
      integerText(specsMetadata["current-schema-id"]),
  );
  const columnNames = schemaColumnNames(parseIcebergSchema(currentSchema));
  const handleClose = (): void => {
    setDetailsOpen(false);
  };

  return (
    <div
      className="fixed inset-0 z-[9999] bg-black/50 flex items-center justify-center font-sans"
      onClick={handleClose}
    >
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-label="Table Specification"
        className="w-[90vw] max-w-6xl bg-surface rounded-xl shadow-2xl border border-edge max-h-[80dvh] flex flex-col"
        onClick={(event) => {
          event.stopPropagation();
        }}
      >
        <div className="flex items-center justify-between px-6 py-4 border-b border-edge shrink-0">
          <div>
            <div className={UI_DIALOG_TITLE_CLASS}>Table Specification</div>
            <div className={`${UI_MONO_MUTED_CLASS} mt-0.5`}>
              {specsMetadata["table-name"]}
            </div>
          </div>
          <button
            type="button"
            aria-label="Close Specs"
            className="w-7 h-7 rounded-full bg-edge text-slate-400 flex items-center justify-center text-base cursor-pointer hover:bg-edge-hover hover:text-slate-200 transition"
            onClick={handleClose}
          >
            ✕
          </button>
        </div>

        <div className="overflow-y-auto px-6 py-5 flex flex-col gap-4">
          <SpecHistory
            metadata={specsMetadata}
            onSelect={(kind, id) => {
              openSpec({ kind, id });
            }}
            selection={selectionDetail}
          />

          {unresolvedSelection && (
            <div className="rounded-xl border border-amber-500/40 bg-amber-900/20 px-4 py-3">
              <p className="text-sm text-amber-300">
                {specSelectionLabel(unresolvedSelection)} is not in this
                table&apos;s loaded metadata. It may belong to another table or
                another metadata version.
              </p>
              <button
                type="button"
                className="mt-2 text-xs text-accent-text hover:underline cursor-pointer"
                onClick={clearSpecSelection}
              >
                Clear selection
              </button>
            </div>
          )}

          {selectionDetail && (
            <SpecDetailPanel
              metadata={specsMetadata}
              detail={selectionDetail}
              specView={specView}
              columnNames={columnNames}
              onViewChange={setSpecView}
              onClear={clearSpecSelection}
            />
          )}
        </div>
      </div>
    </div>
  );
};
export default SpecDetailsOverlay;
