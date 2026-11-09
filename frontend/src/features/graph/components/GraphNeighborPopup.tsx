import { useEffect, useRef, useState } from "react";
import { isKeyboardInputTarget } from "../../../shared/lib/keyboard";
import {
  UI_DIALOG_SECTION_TITLE_CLASS,
  UI_POPUP_HINT_CLASS,
} from "../../../uiTypography";
import type { GraphNode } from "../model/graphModel";
import {
  NEIGHBOR_POPUP_KEYS,
  findChoiceByCombo,
  type NeighborPopupState,
} from "../model/neighborPopup";
import { useScrollKeys } from "../hooks/useGraphKeyboard";
import { useSmoothScroll } from "../hooks/useSmoothScroll";

interface GraphNeighborPopupProps {
  popup: NeighborPopupState;
  onSelect: (node: GraphNode) => void;
  onClose: () => void;
}

const GraphNeighborPopup = ({
  popup,
  onSelect,
  onClose,
}: GraphNeighborPopupProps) => {
  const [typedCombo, setTypedCombo] = useState("");
  const listRef = useRef<HTMLDivElement>(null);
  const { scrollBy } = useSmoothScroll(listRef);
  useScrollKeys(true, scrollBy);

  useEffect(() => {
    const handleKeyDown = (event: KeyboardEvent): void => {
      if (event.ctrlKey || event.metaKey || event.altKey) return;
      if (isKeyboardInputTarget(event.target)) return;
      if (event.key === "Escape") {
        onClose();
        return;
      }
      const key = event.key.toLowerCase();
      if (key.length !== 1 || !NEIGHBOR_POPUP_KEYS.includes(key)) return;

      const combo = typedCombo + key;
      if (combo.length < popup.comboLength) {
        setTypedCombo(combo);
        return;
      }
      const choice = findChoiceByCombo(popup, combo);
      if (choice === null) setTypedCombo("");
      else onSelect(choice.node);
    };
    window.addEventListener("keydown", handleKeyDown);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
    };
  }, [popup, typedCombo, onSelect, onClose]);

  return (
    <div className="pointer-events-none absolute inset-0 z-[1100] flex items-center justify-center">
      <div className="pointer-events-auto w-[70dvw] max-w-4xl rounded-xl border border-edge bg-surface/57 p-4 font-sans shadow-2xl backdrop-blur-md">
        <div className={UI_DIALOG_SECTION_TITLE_CLASS}>
          {popup.direction === "parents"
            ? "Navigate to parent"
            : "Navigate to child"}
        </div>
        <div
          ref={listRef}
          className="flex max-h-[60dvh] flex-col overflow-y-auto"
        >
          {popup.choices.map(({ node, combo }) => (
            <button
              type="button"
              key={node.id}
              onClick={() => {
                onSelect(node);
              }}
              className="flex cursor-pointer items-center gap-3 rounded border-b border-edge px-2 py-2 text-left transition last:border-0 hover:bg-surface-hover"
            >
              <span className="shrink-0 rounded border border-accent bg-accent/35 px-1.5 py-0.5 font-mono text-xs font-bold tracking-widest text-white">
                {combo}
              </span>
              <span className="font-mono text-sm text-ink">{node.label}</span>
            </button>
          ))}
        </div>
        {popup.comboLength > 1 && (
          <div className="mt-3 min-h-[1.5em] text-center font-mono text-sm tracking-widest text-slate-300">
            {typedCombo || <span className="text-slate-600">type combo…</span>}
          </div>
        )}
        <div className={UI_POPUP_HINT_CLASS}>
          Type combo to select · Esc to cancel
        </div>
      </div>
    </div>
  );
};

export default GraphNeighborPopup;
