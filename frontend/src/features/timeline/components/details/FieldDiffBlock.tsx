import { useState } from "react";
import { cn } from "../../../../shared/lib/cn";
import {
  PANEL_COLLAPSE_TOGGLE_CLASS,
  PANEL_FIELD_LABEL_WIDE_CLASS,
} from "../../../../components/PanelContent";
import type {
  DescribedFieldDiff,
  DiffLine,
} from "../../lib/details/describeFieldDiff";

interface FieldDiffBlockProps {
  field: DescribedFieldDiff;
}

const CONTEXT_LINES = 2;

const SIGN = { removed: "-", added: "+", same: " " } as const;

const SCREEN_READER_TONE = {
  removed: "Removed: ",
  added: "Added: ",
  same: "",
} as const;

type VisibleItem =
  | { kind: "line"; index: number; line: DiffLine }
  | { kind: "gap"; index: number; count: number };

/** Keeps changed lines and a little context around them, like a git hunk. */
const changesWithContext = (lines: DiffLine[]): VisibleItem[] => {
  const isShown = lines.map(() => false);
  lines.forEach((line, index) => {
    if (line.tone === "same") {
      return;
    }
    const start = Math.max(0, index - CONTEXT_LINES);
    const end = Math.min(lines.length - 1, index + CONTEXT_LINES);
    for (let shown = start; shown <= end; shown++) {
      isShown[shown] = true;
    }
  });

  const items: VisibleItem[] = [];
  let hidden = 0;
  lines.forEach((line, index) => {
    if (isShown[index]) {
      if (hidden > 0) {
        items.push({ kind: "gap", index, count: hidden });
        hidden = 0;
      }
      items.push({ kind: "line", index, line });
    } else {
      hidden++;
    }
  });
  if (hidden > 0) {
    items.push({ kind: "gap", index: lines.length, count: hidden });
  }

  return items;
};

const allLines = (lines: DiffLine[]): VisibleItem[] =>
  lines.map((line, index) => ({ kind: "line", index, line }));

const unchangedText = (count: number): string =>
  `⋯ ${count.toString()} unchanged line${count === 1 ? "" : "s"}`;

const DiffLineRow = ({ line }: { line: DiffLine }) => (
  <div
    className={cn(
      "flex gap-2 px-4 py-0.5",
      line.tone === "removed" && "bg-red-500/10 text-red-400",
      line.tone === "added" && "bg-green-500/10 text-green-400",
      line.tone === "same" && "text-slate-300",
    )}
  >
    <span
      aria-hidden="true"
      className={cn(
        "w-3 shrink-0 text-center",
        line.tone === "same" ? "opacity-20" : "opacity-60",
      )}
    >
      {SIGN[line.tone]}
    </span>
    <span className="min-w-0 break-all whitespace-pre-wrap">
      <span className="sr-only">{SCREEN_READER_TONE[line.tone]}</span>
      {line.text}
    </span>
  </div>
);

const BracketRow = ({ text }: { text: string }) => (
  <div className="px-4 py-0.5 text-slate-500 opacity-60">{text}</div>
);

const FieldDiffBlock = ({ field }: FieldDiffBlockProps) => {
  const [isExpanded, setIsExpanded] = useState(false);
  const condensed = changesWithContext(field.lines);
  const isCondensable = condensed.some((item) => item.kind === "gap");
  const items =
    isExpanded || !isCondensable ? allLines(field.lines) : condensed;

  return (
    <div className="flex flex-col gap-2 py-3">
      <div className="flex items-center justify-between gap-2">
        <span className={PANEL_FIELD_LABEL_WIDE_CLASS}>
          {field.key.replace(/_/g, " ")}
        </span>
        {isCondensable && (
          <button
            type="button"
            onClick={() => {
              setIsExpanded((previous) => !previous);
            }}
            className={PANEL_COLLAPSE_TOGGLE_CLASS}
          >
            {isExpanded
              ? "Changes only"
              : `All ${field.lines.length.toString()} lines`}
          </button>
        )}
      </div>
      <div className="flex flex-col rounded-lg border border-edge bg-diff-bg py-2 font-mono text-xs">
        {field.openBracket !== null && <BracketRow text={field.openBracket} />}
        {items.map((item) =>
          item.kind === "line" ? (
            <DiffLineRow
              key={`line-${item.index.toString()}`}
              line={item.line}
            />
          ) : (
            <div
              key={`gap-${item.index.toString()}`}
              className="px-4 py-0.5 text-slate-500 italic"
            >
              {unchangedText(item.count)}
            </div>
          ),
        )}
        {field.closeBracket !== null && (
          <BracketRow text={field.closeBracket} />
        )}
      </div>
    </div>
  );
};

export default FieldDiffBlock;
