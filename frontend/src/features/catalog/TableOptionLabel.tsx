import type { ReactNode } from "react";
import type { TableOption } from "./tableOptions";

interface TableOptionLabelProps {
  option: TableOption;
}

interface TextSegment {
  text: string;
  isMatched: boolean;
  start: number;
}

const splitIntoSegments = (
  text: string,
  offset: number,
  matchedIndexes: Set<number>,
): TextSegment[] => {
  const segments: TextSegment[] = [];
  text.split("").forEach((character, index) => {
    const isMatched = matchedIndexes.has(offset + index);
    const previous = segments.at(-1);
    if (previous?.isMatched === isMatched) {
      previous.text += character;
    } else {
      segments.push({ text: character, isMatched, start: index });
    }
  });
  return segments;
};

const highlightText = (
  text: string,
  offset: number,
  matchedIndexes: Set<number>,
): ReactNode[] =>
  splitIntoSegments(text, offset, matchedIndexes).map((segment) =>
    segment.isMatched ? (
      <mark
        key={segment.start}
        className="bg-transparent font-bold text-accent-text"
      >
        {segment.text}
      </mark>
    ) : (
      <span key={segment.start}>{segment.text}</span>
    ),
  );

const TableOptionLabel = ({ option }: TableOptionLabelProps) => {
  if (option.group === "Typed") {
    return (
      <span className="min-w-0 truncate text-sm text-ink">
        Open <span className="font-mono">“{option.name}”</span> as typed
      </span>
    );
  }
  const separatorIndex = option.name.lastIndexOf(".");
  const tableName = option.name.slice(separatorIndex + 1);
  const namespace =
    separatorIndex === -1 ? "" : option.name.slice(0, separatorIndex);
  return (
    <span className="flex min-w-0 flex-col">
      <span className="truncate font-mono text-sm font-semibold text-ink">
        {highlightText(tableName, separatorIndex + 1, option.matchedIndexes)}
      </span>
      {namespace !== "" && (
        <span className="truncate font-mono text-xs text-slate-400">
          {highlightText(namespace, 0, option.matchedIndexes)}
        </span>
      )}
    </span>
  );
};

export default TableOptionLabel;
