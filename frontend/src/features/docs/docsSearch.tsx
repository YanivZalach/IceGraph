import type { ReactNode } from "react";

const escapeRegExp = (text: string): string =>
  text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

export const findAllIndices = (text: string, query: string): number[] => {
  const indices = [];
  const lowerText = text.toLowerCase();
  const lowerQuery = query.toLowerCase();
  let from = 0;
  let index = lowerText.indexOf(lowerQuery, from);

  while (index !== -1) {
    indices.push(index);
    from = index + lowerQuery.length;
    index = lowerText.indexOf(lowerQuery, from);
  }

  return indices;
};

export const markdownToSearchText = (markdown: string): string =>
  markdown
    .replace(/```[^\n]*\n/g, "")
    .replace(/```/g, "")
    .replace(/\[([^\]]+)]\([^)]+\)/g, "$1")
    .replace(/[*#`]/g, "")
    .replace(/^\s*-\s+/gm, "")
    .replace(/\s+/g, " ")
    .trim();

export const highlightMatch = (text: string, query: string): ReactNode => {
  const regularExpression = new RegExp(`(${escapeRegExp(query)})`, "gi");

  return text.split(regularExpression).map((part, index) =>
    part.toLowerCase() === query.toLowerCase() ? (
      <mark
        key={String(index)}
        className="bg-yellow-400 text-black px-1 rounded"
      >
        {part}
      </mark>
    ) : (
      part
    ),
  );
};
