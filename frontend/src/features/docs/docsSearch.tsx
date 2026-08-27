import { cloneElement, Fragment, isValidElement } from "react";
import type { ReactNode } from "react";
import Key from "./components/Key";
import ShortcutRow from "./components/ShortcutRow";

interface MatchState {
  count: number;
  target: number;
  key: number;
}

const escapeRegExp = (text: string): string => {
  return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
};

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

export const highlightMatch = (text: string, query: string): ReactNode => {
  if (!query) return text;

  const regex = new RegExp(`(${escapeRegExp(query)})`, "gi");

  return text.split(regex).map((part, index) => {
    if (part.toLowerCase() === query.toLowerCase()) {
      return (
        <mark key={index} className="bg-yellow-400 text-black px-1 rounded">
          {part}
        </mark>
      );
    }

    return part;
  });
};

export const highlightTreeMatches = (
  node: ReactNode,
  query: string,
  matchState: MatchState,
): ReactNode => {
  if (node == null || typeof node === "boolean") return node;

  if (typeof node === "string") {
    if (!node.toLowerCase().includes(query.toLowerCase())) return node;

    const regex = new RegExp(`(${escapeRegExp(query)})`, "gi");

    return node.split(regex).map((part) => {
      if (part.toLowerCase() !== query.toLowerCase()) return part;

      const isActive = matchState.count === matchState.target;
      matchState.count += 1;

      return (
        <mark
          key={`match-${String(matchState.key++)}`}
          data-active-match={isActive ? "true" : undefined}
          className={
            isActive
              ? "bg-accent text-white px-1 rounded ring-2 ring-white"
              : "bg-yellow-400 text-black px-1 rounded"
          }
        >
          {part}
        </mark>
      );
    });
  }

  if (Array.isArray(node)) {
    return node.map((child: ReactNode, index: number) => (
      <Fragment key={String(index)}>
        {highlightTreeMatches(child, query, matchState)}
      </Fragment>
    ));
  }

  if (isValidElement<{ desc: ReactNode }>(node) && node.type === ShortcutRow) {
    return cloneElement(node, {
      desc: highlightTreeMatches(node.props.desc, query, matchState),
    });
  }

  if (isValidElement<{ k: ReactNode }>(node) && node.type === Key) {
    return cloneElement(node, {
      k: highlightTreeMatches(node.props.k, query, matchState),
    });
  }

  if (isValidElement<{ children?: ReactNode }>(node) && node.props.children) {
    return cloneElement(node, {
      children: highlightTreeMatches(node.props.children, query, matchState),
    });
  }

  return node;
};
export const extractText = (node: ReactNode): string => {
  if (typeof node === "string") return node;

  if (Array.isArray(node)) {
    return node.map(extractText).join(" ");
  }

  if (isValidElement<{ desc: ReactNode }>(node) && node.type === ShortcutRow) {
    return extractText(node.props.desc);
  }

  if (isValidElement<{ k: ReactNode }>(node) && node.type === Key) {
    return extractText(node.props.k);
  }

  if (isValidElement<{ children?: ReactNode }>(node) && node.props.children) {
    return extractText(node.props.children);
  }

  return "";
};
