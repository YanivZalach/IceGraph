import type { Highlight } from "./docs";

const SEARCH_HIGHLIGHT_SELECTOR = "mark[data-docs-search-highlight]";
const SEARCH_HIGHLIGHT_CLASS =
  "bg-accent text-white px-1 rounded ring-2 ring-white";

interface SearchableCharacter {
  node: Text;
  offset: number;
}

const getSearchableContent = (contentElement: HTMLDivElement) => {
  const characters: Array<SearchableCharacter | null> = [];
  let text = "";
  const treeWalker = document.createTreeWalker(
    contentElement,
    NodeFilter.SHOW_TEXT,
  );
  let currentNode = treeWalker.nextNode();

  while (currentNode) {
    if (!(currentNode instanceof Text)) {
      currentNode = treeWalker.nextNode();
      continue;
    }

    const nodeText = currentNode.nodeValue ?? "";
    if (text && !/\s$/.test(text) && nodeText && !/^\s/.test(nodeText)) {
      text += " ";
      characters.push(null);
    }

    for (let offset = 0; offset < nodeText.length; offset += 1) {
      const character = nodeText.charAt(offset);
      if (/\s/.test(character)) {
        if (!text || /\s$/.test(text)) continue;
        text += " ";
        characters.push(null);
      } else {
        text += character;
        characters.push({ node: currentNode, offset });
      }
    }

    currentNode = treeWalker.nextNode();
  }

  return { text: text.trim(), characters };
};

const markSearchMatch = (
  characters: Array<SearchableCharacter | null>,
  start: number,
  length: number,
) => {
  const matchedCharacters = characters.slice(start, start + length);
  const matchesByNode = new Map<Text, { start: number; end: number }>();

  for (const character of matchedCharacters) {
    if (!character) continue;
    const match = matchesByNode.get(character.node);
    if (match) {
      match.end = character.offset + 1;
    } else {
      matchesByNode.set(character.node, {
        start: character.offset,
        end: character.offset + 1,
      });
    }
  }

  let firstMark: HTMLElement | null = null;
  for (const [node, match] of [...matchesByNode].reverse()) {
    const range = document.createRange();
    range.setStart(node, match.start);
    range.setEnd(node, match.end);

    const mark = document.createElement("mark");
    mark.dataset.docsSearchHighlight = "true";
    mark.className = SEARCH_HIGHLIGHT_CLASS;
    range.surroundContents(mark);
    firstMark = mark;
  }

  firstMark?.scrollIntoView({ behavior: "smooth", block: "center" });
};

export const highlightSearchResult = (
  contentElement: HTMLDivElement,
  highlight: Highlight,
) => {
  for (const previousMark of contentElement.querySelectorAll(
    SEARCH_HIGHLIGHT_SELECTOR,
  )) {
    previousMark.replaceWith(...previousMark.childNodes);
  }
  contentElement.normalize();

  const { text, characters } = getSearchableContent(contentElement);
  const lowerText = text.toLowerCase();
  const lowerTerm = highlight.term.toLowerCase();
  let occurrenceIndex = 0;
  let matchIndex = lowerText.indexOf(lowerTerm);

  while (matchIndex !== -1) {
    if (occurrenceIndex === highlight.index) {
      markSearchMatch(characters, matchIndex, highlight.term.length);
      return;
    }

    occurrenceIndex += 1;
    matchIndex = lowerText.indexOf(lowerTerm, matchIndex + lowerTerm.length);
  }
};
