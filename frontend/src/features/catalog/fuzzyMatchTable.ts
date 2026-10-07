export interface TableMatch {
  name: string;
  matchedIndexes: Set<number>;
}

export interface SearchableTable {
  name: string;
  lowerName: string;
}

export interface ScoredTable {
  table: SearchableTable;
  score: number;
  indexes: number[];
  isExact: boolean;
}

interface PreparedQuery {
  tokens: string[];
  remainingTexts: string[];
  searchKey: string;
  exactName: string;
}

interface LastSearch {
  searchKey: string;
  candidates: SearchableTable[];
}

interface TokenMatch {
  indexes: number[];
  score: number;
}

const SEGMENT_SEPARATORS = new Set([".", "_", "-", " "]);

const isSegmentStart = (name: string, index: number): boolean =>
  index === 0 || SEGMENT_SEPARATORS.has(name.charAt(index - 1));

const isSubsequence = (
  name: string,
  text: string,
  fromIndex: number,
): boolean => {
  let cursor = fromIndex;
  for (const character of text) {
    const index = name.indexOf(character, cursor);
    if (index === -1) return false;
    cursor = index + 1;
  }
  return true;
};

const matchSubstring = (
  name: string,
  token: string,
  fromIndex: number,
  remainingText: string,
): TokenMatch | null => {
  let start = -1;
  for (
    let candidate = name.indexOf(token, fromIndex);
    candidate !== -1;
    candidate = name.indexOf(token, candidate + 1)
  ) {
    if (!isSubsequence(name, remainingText, candidate + token.length)) break;
    if (start === -1) start = candidate;
    if (isSegmentStart(name, candidate)) {
      start = candidate;
      break;
    }
  }
  if (start === -1) return null;
  return {
    indexes: Array.from(
      { length: token.length },
      (_, offset) => start + offset,
    ),
    score: token.length * 4 + (isSegmentStart(name, start) ? 8 : 0),
  };
};

const matchSubsequence = (
  name: string,
  token: string,
  fromIndex: number,
): TokenMatch | null => {
  const indexes: number[] = [];
  let score = 0;
  let cursor = fromIndex;
  for (const character of token) {
    const index = name.indexOf(character, cursor);
    if (index === -1) return null;
    const isContiguous = indexes.at(-1) === index - 1;
    score += 1 + (isContiguous ? 2 : 0) + (isSegmentStart(name, index) ? 3 : 0);
    indexes.push(index);
    cursor = index + 1;
  }
  return { indexes, score };
};

const PRINTABLE_ASCII = /^[ -~]*$/;

const toLowerCasePreservingLength = (name: string): string => {
  if (PRINTABLE_ASCII.test(name)) return name.toLowerCase();
  return name
    .split("")
    .map((character) => {
      const lowerCharacter = character.toLowerCase();
      return lowerCharacter.length === 1 ? lowerCharacter : character;
    })
    .join("");
};

const searchableTablesByNames = new WeakMap<string[], SearchableTable[]>();
const lastSearchByTables = new WeakMap<SearchableTable[], LastSearch>();

const scoreTable = (
  table: SearchableTable,
  { tokens, remainingTexts, searchKey, exactName }: PreparedQuery,
): ScoredTable | null => {
  const { lowerName } = table;
  if (!isSubsequence(lowerName, searchKey, 0)) return null;
  const indexes: number[] = [];
  let score = 0;
  let cursor = 0;
  for (const [tokenIndex, token] of tokens.entries()) {
    const match =
      matchSubstring(
        lowerName,
        token,
        cursor,
        remainingTexts[tokenIndex] ?? "",
      ) ?? matchSubsequence(lowerName, token, cursor);
    if (match === null) return null;
    indexes.push(...match.indexes);
    score += match.score;
    cursor = (match.indexes.at(-1) ?? cursor) + 1;
  }
  return {
    table,
    score: score - lowerName.length / 100,
    indexes,
    isExact: lowerName === exactName,
  };
};

const splitSearchQuery = (query: string): string[] =>
  toLowerCasePreservingLength(query)
    .split(/[\s.]+/)
    .filter(Boolean);

export const toSearchableTables = (names: string[]): SearchableTable[] => {
  const cached = searchableTablesByNames.get(names);
  if (cached !== undefined) return cached;
  const searchable = names.map((name) => ({
    name,
    lowerName: toLowerCasePreservingLength(name),
  }));
  searchableTablesByNames.set(names, searchable);
  return searchable;
};

const prepareQuery = (query: string): PreparedQuery => {
  const tokens = splitSearchQuery(query);
  return {
    tokens,
    remainingTexts: tokens.map((_, index) => tokens.slice(index + 1).join("")),
    searchKey: tokens.join(""),
    exactName: toLowerCasePreservingLength(query.trim()),
  };
};

export const searchTables = (
  tables: SearchableTable[],
  query: string,
): ScoredTable[] => {
  const preparedQuery = prepareQuery(query);
  const { searchKey } = preparedQuery;
  const lastSearch = lastSearchByTables.get(tables);
  const candidates =
    lastSearch !== undefined && searchKey.startsWith(lastSearch.searchKey)
      ? lastSearch.candidates
      : tables;
  const scored: ScoredTable[] = [];
  for (const table of candidates) {
    const result = scoreTable(table, preparedQuery);
    if (result !== null) scored.push(result);
  }
  lastSearchByTables.set(tables, {
    searchKey,
    candidates: scored.map(({ table }) => table),
  });
  return scored;
};

const compareScoredTables = (left: ScoredTable, right: ScoredTable): number => {
  if (left.isExact !== right.isExact) return left.isExact ? -1 : 1;
  if (left.score !== right.score) return right.score - left.score;
  if (left.table.name === right.table.name) return 0;
  return left.table.name < right.table.name ? -1 : 1;
};

export const rankTables = (
  scored: ScoredTable[],
  limit: number,
): TableMatch[] => {
  const best: ScoredTable[] = [];
  for (const candidate of scored) {
    const worst = best.at(-1);
    if (
      best.length >= limit &&
      worst !== undefined &&
      compareScoredTables(candidate, worst) >= 0
    ) {
      continue;
    }
    let insertAt = best.length;
    while (insertAt > 0) {
      const previous = best[insertAt - 1];
      if (
        previous === undefined ||
        compareScoredTables(candidate, previous) >= 0
      ) {
        break;
      }
      insertAt -= 1;
    }
    best.splice(insertAt, 0, candidate);
    if (best.length > limit) best.pop();
  }
  return best.map(({ table, indexes }) => ({
    name: table.name,
    matchedIndexes: new Set(indexes),
  }));
};
