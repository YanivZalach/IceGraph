import {
  rankTables,
  searchTables,
  toSearchableTables,
  type TableMatch,
} from "./fuzzyMatchTable";

export type TableOptionGroup = "Recent" | "Tables" | "Matches" | "Typed";

export interface TableOption extends TableMatch {
  group: TableOptionGroup;
}

export type GroupCounts = Partial<Record<TableOptionGroup, number>>;

interface TableOptions {
  options: TableOption[];
  hiddenCount: number;
  groupCounts: GroupCounts;
}

const MAX_VISIBLE_TABLES = 100;

export const optionKey = (option: TableOption): string =>
  `${option.group}:${option.name}`;

const unmatchedOptions = (
  names: string[],
  group: TableOptionGroup,
): TableOption[] =>
  names.map((name) => ({ name, matchedIndexes: new Set(), group }));

export const buildTableOptions = (
  query: string,
  recentTables: string[],
  catalogTables: string[],
): TableOptions => {
  const typedName = query.trim();
  if (typedName === "") {
    const catalogLimit = MAX_VISIBLE_TABLES - recentTables.length;
    return {
      options: [
        ...unmatchedOptions(recentTables, "Recent"),
        ...unmatchedOptions(catalogTables.slice(0, catalogLimit), "Tables"),
      ],
      hiddenCount: catalogTables.length - catalogLimit,
      groupCounts: {
        Recent: recentTables.length,
        Tables: catalogTables.length,
      },
    };
  }
  const recentOutsideCatalog = recentTables.filter(
    (name) => !catalogTables.includes(name),
  );
  const scored = [
    ...searchTables(toSearchableTables(catalogTables), query),
    ...searchTables(toSearchableTables(recentOutsideCatalog), query),
  ];
  const hasExactMatch = scored.some(({ isExact }) => isExact);
  const matchLimit = hasExactMatch
    ? MAX_VISIBLE_TABLES
    : MAX_VISIBLE_TABLES - 1;
  const typedOptions: TableOption[] = hasExactMatch
    ? []
    : [{ name: typedName, matchedIndexes: new Set(), group: "Typed" }];
  return {
    options: [
      ...rankTables(scored, matchLimit).map((match) => ({
        ...match,
        group: "Matches" as const,
      })),
      ...typedOptions,
    ],
    hiddenCount: scored.length - matchLimit,
    groupCounts: { Matches: scored.length },
  };
};
