import type { PartitionSpec, SortOrder } from "../table/api/metadataSchemas";

export type SpecFieldStatus = "added" | "removed" | "changed" | "unchanged";

export type PartitionField = NonNullable<PartitionSpec["fields"]>[number];
export type SortField = NonNullable<SortOrder["fields"]>[number];

export interface SpecFieldRow<TField> {
  status: SpecFieldStatus | null;
  field: TField;
  previous: TField | null;
}

// Both the Specs panel and the metadata page render the same tables. The panel
// supplies diff entries from diffFieldLists, the metadata page supplies a plain
// list, so each is normalized to one row shape.
export const plainSpecRows = <TField>(
  fields: readonly TField[] | undefined,
): SpecFieldRow<TField>[] =>
  (fields ?? []).map((field) => ({ status: null, field, previous: null }));

interface DiffEntry<TField> {
  status: SpecFieldStatus;
  field?: TField;
  prev?: TField;
  curr?: TField;
}

export const diffSpecRows = <TField>(
  diff: readonly DiffEntry<TField>[] | undefined,
): SpecFieldRow<TField>[] =>
  (diff ?? []).flatMap((entry) => {
    const field = entry.status === "changed" ? entry.curr : entry.field;
    if (field === undefined) return [];
    return [
      {
        status: entry.status,
        field,
        previous: entry.status === "changed" ? (entry.prev ?? null) : null,
      },
    ];
  });

export const SPEC_ROW_BACKGROUND: Record<SpecFieldStatus, string> = {
  added: "bg-green-900/20",
  removed: "bg-red-900/20",
  changed: "bg-amber-900/20",
  unchanged: "opacity-50",
};

export const SPEC_ROW_MARKER: Record<SpecFieldStatus, string | null> = {
  added: "+",
  removed: "−",
  changed: "~",
  unchanged: null,
};
