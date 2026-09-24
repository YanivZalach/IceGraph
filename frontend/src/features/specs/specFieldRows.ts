import type { PartitionSpec, SortOrder } from "../table/api/metadataSchemas";

export type SpecFieldStatus = "added" | "removed" | "changed" | "unchanged";

export type PartitionField = NonNullable<PartitionSpec["fields"]>[number];
export type SortField = NonNullable<SortOrder["fields"]>[number];

export interface SpecFieldRow<TField> {
  status: SpecFieldStatus | null;
  field: TField;
  previous: TField | null;
  position: number;
  previousPosition: number | null;
}

type SpecFieldIdentity = string | number | bigint | null | undefined;

export const plainSpecRows = <TField>(
  fields: readonly TField[] | undefined,
): SpecFieldRow<TField>[] =>
  (fields ?? []).map((field, index) => ({
    status: null,
    field,
    previous: null,
    position: index + 1,
    previousPosition: null,
  }));

export const diffSpecRows = <TField>(
  previousFields: readonly TField[] | undefined,
  currentFields: readonly TField[] | undefined,
  identity: (field: TField) => SpecFieldIdentity,
  positionMatters = false,
): SpecFieldRow<TField>[] => {
  const previous = previousFields ?? [];
  const current = currentFields ?? [];
  const previousByIdentity = new Map<string, number[]>();
  previous.forEach((field, index) => {
    const value = identity(field);
    if (value === undefined || value === null) return;
    const key = String(value);
    const indexes = previousByIdentity.get(key) ?? [];
    indexes.push(index);
    previousByIdentity.set(key, indexes);
  });
  const matchedPrevious = new Set<number>();
  const rows = current.map((field, index): SpecFieldRow<TField> => {
    const value = identity(field);
    const candidates =
      value === undefined || value === null
        ? []
        : (previousByIdentity.get(String(value)) ?? []);
    const previousIndex = candidates.find(
      (candidate) => !matchedPrevious.has(candidate),
    );
    if (previousIndex === undefined) {
      return {
        status: "added",
        field,
        previous: null,
        position: index + 1,
        previousPosition: null,
      };
    }
    matchedPrevious.add(previousIndex);
    const previousField = previous[previousIndex];
    if (previousField === undefined)
      throw new Error("Matched spec field is missing");
    const hasChanged =
      JSON.stringify(previousField) !== JSON.stringify(field) ||
      (positionMatters && previousIndex !== index);
    return {
      status: hasChanged ? "changed" : "unchanged",
      field,
      previous: hasChanged ? previousField : null,
      position: index + 1,
      previousPosition: hasChanged ? previousIndex + 1 : null,
    };
  });
  previous.forEach((field, index) => {
    if (matchedPrevious.has(index)) return;
    rows.push({
      status: "removed",
      field,
      previous: null,
      position: index + 1,
      previousPosition: null,
    });
  });
  return rows;
};

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

export const SPEC_ROW_MARKER_TONE: Record<SpecFieldStatus, string> = {
  added: "text-green-400",
  removed: "text-red-400",
  changed: "text-amber-400",
  unchanged: "text-slate-500",
};
