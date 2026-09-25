import {
  createContext,
  useContext,
  type Dispatch,
  type SetStateAction,
} from "react";
import type { UseQueryResult } from "@tanstack/react-query";
import type { GraphData, GraphStages } from "../table/api/graphSchemas";
import type {
  IcebergInteger,
  TableMetadata,
  TableSchema,
  PartitionSpec,
  SortOrder,
} from "../table/api/metadataSchemas";

export interface SpecSelection {
  kind: "schema" | "partition" | "order";
  id: IcebergInteger;
}

export type SpecView = "full" | "diff";

export interface SpecSearch {
  specs?: "open";
  spec_kind?: SpecSelection["kind"];
  spec_id?: string;
}

export const isSpecsOverlayState = (value: unknown): boolean =>
  typeof value === "object" &&
  value !== null &&
  "specsOverlay" in value &&
  value.specsOverlay === true;

export const parseSpecSelection = (
  kind: unknown,
  id: unknown,
): SpecSelection | null =>
  (kind === "schema" || kind === "partition" || kind === "order") &&
  typeof id === "string" &&
  id !== ""
    ? { kind, id }
    : null;

export type SpecDetail =
  | { type: "schema"; id: IcebergInteger; label: string; data: TableSchema }
  | {
      type: "partition";
      id: IcebergInteger;
      label: string;
      data: PartitionSpec;
    }
  | { type: "order"; id: IcebergInteger; label: string; data: SortOrder };

const SPEC_KIND_LABELS: Record<SpecSelection["kind"], string> = {
  schema: "Schema",
  partition: "Partition",
  order: "Order",
};

export const specSelectionLabel = (selection: SpecSelection): string =>
  `${SPEC_KIND_LABELS[selection.kind]} ID: ${String(selection.id)}`;

export const resolveSpecSelection = (
  metadata: TableMetadata | undefined,
  selection: SpecSelection | null,
): SpecDetail | null => {
  if (!metadata || !selection) return null;
  const { kind, id } = selection;
  const label = specSelectionLabel(selection);
  if (kind === "schema") {
    const data = metadata.schemas?.find(
      (item) => String(item["schema-id"]) === String(id),
    );
    return data ? { type: kind, id: data["schema-id"], label, data } : null;
  }
  if (kind === "partition") {
    const data = metadata["partition-specs"]?.find(
      (item) => String(item["spec-id"]) === String(id),
    );
    return data ? { type: kind, id: data["spec-id"], label, data } : null;
  }
  const data = metadata["sort-orders"]?.find(
    (item) => String(item["order-id"]) === String(id),
  );
  return data ? { type: kind, id: data["order-id"], label, data } : null;
};

type VersionedSpec = TableSchema | PartitionSpec | SortOrder;

const icebergInteger = (value: unknown): IcebergInteger => {
  if (typeof value === "string" || typeof value === "number") return value;
  throw new Error("Spec ID is missing");
};

const specId = (spec: VersionedSpec): IcebergInteger => {
  if ("schema-id" in spec) return icebergInteger(spec["schema-id"]);
  if ("spec-id" in spec) return icebergInteger(spec["spec-id"]);
  return spec["order-id"];
};

export const findPreviousSpec = <T extends VersionedSpec>(
  items: readonly T[],
  selected: T,
): T | null => {
  const selectedId = BigInt(String(specId(selected)));
  let previous: T | null = null;
  let previousId: bigint | null = null;
  items.forEach((item) => {
    const itemId = BigInt(String(specId(item)));
    if (itemId < selectedId && (previousId === null || itemId > previousId)) {
      previous = item;
      previousId = itemId;
    }
  });
  return previous;
};

export const hasPreviousSpec = (
  metadata: TableMetadata | undefined,
  selection: SpecSelection,
): boolean => {
  const detail = resolveSpecSelection(metadata, selection);
  if (detail === null) return false;
  if (detail.type === "schema")
    return findPreviousSpec(metadata?.schemas ?? [], detail.data) !== null;
  if (detail.type === "partition")
    return (
      findPreviousSpec(metadata?.["partition-specs"] ?? [], detail.data) !==
      null
    );
  return (
    findPreviousSpec(metadata?.["sort-orders"] ?? [], detail.data) !== null
  );
};

export interface TableSpecsState {
  detailsOpen: boolean;
  setDetailsOpen: (isOpen: boolean) => void;
  selectionDetail: SpecDetail | null;
  unresolvedSelection: SpecSelection | null;
  clearSpecSelection: () => void;
  openSpec: (selection: SpecSelection) => void;
  specView: SpecView;
  setSpecView: (view: SpecView) => void;
  specsMetadata: TableMetadata | undefined;
  graphQuery: UseQueryResult<GraphData>;
  collectionStages: GraphStages | null | undefined;
  rebuildGraph: () => Promise<void>;
  errors: Record<string, unknown>;
  warnings: Record<string, unknown>;
  issuesOpen: boolean;
  setIssuesOpen: Dispatch<SetStateAction<boolean>>;
}

export const TableSpecsContext = createContext<TableSpecsState | null>(null);

export const useTableSpecs = (): TableSpecsState => {
  const context = useContext(TableSpecsContext);
  if (context === null)
    throw new Error("useTableSpecs must be used under its provider");
  return context;
};
