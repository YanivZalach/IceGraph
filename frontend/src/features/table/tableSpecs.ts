import {
  createContext,
  useContext,
  type Dispatch,
  type SetStateAction,
} from "react";
import type { UseQueryResult } from "@tanstack/react-query";
import type { GraphData } from "./api/graphSchemas";
import type {
  IcebergInteger,
  TableMetadata,
  TableSchema,
  PartitionSpec,
  SortOrder,
} from "./api/metadataSchemas";

export interface SpecSelection {
  kind: "schema" | "partition" | "order";
  id: IcebergInteger;
}

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

export interface TableSpecsState {
  detailsOpen: boolean;
  setDetailsOpen: (isOpen: boolean) => void;
  selectionDetail: SpecDetail | null;
  unresolvedSelection: SpecSelection | null;
  clearSpecSelection: () => void;
  openSpec: (selection: SpecSelection) => void;
  graphQuery: UseQueryResult<GraphData>;
  collectionStages: Record<string, string> | null | undefined;
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
