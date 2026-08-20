import { createContext, useContext } from "react";

export interface TableGraphData {
  nodes: unknown[];
  edges: unknown[];
  metadata: unknown;
  errors: Record<string, unknown>;
}

export const TableGraphDataContext = createContext<TableGraphData | null>(null);

export const useTableGraphData = (): TableGraphData => {
  const tableGraphData = useContext(TableGraphDataContext);
  if (tableGraphData === null) {
    throw new Error("useTableGraphData must be used under its provider");
  }
  return tableGraphData;
};
