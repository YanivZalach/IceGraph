import { createContext, useContext, useState } from "react";

const TableSpecsContext = createContext();

export function TableSpecsProvider({ children }) {
  const [detailsOpen, setDetailsOpen] = useState(false);
  const [selectionDetail, setSelectionDetail] = useState(null);
  const [rawData, setRawData] = useState(null);
  const [errors, setErrors] = useState({});
  const [warnings, setWarnings] = useState({});
  const [issuesOpen, setIssuesOpen] = useState(false);
  return (
    <TableSpecsContext.Provider
      value={{
        detailsOpen,
        setDetailsOpen,
        selectionDetail,
        setSelectionDetail,
        rawData,
        setRawData,
        errors,
        setErrors,
        warnings,
        setWarnings,
        issuesOpen,
        setIssuesOpen,
      }}
    >
      {children}
    </TableSpecsContext.Provider>
  );
}

export function useTableSpecs() {
  return useContext(TableSpecsContext);
}
