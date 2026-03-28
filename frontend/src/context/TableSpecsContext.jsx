import { createContext, useContext, useState } from 'react'

const TableSpecsContext = createContext()

export function TableSpecsProvider({ children }) {
  const [detailsOpen, setDetailsOpen] = useState(false)
  const [selectionDetail, setSelectionDetail] = useState(null)
  return (
    <TableSpecsContext.Provider value={{ detailsOpen, setDetailsOpen, selectionDetail, setSelectionDetail }}>
      {children}
    </TableSpecsContext.Provider>
  )
}

export function useTableSpecs() {
  return useContext(TableSpecsContext)
}
