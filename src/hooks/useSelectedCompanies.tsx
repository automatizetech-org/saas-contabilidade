import { createContext, useContext, useState, useCallback, useEffect, type ReactNode } from "react"
import { getSelectedCompanyIds, setSelectedCompanyIds as persist } from "@/services/selectedCompaniesStorage"

type SelectedCompaniesContextValue = {
  selectedCompanyIds: string[]
  setSelectedCompanyIds: (ids: string[]) => void
}

const SelectedCompaniesContext = createContext<SelectedCompaniesContextValue | null>(null)

export function SelectedCompaniesProvider({ children }: { children: ReactNode }) {
  const [selectedCompanyIds, setState] = useState<string[]>(() => getSelectedCompanyIds())

  const setSelectedCompanyIds = useCallback((ids: string[]) => {
    persist(ids)
    setState(ids)
  }, [])

  useEffect(() => {
    const handler = () => setState(getSelectedCompanyIds())
    window.addEventListener("storage", handler)
    return () => window.removeEventListener("storage", handler)
  }, [])

  return (
    <SelectedCompaniesContext.Provider value={{ selectedCompanyIds, setSelectedCompanyIds }}>
      {children}
    </SelectedCompaniesContext.Provider>
  )
}

export function useSelectedCompanyIds() {
  const ctx = useContext(SelectedCompaniesContext)
  return ctx ?? { selectedCompanyIds: [], setSelectedCompanyIds: () => {} }
}
