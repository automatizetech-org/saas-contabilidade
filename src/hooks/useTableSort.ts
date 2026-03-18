import { useCallback, useMemo, useState } from "react"

export type SortDirection = "asc" | "desc" | null

/**
 * Ordenação global de tabela: 1º clique = crescente, 2º = decrescente, 3º = desmarca.
 * Uso: passar items e getValue(columnKey, item); toggleSort(columnKey) no header.
 */
export function useTableSort<T>(
  items: T[],
  getValue: (columnKey: string, item: T) => string | number | null | undefined
): {
  sortKey: string | null
  sortDirection: SortDirection
  toggleSort: (columnKey: string) => void
  sortedItems: T[]
} {
  const [state, setState] = useState<{ key: string | null; dir: SortDirection }>({ key: null, dir: null })

  const toggleSort = useCallback((columnKey: string) => {
    setState((prev) => {
      if (prev.key !== columnKey) return { key: columnKey, dir: "asc" as SortDirection }
      if (prev.dir === "asc") return { key: columnKey, dir: "desc" }
      return { key: null, dir: null }
    })
  }, [])

  const sortedItems = useMemo(() => {
    if (!state.key || !state.dir) return items
    return [...items].sort((a, b) => {
      const va = getValue(state.key!, a)
      const vb = getValue(state.key!, b)
      const aVal = va === null || va === undefined ? "" : String(va)
      const bVal = vb === null || vb === undefined ? "" : String(vb)
      const cmp = aVal.localeCompare(bVal, undefined, { numeric: true })
      return state.dir === "asc" ? cmp : -cmp
    })
  }, [items, state.key, state.dir, getValue])

  return {
    sortKey: state.key,
    sortDirection: state.dir,
    toggleSort,
    sortedItems,
  }
}
