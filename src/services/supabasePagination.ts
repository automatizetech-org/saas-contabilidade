type PagedResponse<T> = Promise<{
  data: T[] | null
  error: { message: string } | null
}>

const DEFAULT_PAGE_SIZE = 1000
const MAX_AUTO_PAGES = 5000

export async function fetchAllPages<T>(
  fetchPage: (from: number, to: number) => PagedResponse<T>,
  pageSize = DEFAULT_PAGE_SIZE,
): Promise<T[]> {
  const safePageSize = Math.max(1, pageSize)
  const rows: T[] = []

  for (let page = 0; page < MAX_AUTO_PAGES; page += 1) {
    const from = page * safePageSize
    const to = from + safePageSize - 1
    const { data, error } = await fetchPage(from, to)
    if (error) throw error

    const batch = data ?? []
    rows.push(...batch)

    if (batch.length < safePageSize) {
      return rows
    }
  }

  throw new Error("Limite interno de paginação excedido ao buscar dados do Supabase.")
}
