const KEY = "fleury_selected_company_ids"
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i

function normalizeSelectedCompanyIds(value: unknown): string[] {
  if (!Array.isArray(value)) return []

  return Array.from(
    new Set(
      value
        .map((entry) => (typeof entry === "string" ? entry.trim() : ""))
        .filter((entry) => UUID_PATTERN.test(entry)),
    ),
  )
}

export function getSelectedCompanyIds(): string[] {
  try {
    const raw = localStorage.getItem(KEY)
    if (!raw) return []
    const parsed = JSON.parse(raw) as unknown
    const normalized = normalizeSelectedCompanyIds(parsed)
    if (Array.isArray(parsed) && JSON.stringify(parsed) !== JSON.stringify(normalized)) {
      localStorage.setItem(KEY, JSON.stringify(normalized))
    }
    return normalized
  } catch {
    return []
  }
}

export function setSelectedCompanyIds(ids: string[]) {
  localStorage.setItem(KEY, JSON.stringify(normalizeSelectedCompanyIds(ids)))
}
