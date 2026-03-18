const KEY = "fleury_selected_company_ids"

export function getSelectedCompanyIds(): string[] {
  try {
    const raw = localStorage.getItem(KEY)
    if (!raw) return []
    const parsed = JSON.parse(raw) as unknown
    return Array.isArray(parsed) ? parsed.filter((x): x is string => typeof x === "string") : []
  } catch {
    return []
  }
}

export function setSelectedCompanyIds(ids: string[]) {
  localStorage.setItem(KEY, JSON.stringify(ids))
}
