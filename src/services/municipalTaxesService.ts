import { supabase } from "./supabaseClient"
import { fetchAllPages } from "./supabasePagination"
import type { Tables } from "@/types/database"

export type MunicipalTaxDebt = Tables<"municipal_tax_debts">
export type MunicipalTaxCollectionRun = Tables<"municipal_tax_collection_runs">
export type MunicipalTaxStatusClass = "vencido" | "a_vencer" | "regular"

export type MunicipalTaxDebtView = MunicipalTaxDebt & {
  company_name: string
  company_document: string | null
  status_class: MunicipalTaxStatusClass
  days_until_due: number | null
}

export type MunicipalTaxFilters = {
  companyIds?: string[] | null
  year?: string
  status?: MunicipalTaxStatusClass | "todos"
  dateFrom?: string
  dateTo?: string
  search?: string
}

export type MunicipalTaxPageSortKey =
  | "company_name"
  | "tributo"
  | "ano"
  | "numero_documento"
  | "data_vencimento"
  | "valor"
  | "situacao"
  | "status_class"
  | null

export type MunicipalTaxPageSortDirection = "asc" | "desc" | null

export type MunicipalTaxPageParams = MunicipalTaxFilters & {
  page: number
  pageSize: number
  sortKey?: MunicipalTaxPageSortKey
  sortDirection?: MunicipalTaxPageSortDirection
}

export type MunicipalTaxOverview = {
  cards: MunicipalTaxSummary
  byStatus: Array<{ key: MunicipalTaxStatusClass; name: string; total: number }>
  dueSoon: MunicipalTaxDebtView[]
  byCompany: Array<{ name: string; total: number }>
  byYear: Array<{ name: string; total: number }>
  years: number[]
}

export type MunicipalTaxDebtsPageResult = {
  items: MunicipalTaxDebtView[]
  total: number
}

export type MunicipalTaxSummary = {
  totalDebitos: number
  totalVencido: number
  totalAVencer: number
  quantidadeDebitos: number
  empresasComVencidos: number
  empresasProximasVencimento: number
  totalValor: number
}

const DAY_IN_MS = 24 * 60 * 60 * 1000

function normalizeBaseDate() {
  const today = new Date()
  return new Date(today.getFullYear(), today.getMonth(), today.getDate())
}

export function getMunicipalTaxStatusClass(dataVencimento: string | null, valor?: number | string | null): MunicipalTaxStatusClass {
  const numValor = typeof valor === "string" ? Number(valor) : Number(valor ?? 0)
  if (numValor === 0) return "regular"
  if (!dataVencimento) return "regular"
  const due = new Date(`${dataVencimento}T00:00:00`)
  const diff = Math.ceil((due.getTime() - normalizeBaseDate().getTime()) / DAY_IN_MS)
  if (diff < 0) return "vencido"
  if (diff <= 30) return "a_vencer"
  return "regular"
}

export function getMunicipalTaxDaysUntilDue(dataVencimento: string | null) {
  if (!dataVencimento) return null
  const due = new Date(`${dataVencimento}T00:00:00`)
  return Math.ceil((due.getTime() - normalizeBaseDate().getTime()) / DAY_IN_MS)
}

function matchesFilters(item: MunicipalTaxDebtView, filters: MunicipalTaxFilters) {
  if (filters.companyIds?.length && !filters.companyIds.includes(item.company_id)) return false
  if (filters.year && filters.year !== "todos" && String(item.ano || "") !== filters.year) return false
  if (filters.status && filters.status !== "todos" && item.status_class !== filters.status) return false
  if (filters.dateFrom && item.data_vencimento && item.data_vencimento < filters.dateFrom) return false
  if (filters.dateTo && item.data_vencimento && item.data_vencimento > filters.dateTo) return false
  const search = filters.search?.trim().toLowerCase()
  if (search) {
    const haystack = [
      item.company_name,
      item.tributo,
      item.numero_documento,
      item.situacao,
      item.company_document,
    ]
      .map((value) => String(value || "").toLowerCase())
      .join(" ")
    if (!haystack.includes(search)) return false
  }
  return true
}

export async function getMunicipalTaxDebts(filters: MunicipalTaxFilters = {}): Promise<MunicipalTaxDebtView[]> {
  const debts = await fetchAllPages<MunicipalTaxDebt>((from, to) => {
    let query = supabase
      .from("municipal_tax_debts")
      .select("*")
      .order("data_vencimento", { ascending: true })
      .order("tributo", { ascending: true })
      .range(from, to)

    if (filters.companyIds?.length) {
      query = query.in("company_id", filters.companyIds)
    }

    return query
  })

  const companyIds = [...new Set(debts.map((item) => item.company_id))]
  const companies = companyIds.length
    ? await fetchAllPages<{ id: string; name: string; document: string | null }>((from, to) =>
        supabase.from("companies").select("id, name, document").in("id", companyIds).order("name").range(from, to)
      )
    : []

  const companyMap = new Map(companies.map((company) => [company.id, company]))

  return debts
    .map((item) => ({
      ...item,
      company_name: companyMap.get(item.company_id)?.name ?? "Empresa sem nome",
      company_document: companyMap.get(item.company_id)?.document ?? null,
      status_class: getMunicipalTaxStatusClass(item.data_vencimento, item.valor),
      days_until_due: getMunicipalTaxDaysUntilDue(item.data_vencimento),
    }))
    .filter((item) => matchesFilters(item, filters))
}

function compareMunicipalDebtItems(a: MunicipalTaxDebtView, b: MunicipalTaxDebtView, sortKey: MunicipalTaxPageSortKey, sortDirection: MunicipalTaxPageSortDirection) {
  if (!sortKey || !sortDirection) {
    return String(a.data_vencimento ?? "").localeCompare(String(b.data_vencimento ?? "")) ||
      String(a.company_name ?? "").localeCompare(String(b.company_name ?? ""), "pt-BR")
  }

  const getValue = (item: MunicipalTaxDebtView) => {
    switch (sortKey) {
      case "company_name": return String(item.company_name ?? "").toLowerCase()
      case "tributo": return String(item.tributo ?? "").toLowerCase()
      case "ano": return Number(item.ano ?? 0)
      case "numero_documento": return String(item.numero_documento ?? "")
      case "data_vencimento": return String(item.data_vencimento ?? "")
      case "valor": return Number(item.valor ?? 0)
      case "situacao": return String(item.situacao ?? "").toLowerCase()
      case "status_class": return String(item.status_class ?? "")
      default: return ""
    }
  }

  const left = getValue(a)
  const right = getValue(b)
  const result =
    typeof left === "number" && typeof right === "number"
      ? left - right
      : String(left).localeCompare(String(right), "pt-BR", { numeric: true })

  return sortDirection === "desc" ? -result : result
}

export async function getMunicipalTaxOverview(filters: MunicipalTaxFilters = {}): Promise<MunicipalTaxOverview> {
  try {
    const { data, error } = await supabase.rpc("get_municipal_tax_overview_summary", {
      company_ids: filters.companyIds?.length ? filters.companyIds : null,
      year_filter: filters.year ?? null,
      status_filter: filters.status ?? null,
      date_from: filters.dateFrom ?? null,
      date_to: filters.dateTo ?? null,
      search_text: filters.search ?? null,
    })
    if (error) throw error

    const payload = (data ?? {}) as {
      cards?: Record<string, unknown>
      byStatus?: Array<{ key?: MunicipalTaxStatusClass; name?: string; total?: number }>
      dueSoon?: Array<Partial<MunicipalTaxDebtView>>
      byCompany?: Array<{ name?: string; total?: number }>
      byYear?: Array<{ name?: string; total?: number }>
      years?: Array<number | string>
    }

    return {
      cards: {
        totalDebitos: Number(payload.cards?.quantidadeDebitos ?? 0),
        totalVencido: Number(payload.cards?.totalVencido ?? 0),
        totalAVencer: Number(payload.cards?.totalAVencer ?? 0),
        quantidadeDebitos: Number(payload.cards?.quantidadeDebitos ?? 0),
        empresasComVencidos: Number(payload.cards?.empresasComVencidos ?? 0),
        empresasProximasVencimento: Number(payload.cards?.empresasProximasVencimento ?? 0),
        totalValor: Number(payload.cards?.totalValor ?? 0),
      },
      byStatus: (payload.byStatus ?? []).map((item) => ({
        key: (item.key ?? "regular") as MunicipalTaxStatusClass,
        name: item.name ?? "",
        total: Number(item.total ?? 0),
      })),
      dueSoon: (payload.dueSoon ?? []).map((item) => ({
        ...(item as MunicipalTaxDebtView),
        company_name: item.company_name ?? "Empresa sem nome",
        company_document: item.company_document ?? null,
        status_class: (item.status_class ?? "regular") as MunicipalTaxStatusClass,
        days_until_due: item.days_until_due == null ? null : Number(item.days_until_due),
      })),
      byCompany: (payload.byCompany ?? []).map((item) => ({
        name: item.name ?? "",
        total: Number(item.total ?? 0),
      })),
      byYear: (payload.byYear ?? []).map((item) => ({
        name: item.name ?? "",
        total: Number(item.total ?? 0),
      })),
      years: (payload.years ?? []).map((value) => Number(value)).filter((value) => Number.isFinite(value)),
    }
  } catch {
    const items = await getMunicipalTaxDebts(filters)
    const cards = getMunicipalTaxSummary(items)
    const today = new Date().toISOString().slice(0, 10)
    return {
      cards,
      byStatus: (["vencido", "a_vencer", "regular"] as MunicipalTaxStatusClass[]).map((status) => ({
        key: status,
        name: status === "vencido" ? "Vencido" : status === "a_vencer" ? "A vencer (proximos 30 dias)" : "Regular",
        total: items.filter((item) => item.status_class === status).length,
      })),
      dueSoon: [...items]
        .filter((item) => item.data_vencimento && item.data_vencimento >= today)
        .sort((a, b) => String(a.data_vencimento ?? "").localeCompare(String(b.data_vencimento ?? "")))
        .slice(0, 30),
      byCompany: [...new Map(items.map((item) => [item.company_name, 0])).keys()].map((name) => ({
        name,
        total: items.filter((item) => item.company_name === name).reduce((sum, item) => sum + Number(item.valor ?? 0), 0),
      })).sort((a, b) => b.total - a.total).slice(0, 8),
      byYear: [...new Map(items.map((item) => [String(item.ano ?? 0), 0])).keys()].map((name) => ({
        name,
        total: items.filter((item) => String(item.ano ?? 0) === name).reduce((sum, item) => sum + Number(item.valor ?? 0), 0),
      })).sort((a, b) => Number(a.name) - Number(b.name)),
      years: [...new Set(items.map((item) => item.ano).filter((value): value is number => typeof value === "number"))].sort((a, b) => b - a),
    }
  }
}

export async function getMunicipalTaxDebtsPage(params: MunicipalTaxPageParams): Promise<MunicipalTaxDebtsPageResult> {
  try {
    const { data, error } = await supabase.rpc("get_municipal_tax_debts_page", {
      company_ids: params.companyIds?.length ? params.companyIds : null,
      year_filter: params.year ?? null,
      status_filter: params.status ?? null,
      date_from: params.dateFrom ?? null,
      date_to: params.dateTo ?? null,
      search_text: params.search ?? null,
      sort_key: params.sortKey ?? null,
      sort_direction: params.sortDirection ?? "desc",
      page_number: params.page,
      page_size: params.pageSize,
    })
    if (error) throw error

    const rows = (data ?? []).map((item) => ({
      ...(item as MunicipalTaxDebtView),
      company_name: item.company_name ?? "Empresa sem nome",
      company_document: item.company_document ?? null,
      status_class: (item.status_class ?? "regular") as MunicipalTaxStatusClass,
      days_until_due: item.days_until_due == null ? null : Number(item.days_until_due),
    }))

    return {
      items: rows,
      total: Number(rows[0]?.total_count ?? 0),
    }
  } catch {
    const items = await getMunicipalTaxDebts(params)
    const sorted = [...items].sort((left, right) => compareMunicipalDebtItems(left, right, params.sortKey ?? null, params.sortDirection ?? null))
    const from = Math.max(0, (params.page - 1) * params.pageSize)
    const to = from + params.pageSize
    return {
      items: sorted.slice(from, to),
      total: sorted.length,
    }
  }
}

export function getMunicipalTaxSummary(items: MunicipalTaxDebtView[]): MunicipalTaxSummary {
  const empresasComVencidos = new Set(items.filter((item) => item.status_class === "vencido").map((item) => item.company_id)).size
  const empresasProximasVencimento = new Set(items.filter((item) => item.status_class === "a_vencer").map((item) => item.company_id)).size

  return {
    totalDebitos: items.length,
    totalVencido: items.filter((item) => item.status_class === "vencido").reduce((sum, item) => sum + Number(item.valor || 0), 0),
    totalAVencer: items.filter((item) => item.status_class === "a_vencer").reduce((sum, item) => sum + Number(item.valor || 0), 0),
    quantidadeDebitos: items.length,
    empresasComVencidos,
    empresasProximasVencimento,
    totalValor: items.reduce((sum, item) => sum + Number(item.valor || 0), 0),
  }
}

export async function getLatestMunicipalTaxRuns(limit = 20): Promise<MunicipalTaxCollectionRun[]> {
  const { data, error } = await supabase
    .from("municipal_tax_collection_runs")
    .select("*")
    .eq("robot_technical_id", "goiania_taxas_impostos")
    .order("created_at", { ascending: false })
    .limit(limit)
  if (error) throw error
  return (data ?? []) as MunicipalTaxCollectionRun[]
}
