import { supabase } from "./supabaseClient"
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
  let query = supabase
    .from("municipal_tax_debts")
    .select("*")
    .order("data_vencimento", { ascending: true })
    .order("tributo", { ascending: true })

  if (filters.companyIds?.length) {
    query = query.in("company_id", filters.companyIds)
  }

  const { data, error } = await query
  if (error) throw error

  const debts = (data ?? []) as MunicipalTaxDebt[]
  const companyIds = [...new Set(debts.map((item) => item.company_id))]
  const { data: companies, error: companiesError } = companyIds.length
    ? await supabase.from("companies").select("id, name, document").in("id", companyIds)
    : { data: [], error: null }
  if (companiesError) throw companiesError

  const companyMap = new Map((companies ?? []).map((company) => [company.id, company]))

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
