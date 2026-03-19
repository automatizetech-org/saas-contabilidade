import { supabase } from "./supabaseClient"
import {
  getCertidoesDocuments,
  getFiscalDocumentsByType,
  getFiscalDocumentsNfeNfc,
  type UnifiedDocumentRow,
} from "./dashboardService"

export type CursorPageToken = {
  sortDate: string
  createdAt: string
  id: string
}

export type UnifiedDocumentFilters = {
  companyIds?: string[] | null
  category?: string
  fileKind?: "Todos" | "XML" | "PDF"
  search?: string
  dateFrom?: string
  dateTo?: string
  cursor?: CursorPageToken | null
  limit: number
}

export type UnifiedDocumentPageResult = {
  items: UnifiedDocumentRow[]
  nextCursor: CursorPageToken | null
  hasMore: boolean
  refreshAt: string | null
}

export type FiscalDetailKind = "nfs" | "nfe" | "nfc" | "nfe-nfc" | "certidoes"

export type FiscalDetailPageFilters = {
  kind: FiscalDetailKind
  companyIds?: string[] | null
  search?: string
  dateFrom?: string
  dateTo?: string
  fileKind?: "all" | "xml" | "pdf"
  origem?: "all" | "recebidas" | "emitidas"
  modelo?: "all" | "55" | "65"
  certidaoTipo?: string
  cursor?: CursorPageToken | null
  limit: number
}

export type FiscalDetailSummary = {
  cards: {
    totalDocuments: number
    availableDocuments: number
    thisMonth: number
    nfeCount: number
    nfcCount: number
  }
  byMonth: Array<{ key: string; value: number }>
}

export type CertidoesOverview = {
  cards: {
    total: number
    negativas: number
    irregulares: number
  }
  chartData: Array<{ name: string; value: number }>
}

function normalizeText(value: unknown) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
}

function normalizeDigits(value: unknown) {
  return String(value ?? "").replace(/\D/g, "")
}

function mapUnifiedRpcRow(row: any): UnifiedDocumentRow {
  return {
    id: String(row.id),
    company_id: String(row.company_id),
    empresa: row.empresa ?? "",
    cnpj: row.cnpj ?? null,
    source: row.source,
    type: row.type ?? "",
    origem: row.origem ?? null,
    status: row.status ?? null,
    periodo: row.periodo ?? null,
    document_date: row.document_date ?? null,
    created_at: row.created_at ?? "",
    file_path: row.file_path ?? null,
    chave: row.chave ?? null,
  }
}

function parseCursorToken(input: unknown): CursorPageToken | null {
  if (!input || typeof input !== "object") return null
  const value = input as Record<string, unknown>
  const sortDate = String(value.sortDate ?? "").trim()
  const createdAt = String(value.createdAt ?? "").trim()
  const id = String(value.id ?? "").trim()
  if (!sortDate || !createdAt || !id) return null
  return { sortDate, createdAt, id }
}

function parseCursorPayload<T>(payload: any, mapRow: (row: any) => T) {
  const items = Array.isArray(payload?.items) ? payload.items.map((row) => mapRow(row)) : []
  return {
    items,
    nextCursor: parseCursorToken(payload?.nextCursor),
    hasMore: Boolean(payload?.hasMore),
    refreshAt: payload?.refreshAt ? String(payload.refreshAt) : null,
  }
}

export async function getUnifiedDocumentsPage(filters: UnifiedDocumentFilters): Promise<UnifiedDocumentPageResult> {
  const { data, error } = await supabase.rpc("get_document_rows_cursor", {
    company_ids: filters.companyIds?.length ? filters.companyIds : null,
    category_filter: filters.category && filters.category !== "Todos" ? filters.category : null,
    file_kind: filters.fileKind && filters.fileKind !== "Todos" ? filters.fileKind.toLowerCase() : null,
    search_text: filters.search ?? null,
    date_from: filters.dateFrom || null,
    date_to: filters.dateTo || null,
    cursor_sort_date: filters.cursor?.sortDate ?? null,
    cursor_created_at: filters.cursor?.createdAt ?? null,
    cursor_id: filters.cursor?.id ?? null,
    limit_count: filters.limit,
  })
  if (error) throw error

  return parseCursorPayload(data, mapUnifiedRpcRow)
}

export type ZipPathRow = { file_path: string; empresa: string; category_key: string }

/** Retorna apenas file_path, empresa e category_key de documentos que batem com os filtros (sem cursor). */
export async function getUnifiedDocumentsZipPaths(
  filters: Omit<UnifiedDocumentFilters, "cursor" | "limit">,
  limitCount = 50000
): Promise<ZipPathRow[]> {
  const { data, error } = await supabase.rpc("get_document_rows_zip_paths", {
    company_ids: filters.companyIds?.length ? filters.companyIds : null,
    category_filter: filters.category && filters.category !== "Todos" ? filters.category : null,
    file_kind: filters.fileKind && filters.fileKind !== "Todos" ? filters.fileKind.toLowerCase() : null,
    search_text: filters.search ?? null,
    date_from: filters.dateFrom || null,
    date_to: filters.dateTo || null,
    limit_count: limitCount,
  })
  if (error) throw error
  const arr = Array.isArray(data) ? data : (data != null && typeof data === "object" && "file_path" in (data as object) ? [data] : [])
  return (arr as any[]).map((row: any) => ({
    file_path: String(row?.file_path ?? "").trim(),
    empresa: String(row?.empresa ?? "").trim() || "EMPRESA",
    category_key: String(row?.category_key ?? "").trim() || "outros",
  })).filter((r) => r.file_path.length > 0)
}

type FiscalListRow = {
  id: string
  company_id: string
  empresa: string
  cnpj: string | null
  type: string
  chave: string | null
  periodo: string | null
  status: string | null
  document_date: string | null
  created_at: string
  file_path: string | null
  origem: "recebidas" | "emitidas" | null
  modelo: "55" | "65" | null
  tipo_certidao: string | null
}

function dedupeDocumentsByKey<T extends { chave?: string | null; id: string }>(documents: T[]) {
  const seen = new Set<string>()
  const result: T[] = []
  for (const document of documents) {
    const key = (document.chave || "").trim() || document.id
    if (seen.has(key)) continue
    seen.add(key)
    result.push(document)
  }
  return result
}

function getFiscalRowsReferenceDate(row: FiscalListRow) {
  return String(row.document_date ?? row.created_at ?? "").slice(0, 10)
}

function buildMonthSeries(rows: FiscalListRow[], dateFrom?: string, dateTo?: string) {
  const start = dateFrom ? new Date(`${dateFrom}T12:00:00`) : new Date(new Date().getFullYear(), new Date().getMonth() - 11, 1)
  const end = dateTo ? new Date(`${dateTo}T12:00:00`) : new Date()
  const months: Array<{ key: string; value: number }> = []
  const counters = new Map<string, number>()
  for (const row of rows) {
    const referenceDate = getFiscalRowsReferenceDate(row)
    if (!referenceDate) continue
    const monthKey = referenceDate.slice(0, 7)
    counters.set(monthKey, (counters.get(monthKey) ?? 0) + 1)
  }
  const cursor = new Date(start.getFullYear(), start.getMonth(), 1)
  const last = new Date(end.getFullYear(), end.getMonth(), 1)
  while (cursor <= last) {
    const key = `${cursor.getFullYear()}-${String(cursor.getMonth() + 1).padStart(2, "0")}`
    months.push({ key, value: counters.get(key) ?? 0 })
    cursor.setMonth(cursor.getMonth() + 1)
  }
  return months
}

export async function getCertidoesOverviewSummary(companyIds: string[] | null): Promise<CertidoesOverview> {
  try {
    const { data, error } = await supabase.rpc("get_certidoes_overview_summary", {
      company_ids: companyIds?.length ? companyIds : null,
    })
    if (error) throw error

    const payload = (data ?? {}) as CertidoesOverview
    return {
      cards: {
        total: Number(payload.cards?.total ?? 0),
        negativas: Number(payload.cards?.negativas ?? 0),
        irregulares: Number(payload.cards?.irregulares ?? 0),
      },
      chartData: (payload.chartData ?? []).map((item) => ({ name: item.name, value: Number(item.value ?? 0) })),
    }
  } catch {
    const certidoes = await getCertidoesDocuments(companyIds)
    const negativas = certidoes.filter((certidao) => {
      const status = String((certidao as { status?: string | null }).status || "").toLowerCase()
      return status === "regular" || status === "negativa"
    }).length
    const irregulares = certidoes.length - negativas
    return {
      cards: {
        total: certidoes.length,
        negativas,
        irregulares,
      },
      chartData: [
        { name: "Negativas", value: negativas },
        { name: "Irregulares", value: irregulares },
      ],
    }
  }
}

export async function getFiscalDetailSummary(filters: Omit<FiscalDetailPageFilters, "search" | "fileKind" | "origem" | "modelo" | "certidaoTipo" | "page" | "pageSize">): Promise<FiscalDetailSummary> {
  const detailKind = filters.kind === "nfe-nfc" ? "NFE_NFC" : filters.kind.toUpperCase()
  try {
    const { data, error } = await supabase.rpc("get_fiscal_detail_summary", {
      detail_kind: detailKind,
      company_ids: filters.companyIds?.length ? filters.companyIds : null,
      date_from: filters.dateFrom || null,
      date_to: filters.dateTo || null,
    })
    if (error) throw error

    const payload = (data ?? {}) as FiscalDetailSummary
    return {
      cards: {
        totalDocuments: Number(payload.cards?.totalDocuments ?? 0),
        availableDocuments: Number(payload.cards?.availableDocuments ?? 0),
        thisMonth: Number(payload.cards?.thisMonth ?? 0),
        nfeCount: Number(payload.cards?.nfeCount ?? 0),
        nfcCount: Number(payload.cards?.nfcCount ?? 0),
      },
      byMonth: (payload.byMonth ?? []).map((item) => ({
        key: item.key,
        value: Number(item.value ?? 0),
      })),
    }
  } catch {
    const rows = filters.kind === "nfe-nfc"
      ? await getFiscalDocumentsNfeNfc(filters.companyIds?.length ? filters.companyIds : null)
      : await getFiscalDocumentsByType(filters.kind.toUpperCase() as "NFS" | "NFE" | "NFC", filters.companyIds?.length ? filters.companyIds : null)
    const deduped = filters.kind === "nfe-nfc" || filters.kind === "nfs" ? dedupeDocumentsByKey(rows) : rows
    const filtered = rows.filter((row) => {
      const referenceDate = String(row.document_date ?? row.created_at ?? "").slice(0, 10)
      if (filters.dateFrom && referenceDate < filters.dateFrom) return false
      if (filters.dateTo && referenceDate > filters.dateTo) return false
      return true
    })
    const filteredDeduped = filters.kind === "nfe-nfc" || filters.kind === "nfs" ? dedupeDocumentsByKey(filtered) : filtered
    const currentMonth = new Date().toISOString().slice(0, 7)
    return {
      cards: {
        totalDocuments: filteredDeduped.length,
        availableDocuments: filtered.filter((row) => row.file_path).length,
        thisMonth: filteredDeduped.filter((row) => String(row.periodo ?? "").slice(0, 7) === currentMonth).length,
        nfeCount: filtered.filter((row) => row.type === "NFE").length,
        nfcCount: filtered.filter((row) => row.type === "NFC").length,
      },
      byMonth: buildMonthSeries(filtered.map((row) => ({
        ...row,
        origem: null,
        modelo: row.type === "NFE" ? "55" : row.type === "NFC" ? "65" : null,
        tipo_certidao: null,
      })), filters.dateFrom, filters.dateTo),
    }
  }
}

function mapFiscalDetailRpcRow(row: any): FiscalListRow {
  return {
    id: String(row.id),
    company_id: String(row.company_id),
    empresa: row.empresa ?? "",
    cnpj: row.cnpj ?? null,
    type: row.type ?? "",
    chave: row.chave ?? null,
    periodo: row.periodo ?? null,
    status: row.status ?? null,
    document_date: row.document_date ?? null,
    created_at: row.created_at ?? "",
    file_path: row.file_path ?? null,
    origem: (row.origem ?? null) as "recebidas" | "emitidas" | null,
    modelo: (row.modelo ?? null) as "55" | "65" | null,
    tipo_certidao: row.tipo_certidao ?? null,
  }
}

export async function getFiscalDetailDocumentsPage(filters: FiscalDetailPageFilters): Promise<{ items: FiscalListRow[]; nextCursor: CursorPageToken | null; hasMore: boolean; refreshAt: string | null }> {
  const detailKind = filters.kind === "nfe-nfc" ? "NFE_NFC" : filters.kind.toUpperCase()
  try {
    const { data, error } = await supabase.rpc("get_fiscal_detail_documents_cursor", {
      detail_kind: detailKind,
      company_ids: filters.companyIds?.length ? filters.companyIds : null,
      search_text: filters.search ?? null,
      date_from: filters.dateFrom || null,
      date_to: filters.dateTo || null,
      file_kind: filters.fileKind && filters.fileKind !== "all" ? filters.fileKind : null,
      origem_filter: filters.origem && filters.origem !== "all" ? filters.origem : null,
      modelo_filter: filters.modelo && filters.modelo !== "all" ? filters.modelo : null,
      certidao_tipo_filter: filters.certidaoTipo && filters.certidaoTipo !== "all" ? filters.certidaoTipo : null,
      cursor_sort_date: filters.cursor?.sortDate ?? null,
      cursor_created_at: filters.cursor?.createdAt ?? null,
      cursor_id: filters.cursor?.id ?? null,
      limit_count: filters.limit,
    })
    if (error) throw error

    return parseCursorPayload(data, mapFiscalDetailRpcRow)
  } catch {
    const baseRows = filters.kind === "certidoes"
      ? (await getCertidoesDocuments(filters.companyIds?.length ? filters.companyIds : null)).map((row) => ({
          id: row.id,
          company_id: row.company_id,
          empresa: row.empresa ?? "",
          cnpj: row.cnpj ?? null,
          type: `CERTIDÃO - ${String((row as { tipo_certidao?: string }).tipo_certidao ?? "OUTRA")}`,
          chave: null,
          periodo: (row as { periodo?: string | null }).periodo ?? null,
          status: (row as { status?: string | null }).status ?? null,
          document_date: (row as { document_date?: string | null }).document_date ?? null,
          created_at: row.created_at,
          file_path: (row as { file_path?: string | null }).file_path ?? null,
          origem: null,
          modelo: null,
          tipo_certidao: normalizeText((row as { tipo_certidao?: string }).tipo_certidao ?? ""),
        }))
      : (filters.kind === "nfe-nfc"
        ? await getFiscalDocumentsNfeNfc(filters.companyIds?.length ? filters.companyIds : null)
        : await getFiscalDocumentsByType(filters.kind.toUpperCase() as "NFS" | "NFE" | "NFC", filters.companyIds?.length ? filters.companyIds : null))
          .map((row) => ({
            id: row.id,
            company_id: row.company_id,
            empresa: row.empresa ?? "",
            cnpj: row.cnpj ?? null,
            type: row.type,
            chave: row.chave ?? null,
            periodo: row.periodo ?? null,
            status: row.status ?? null,
            document_date: row.document_date ?? null,
            created_at: row.created_at,
            file_path: row.file_path ?? null,
            origem: row.file_path?.includes("/Recebidas/") ? "recebidas" : row.file_path?.includes("/Emitidas/") ? "emitidas" : null,
            modelo: row.type === "NFE" ? "55" : row.type === "NFC" ? "65" : null,
            tipo_certidao: null,
          }))

    const filtered = baseRows.filter((row) => {
      const referenceDate = getFiscalRowsReferenceDate(row)
      if (filters.search) {
        const search = normalizeText(filters.search)
        const digitsSearch = normalizeDigits(filters.search)
        const matchesSearch =
          normalizeText(row.empresa).includes(search) ||
          normalizeText(row.type).includes(search) ||
          normalizeText(row.status).includes(search) ||
          normalizeText(row.chave).includes(search) ||
          normalizeText(row.tipo_certidao).includes(search) ||
          (digitsSearch.length > 0 && normalizeDigits(row.cnpj).includes(digitsSearch))
        if (!matchesSearch) return false
      }
      if (filters.dateFrom && referenceDate < filters.dateFrom) return false
      if (filters.dateTo && referenceDate > filters.dateTo) return false
      if (filters.fileKind && filters.fileKind !== "all") {
        const lower = String(row.file_path ?? "").toLowerCase()
        if (filters.fileKind === "xml" && !lower.endsWith(".xml")) return false
        if (filters.fileKind === "pdf" && !lower.endsWith(".pdf")) return false
      }
      if (filters.origem && filters.origem !== "all" && row.origem !== filters.origem) return false
      if (filters.modelo && filters.modelo !== "all" && row.modelo !== filters.modelo) return false
      if (filters.certidaoTipo && filters.certidaoTipo !== "all" && row.tipo_certidao !== normalizeText(filters.certidaoTipo)) return false
      return true
    })

    const ordered = [...filtered].sort((left, right) => String(right.document_date ?? right.created_at).localeCompare(String(left.document_date ?? left.created_at)))
    return {
      items: ordered.slice(0, filters.limit),
      nextCursor: ordered.length > filters.limit ? {
        sortDate: String(ordered[filters.limit - 1]?.document_date ?? ordered[filters.limit - 1]?.created_at ?? "").slice(0, 10),
        createdAt: String(ordered[filters.limit - 1]?.created_at ?? ""),
        id: String(ordered[filters.limit - 1]?.id ?? ""),
      } : null,
      hasMore: ordered.length > filters.limit,
      refreshAt: null,
    }
  }
}

const FISCAL_ZIP_PAGE_LIMIT = 500
const FISCAL_ZIP_MAX_ITEMS = 50000

/** Coleta todos os IDs (com file_path) da lista fiscal conforme os filtros, paginando em lotes. Para ZIP da lista inteira. */
export async function getFiscalDetailDocumentIdsForZip(
  filters: Omit<FiscalDetailPageFilters, "cursor" | "limit">
): Promise<string[]> {
  // Deduplica por `file_path` (evita múltiplas cópias físicas no ZIP)
  const filePathToId = new Map<string, string>()
  let cursor: CursorPageToken | null = null
  do {
    const result = await getFiscalDetailDocumentsPage({
      ...filters,
      cursor,
      limit: FISCAL_ZIP_PAGE_LIMIT,
    })
    for (const row of result.items) {
      const fp = String(row.file_path ?? "").trim()
      if (!fp) continue
      if (!filePathToId.has(fp)) filePathToId.set(fp, row.id)
      if (filePathToId.size >= FISCAL_ZIP_MAX_ITEMS) break
    }
    if (filePathToId.size >= FISCAL_ZIP_MAX_ITEMS) break
    if (!result.hasMore || !result.nextCursor) break
    cursor = result.nextCursor
  } while (true)
  return Array.from(filePathToId.values())
}

export type FiscalZipPathRow = {
  file_path: string
  empresa: string
}

/** Coleta todos os `file_path` (e `empresa`) do cursor fiscal conforme os filtros, para ZIP da lista inteira. */
export async function getFiscalDetailDocumentPathsForZip(
  filters: Omit<FiscalDetailPageFilters, "cursor" | "limit">
): Promise<FiscalZipPathRow[]> {
  const filePathToEmpresa = new Map<string, string>()
  let cursor: CursorPageToken | null = null
  do {
    const result = await getFiscalDetailDocumentsPage({
      ...filters,
      cursor,
      limit: FISCAL_ZIP_PAGE_LIMIT,
    })
    for (const row of result.items) {
      const fp = String(row.file_path ?? "").trim()
      if (!fp) continue
      if (!filePathToEmpresa.has(fp)) filePathToEmpresa.set(fp, row.empresa || "EMPRESA")
      if (filePathToEmpresa.size >= FISCAL_ZIP_MAX_ITEMS) break
    }
    if (filePathToEmpresa.size >= FISCAL_ZIP_MAX_ITEMS) break
    if (!result.hasMore || !result.nextCursor) break
    cursor = result.nextCursor
  } while (true)

  return Array.from(filePathToEmpresa.entries()).map(([file_path, empresa]) => ({ file_path, empresa }))
}
