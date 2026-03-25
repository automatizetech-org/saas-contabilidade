import { supabase } from "./supabaseClient"
import { fetchAllPages } from "./supabasePagination"

const preferDashboardFallback = Boolean(import.meta.env.DEV)
let canUseFiscalOverviewAnalyticsRpc = !preferDashboardFallback
let canUseDashboardOverviewRpc = !preferDashboardFallback
let canUseNfsStatsRangeSummaryRpc = !preferDashboardFallback

function buildMonthKeys(monthsBack: number) {
  const now = new Date()
  return Array.from({ length: monthsBack }).map((_, index) => {
    const date = new Date(now.getFullYear(), now.getMonth() - (monthsBack - index - 1), 1)
    return {
      key: `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`,
      label: date.toLocaleDateString("pt-BR", { month: "short", year: "2-digit" }),
    }
  })
}

function resolveDocumentReferenceDate(document: { document_date?: string | null; created_at?: string | null; periodo?: string | null }) {
  const documentDate = String(document.document_date || "").slice(0, 10)
  if (documentDate) return documentDate

  const createdAt = String(document.created_at || "").slice(0, 10)
  if (createdAt) return createdAt

  const period = String(document.periodo || "").trim()
  if (/^\d{4}-\d{2}$/.test(period)) return `${period}-01`

  return ""
}

function isWithinDateRange(value: string, dateFrom?: string, dateTo?: string) {
  if (!value) return false
  if (dateFrom && value < dateFrom) return false
  if (dateTo && value > dateTo) return false
  return true
}

function normalizeRelativeFilePath(value: unknown) {
  const raw = String(value ?? "").trim()
  if (!raw) return null
  return raw.replace(/\\/g, "/")
}

function sanitizeCompanyFolderName(value: string) {
  const normalized = String(value || "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
  return normalized.replace(/[^A-Za-z0-9 _.-]/g, "").trim() || "EMPRESA"
}

function buildCertidaoFallbackFilePath(
  companyName: string,
  tipoCertidao: string,
  payload: Record<string, unknown>,
) {
  const normalizedCompany = String(companyName || "").trim()
  const normalizedTipo = String(tipoCertidao || "").trim()
  const documentDate = String(payload.document_date || payload.data_consulta || "").slice(0, 10)
  if (!normalizedCompany || !normalizedTipo || !/^\d{4}-\d{2}-\d{2}$/.test(documentDate)) {
    return null
  }
  const [year, month, day] = documentDate.split("-")
  const safeCompany = sanitizeCompanyFolderName(normalizedCompany)
  return `${safeCompany}/FISCAL/CERTIDOES/${year}/${month}/${day}/${normalizedTipo}.pdf`
}

function resolveCertidaoFilePath(
  companyName: string,
  tipoCertidao: string,
  payload: Record<string, unknown>,
) {
  const directKeys = ["arquivo_pdf", "file_path", "pdf_path", "path", "relative_path"]
  for (const key of directKeys) {
    const normalized = normalizeRelativeFilePath(payload[key])
    if (normalized) return normalized
  }
  return buildCertidaoFallbackFilePath(companyName, tipoCertidao, payload)
}

function buildMonthsBetween(dateFrom: string, dateTo: string) {
  const from = new Date(`${dateFrom}T12:00:00`)
  const to = new Date(`${dateTo}T12:00:00`)
  const months: Array<{ key: string; label: string }> = []
  const cursor = new Date(from.getFullYear(), from.getMonth(), 1)
  const end = new Date(to.getFullYear(), to.getMonth(), 1)
  while (cursor <= end) {
    months.push({
      key: `${cursor.getFullYear()}-${String(cursor.getMonth() + 1).padStart(2, "0")}`,
      label: cursor.toLocaleDateString("pt-BR", { month: "short", year: "2-digit" }),
    })
    cursor.setMonth(cursor.getMonth() + 1)
  }
  return months
}

type ServiceCodeStat = { code: string; description: string; total_value: number }

type CompanyLookup = { id: string; name: string; document?: string | null }

async function fetchCompaniesByIds(companyIds: string[], withDocument = false): Promise<CompanyLookup[]> {
  const normalizedIds = [...new Set(companyIds.map((id) => String(id || "").trim()).filter(Boolean))]
  if (normalizedIds.length === 0) return []

  const select = withDocument ? "id, name, document" : "id, name"
  return fetchAllPages<CompanyLookup>((from, to) =>
    supabase
      .from("companies")
      .select(select)
      .in("id", normalizedIds)
      .order("name")
      .range(from, to)
  )
}

function normalizeCompanyIds(companyIds: string[] | null) {
  if (!companyIds?.length) return []
  return [...new Set(
    companyIds
      .map((id) => String(id || "").trim().toLowerCase())
      .filter(Boolean)
  )]
}

function normalizeCertidaoStatus(status: unknown) {
  const normalized = String(status ?? "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
  if (
    normalized === "empregador nao cadastrado" ||
    normalized === "regular" ||
    normalized === "positiva com efeito de negativa" ||
    normalized === "positiva com efeitos de negativa"
  ) {
    return "negativa"
  }
  if (normalized === "positiva") return "irregular"
  return String(status ?? "").trim() || null
}

function parseMoneyLike(value: unknown) {
  if (typeof value === "number") return Number.isFinite(value) ? value : 0
  if (typeof value !== "string") return 0
  const raw = value.trim()
  if (!raw) return 0
  const normalized = raw.includes(",") && raw.includes(".")
    ? raw.replace(/\./g, "").replace(",", ".")
    : raw.includes(",")
      ? raw.replace(",", ".")
      : raw
  const parsed = Number(normalized)
  return Number.isFinite(parsed) ? parsed : 0
}

function normalizeServiceCodes(input: unknown): ServiceCodeStat[] {
  if (!input) return []

  let list: unknown[] = []
  if (Array.isArray(input)) {
    list = input
  } else if (typeof input === "string") {
    try {
      return normalizeServiceCodes(JSON.parse(input))
    } catch {
      return []
    }
  } else if (typeof input === "object") {
    const record = input as Record<string, unknown>
    if (Array.isArray(record.service_codes)) {
      list = record.service_codes
    } else {
      list = Object.values(record)
    }
  }

  return list
    .map((item) => {
      if (!item || typeof item !== "object") return null
      const row = item as Record<string, unknown>
      const code = String(
        row.code ??
        row.codigo ??
        row.service_code ??
        row.ctribnac ??
        row.cTribNac ??
        ""
      ).trim()
      const description = String(
        row.description ??
        row.descricao ??
        row.label ??
        row.name ??
        row.xTribNac ??
        row.xtribnac ??
        ""
      ).trim()
      const totalValue = parseMoneyLike(
        row.total_value ??
        row.totalValue ??
        row.valor_total ??
        row.valor ??
        row.value ??
        row.amount ??
        row.total
      )
      if (!code && !description) return null
      return { code, description, total_value: totalValue }
    })
    .filter((item): item is ServiceCodeStat => Boolean(item))
}

function mergeServiceCodeMaps(
  target: Map<string, ServiceCodeStat>,
  codes: ServiceCodeStat[]
) {
  for (const code of codes) {
    const key = `${code.code}|${code.description}`
    const existing = target.get(key)
    if (existing) {
      existing.total_value += code.total_value
    } else {
      target.set(key, { ...code })
    }
  }
}

function mapToSortedServiceCodes(index: Map<string, ServiceCodeStat>) {
  return [...index.values()]
    .filter((x) => x.code || x.description)
    .sort((a, b) => b.total_value - a.total_value)
}

function sumServiceCodes(codes: ServiceCodeStat[]) {
  return codes.reduce((sum, item) => sum + parseMoneyLike(item.total_value), 0)
}

function buildMonthRangeKeys(dateFrom: string, dateTo: string) {
  const from = dateFrom.slice(0, 7)
  const to = dateTo.slice(0, 7)
  if (from > to) return buildMonthRangeKeys(dateTo, dateFrom)
  const months: string[] = []
  const yFrom = parseInt(from.slice(0, 4), 10)
  const mFrom = parseInt(from.slice(5, 7), 10)
  const yTo = parseInt(to.slice(0, 4), 10)
  const mTo = parseInt(to.slice(5, 7), 10)
  for (let y = yFrom; y <= yTo; y++) {
    const mStart = y === yFrom ? mFrom : 1
    const mEnd = y === yTo ? mTo : 12
    for (let m = mStart; m <= mEnd; m++) {
      months.push(`${y}-${String(m).padStart(2, "0")}`)
    }
  }
  if (months.length === 0) {
    const now = new Date()
    months.push(`${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`)
  }
  return months
}

async function getActiveNfsCompanyPeriods(companyIds: string[] | null, months: string[]) {
  const normalizedCompanyIds = normalizeCompanyIds(companyIds)
  let q = supabase
    .from("fiscal_documents")
    .select("company_id, periodo, file_path")
    .eq("type", "NFS")
    .not("file_path", "is", null)
  if (months.length === 1) {
    q = q.eq("periodo", months[0])
  } else if (months.length > 1) {
    q = q.in("periodo", months)
  }
  if (normalizedCompanyIds.length === 1) {
    q = q.eq("company_id", normalizedCompanyIds[0])
  } else if (normalizedCompanyIds.length > 1) {
    q = q.in("company_id", normalizedCompanyIds)
  }
  const rows = await fetchAllPages<{
    company_id: string
    periodo?: string | null
    file_path?: string | null
  }>((from, to) => q.range(from, to))
  return new Set(
    rows
      .filter((row) => Boolean(String(row.file_path ?? "").trim()))
      .map((row) => {
        const companyId = String(row.company_id || "").trim().toLowerCase()
        const period = String(row.periodo || "").trim().slice(0, 7)
        return companyId && /^\d{4}-\d{2}$/.test(period) ? `${companyId}:${period}` : ""
      })
      .filter(Boolean),
  )
}

function getEmptyNfsStats(period: string) {
  return {
    period,
    totalQty: 0,
    valorEmitidas: 0,
    valorRecebidas: 0,
    previousValorEmitidas: 0,
    previousValorRecebidas: 0,
    serviceCodesRanking: [] as ServiceCodeStat[],
    serviceCodesRankingPrestadas: [] as ServiceCodeStat[],
    serviceCodesRankingTomadas: [] as ServiceCodeStat[],
  }
}

/** Contagens reais (notas únicas por chave/id). Usa a mesma RPC do dashboard quando possível. */
export async function getDashboardCounts(companyIds: string[] | null) {
  try {
    const { data, error } = await supabase.rpc("get_dashboard_overview_summary", {
      company_ids: companyIds && companyIds.length > 0 ? companyIds : null,
    })
    if (error) throw error
    const payload = (data ?? {}) as { companiesCount?: number; totalNotasFiscais?: number; documentsCount?: number }
    return {
      companiesCount: Number(payload.companiesCount ?? 0),
      documentsCount: Number(payload.totalNotasFiscais ?? payload.documentsCount ?? 0),
    }
  } catch {
    const filterByCompany = companyIds && companyIds.length > 0
    const companyFilter = filterByCompany ? companyIds : undefined
    const [companiesRes, docsRes] = await Promise.all([
      supabase.from("companies").select("id", { count: "exact", head: true }),
      companyFilter
        ? supabase.from("fiscal_documents").select("id, chave").in("company_id", companyFilter).limit(10000)
        : supabase.from("fiscal_documents").select("id, chave").limit(10000),
    ])
    if (companiesRes.error) throw companiesRes.error
    const docsResTyped = docsRes as { data?: Array<{ id: string; chave?: string | null }>; error?: Error }
    const docs = docsResTyped.error ? [] : (docsResTyped.data ?? [])
    const seen = new Set<string>()
    const uniq = docs.filter((d) => {
      const key = (d.chave && String(d.chave).trim()) ? String(d.chave).trim() : d.id
      if (seen.has(key)) return false
      seen.add(key)
      return true
    })
    return {
      companiesCount: companiesRes.count ?? 0,
      documentsCount: uniq.length,
    }
  }
}

/** Lista documentos fiscais recentes (um por chave/id único), ordenados por created_at. */
export async function getRecentFiscalDocuments(companyIds: string[] | null, limit: number) {
  let q = supabase
    .from("fiscal_documents")
    .select("id, chave, company_id, type, status, created_at")
    .order("created_at", { ascending: false })
    .limit(Math.min(limit * 3, 500))
  if (companyIds && companyIds.length > 0) {
    q = q.in("company_id", companyIds)
  }
  const { data, error } = await q
  if (error) throw error
  const raw = (data ?? []) as Array<{ id: string; chave?: string | null; company_id: string; type: string; status: string; created_at: string }>
  const seen = new Set<string>()
  const list = raw.filter((d) => {
    const key = (d.chave && String(d.chave).trim()) ? String(d.chave).trim() : d.id
    if (seen.has(key)) return false
    seen.add(key)
    return true
  }).slice(0, limit)
  const companyIdsList = [...new Set(list.map((d) => d.company_id))]
  if (companyIdsList.length === 0) return []
  const companies = await fetchCompaniesByIds(companyIdsList)
  const names = new Map(companies.map((c) => [c.id, c.name]))
  return list.map((d) => ({
    id: d.id,
    company_id: d.company_id,
    type: d.type,
    status: d.status,
    created_at: d.created_at,
    companyName: names.get(d.company_id) ?? "",
  }))
}

export async function getFiscalDocumentsByType(
  type: "NFS" | "NFE" | "NFC",
  companyIds: string[] | null
) {
  // file_path omitido do select para não quebrar em projetos sem a coluna.
  // Para habilitar download por caminho, rode a migration 00000002_fiscal_documents_file_path.sql
  // e descomente file_path no select e use: file_path: d.file_path ?? null no return.
  let q = supabase
    .from("fiscal_documents")
    .select("id, company_id, type, chave, periodo, status, document_date, file_path, created_at")
    .eq("type", type)
    .order("document_date", { ascending: false })
    .order("created_at", { ascending: false })
  if (companyIds && companyIds.length > 0) {
    q = q.in("company_id", companyIds)
  }
  const list = await fetchAllPages<{
    id: string
    company_id: string
    type: string
    chave: string | null
    periodo: string
    status: string
    document_date: string | null
    file_path?: string | null
    created_at: string
  }>((from, to) => q.range(from, to))
  const companyIdsList = [...new Set(list.map((d) => d.company_id))]
  if (companyIdsList.length === 0) return []
  const companies = await fetchCompaniesByIds(companyIdsList, true)
  const names = new Map(companies.map((c) => [c.id, c.name]))
  const documents = new Map(companies.map((c) => [c.id, c.document ?? null]))
  return list.map((d) => ({
    ...d,
    empresa: names.get(d.company_id) ?? "",
    cnpj: documents.get(d.company_id) ?? "",
    file_path: (d as { file_path?: string | null }).file_path ?? null,
  }))
}

/** Lista documentos NFE e NFC juntos (para o tópico unificado NFE/NFC). */
export async function getFiscalDocumentsNfeNfc(companyIds: string[] | null) {
  let q = supabase
    .from("fiscal_documents")
    .select("id, company_id, type, chave, periodo, status, document_date, file_path, created_at")
    .in("type", ["NFE", "NFC"])
    .order("document_date", { ascending: false })
    .order("created_at", { ascending: false })
  if (companyIds && companyIds.length > 0) {
    q = q.in("company_id", companyIds)
  }
  const list = await fetchAllPages<{
    id: string
    company_id: string
    type: string
    chave: string | null
    periodo: string
    status: string
    document_date: string | null
    file_path?: string | null
    created_at: string
  }>((from, to) => q.range(from, to))
  const companyIdsList = [...new Set(list.map((d) => d.company_id))]
  if (companyIdsList.length === 0) return []
  const companies = await fetchCompaniesByIds(companyIdsList, true)
  const names = new Map(companies.map((c) => [c.id, c.name]))
  const documents = new Map(companies.map((c) => [c.id, c.document ?? null]))
  return list.map((d) => ({
    ...d,
    empresa: names.get(d.company_id) ?? "",
    cnpj: documents.get(d.company_id) ?? "",
    file_path: (d as { file_path?: string | null }).file_path ?? null,
  }))
}

export async function getCertidoesDocuments(companyIds: string[] | null) {
  let q = supabase
    .from("sync_events")
    .select("id, company_id, tipo, payload, created_at")
    .eq("tipo", "certidao_resultado")
    .order("created_at", { ascending: false })
  if (companyIds && companyIds.length > 0) {
    q = q.in("company_id", companyIds)
  }
  const rows = await fetchAllPages<{
    id: string
    company_id: string
    tipo: string
    payload?: string | null
    created_at: string
  }>((from, to) => q.range(from, to))
  const list = rows.map((row) => {
    let payload: Record<string, unknown> = {}
    try {
      payload = JSON.parse((row as { payload?: string | null }).payload || "{}")
    } catch {
      payload = {}
    }
    return {
      id: row.id,
      company_id: row.company_id,
      created_at: row.created_at,
      tipo: row.tipo,
      payload,
    }
  })
  const companyIdsList = [...new Set(list.map((d) => d.company_id).filter(Boolean))]
  if (companyIdsList.length === 0) return []
  const companies = await fetchCompaniesByIds(companyIdsList, true)
  const names = new Map(companies.map((c) => [c.id, c.name]))
  const documents = new Map(companies.map((c) => [c.id, c.document ?? null]))
  const tipoLabel: Record<string, string> = {
    federal: "Federal",
    estadual_go: "Estadual (GO)",
    fgts: "FGTS",
  }
  const latestByCompanyAndType = new Map<string, {
    id: string
    company_id: string
    periodo: string | null
    status: string | null
    document_date: string | null
    tipo_certidao: string
    file_path: string | null
    created_at: string
  }>()
  for (const d of list) {
    const payload = d.payload || {}
    const tipoCertidao = String(payload.tipo_certidao || "").trim()
    if (!tipoCertidao) continue
    const companyName = names.get(d.company_id) ?? ""
    const key = `${d.company_id}:${tipoCertidao}`
    const current = latestByCompanyAndType.get(key)
    const candidate = {
      id: d.id,
      company_id: String(d.company_id || ""),
      periodo: String(payload.periodo || "") || null,
      status: normalizeCertidaoStatus(payload.status),
      document_date: String(payload.document_date || payload.data_consulta || "").slice(0, 10) || null,
      tipo_certidao: tipoCertidao,
      file_path: resolveCertidaoFilePath(companyName, tipoCertidao, payload),
      created_at: String(d.created_at || ""),
    }
    if (!current || candidate.created_at > current.created_at) {
      latestByCompanyAndType.set(key, candidate)
    }
  }
  return [...latestByCompanyAndType.values()]
    .map((d) => {
      return {
        ...d,
        empresa: names.get(d.company_id) ?? "",
        cnpj: documents.get(d.company_id) ?? "",
        tipo_certidao: tipoLabel[d.tipo_certidao] ?? d.tipo_certidao,
      }
    })
}

/** Resumo fiscal para a visão geral: totais por tipo (NFS, NFE, NFC) com métricas reais (um documento = chave/id único). Opcional: period YYYY-MM para filtrar por período. */
export async function getFiscalSummary(companyIds: string[] | null, period?: string) {
  let q = supabase.from("fiscal_documents").select("id, chave, type, file_path, created_at, periodo")
  if (companyIds && companyIds.length > 0) {
    q = q.in("company_id", companyIds)
  }
  const rows = await fetchAllPages<{
    id: string
    chave?: string | null
    type: string
    file_path: string | null
    created_at: string
    periodo: string
  }>((from, to) => q.range(from, to))
  const docKey = (r: { id: string; chave?: string | null }) =>
    (r.chave && String(r.chave).trim() ? String(r.chave).trim() : r.id) as string
  const seen = new Set<string>()
  const rowsUniq = rows.filter((r) => {
    const key = docKey(r)
    if (seen.has(key)) return false
    seen.add(key)
    return true
  })
  const periodFilter = period && /^\d{4}-\d{2}$/.test(period) ? period : null
  const rowsFiltered = periodFilter
    ? rowsUniq.filter((r) => (r.periodo || "").trim() === periodFilter)
    : rowsUniq
  const now = new Date()
  const mesAtual = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`
  type ByTypeMetric = { total: number; disponiveis: number; esteMes: number }
  const byType: Record<string, ByTypeMetric> = {
    NFS: { total: 0, disponiveis: 0, esteMes: 0 },
    NFE: { total: 0, disponiveis: 0, esteMes: 0 },
    NFC: { total: 0, disponiveis: 0, esteMes: 0 },
  }
  for (const r of rowsFiltered) {
    const t = (r.type || "NFS").toUpperCase() as "NFS" | "NFE" | "NFC"
    if (!byType[t]) byType[t] = { total: 0, disponiveis: 0, esteMes: 0 }
    byType[t].total++
    if (r.file_path && String(r.file_path).trim()) byType[t].disponiveis++
    const p = (r.periodo || "").trim()
    if (/^\d{4}-\d{2}$/.test(p) && p === mesAtual) byType[t].esteMes++
  }
  const totalXmls = rowsFiltered.length
  const totalDisponiveis = rowsFiltered.filter((r) => r.file_path && String(r.file_path).trim()).length
  const totalEsteMes = rowsFiltered.filter((r) => {
    const p = (r.periodo || "").trim()
    return /^\d{4}-\d{2}$/.test(p) && p === mesAtual
  }).length
  return {
    byType,
    totalXmls,
    totalDisponiveis,
    totalEsteMes,
  }
}

/** Resumo NFS: totais e ranking de códigos de serviço (nfs_stats, preenchido pelo robô). period = YYYY-MM. */
export async function getNfsStats(companyIds: string[] | null, period?: string) {
  const periodFilter = period && /^\d{4}-\d{2}$/.test(period) ? period : null
  const normalizedCompanyIds = normalizeCompanyIds(companyIds)
  const now = new Date()
  const defPeriod = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`
  const p = periodFilter || defPeriod
  const activePairs = await getActiveNfsCompanyPeriods(companyIds, [p])
  if (activePairs.size === 0) return getEmptyNfsStats(p)
  let q = supabase
    .from("nfs_stats")
    .select("company_id, qty_emitidas, qty_recebidas, valor_emitidas, valor_recebidas, service_codes")
    .eq("period", p)
  if (normalizedCompanyIds.length === 1) {
    q = q.eq("company_id", normalizedCompanyIds[0])
  } else if (normalizedCompanyIds.length > 1) {
    q = q.in("company_id", normalizedCompanyIds)
  }
  const data = await fetchAllPages<{
    company_id: string
    qty_emitidas: number
    qty_recebidas: number
    valor_emitidas: number
    valor_recebidas: number
    service_codes: unknown
  }>((from, to) => q.range(from, to))
  const rows = data.filter((row) => {
    const companyId = String(row.company_id || "").trim().toLowerCase()
    const matchesCompany = normalizedCompanyIds.length === 0 || normalizedCompanyIds.includes(companyId)
    return matchesCompany && activePairs.has(`${companyId}:${p}`)
  })
  let totalQty = 0
  let valorEmitidas = 0
  let valorRecebidas = 0
  const codeIndex = new Map<string, ServiceCodeStat>()
  for (const r of rows) {
    totalQty += parseMoneyLike(r.qty_emitidas) + parseMoneyLike(r.qty_recebidas)
    const codes = normalizeServiceCodes(r.service_codes)
    valorEmitidas += parseMoneyLike(r.valor_emitidas)
    valorRecebidas += parseMoneyLike(r.valor_recebidas)
    mergeServiceCodeMaps(codeIndex, codes)
    const rankingTotal = sumServiceCodes(codes)
    if (parseMoneyLike(r.valor_emitidas) === 0 && parseMoneyLike(r.valor_recebidas) === 0 && rankingTotal > 0) {
      valorEmitidas += rankingTotal
    }
  }
  const serviceCodesRanking = mapToSortedServiceCodes(codeIndex)
  return {
    period: p,
    totalQty,
    valorEmitidas,
    valorRecebidas,
    serviceCodesRanking,
    serviceCodesRankingPrestadas: serviceCodesRanking,
    serviceCodesRankingTomadas: [],
    previousValorEmitidas: 0,
    previousValorRecebidas: 0,
  }
}

/** Agrega nfs_stats para todos os meses entre dateFrom e dateTo (YYYY-MM-DD). */
export async function getNfsStatsByDateRange(companyIds: string[] | null, dateFrom: string, dateTo: string) {
  const months = buildMonthRangeKeys(dateFrom, dateTo)
  const activePairs = await getActiveNfsCompanyPeriods(companyIds, months)
  if (activePairs.size === 0) {
    return getEmptyNfsStats(months.length === 1 ? months[0] : `${months[0]} a ${months[months.length - 1]}`)
  }
  try {
    if (!canUseNfsStatsRangeSummaryRpc) throw new Error("NFS stats range RPC disabled for this session")
    const { data, error } = await supabase.rpc("get_nfs_stats_range_summary", {
      company_ids: companyIds && companyIds.length > 0 ? companyIds : null,
      date_from: dateFrom,
      date_to: dateTo,
    })
    if (error) throw error

    const payload = (data ?? {}) as {
      period?: string
      totalQty?: number
      valorEmitidas?: number
      valorRecebidas?: number
      previousValorEmitidas?: number
      previousValorRecebidas?: number
      serviceCodesRankingPrestadas?: ServiceCodeStat[]
      serviceCodesRankingTomadas?: ServiceCodeStat[]
    }
    if (Number(payload.totalQty ?? 0) === 0 && activePairs.size > 0) {
      throw new Error("NFS stats range RPC returned empty payload")
    }

    return {
      period: payload.period ?? "",
      totalQty: Number(payload.totalQty ?? 0),
      valorEmitidas: Number(payload.valorEmitidas ?? 0),
      valorRecebidas: Number(payload.valorRecebidas ?? 0),
      previousValorEmitidas: Number(payload.previousValorEmitidas ?? 0),
      previousValorRecebidas: Number(payload.previousValorRecebidas ?? 0),
      serviceCodesRankingPrestadas: (payload.serviceCodesRankingPrestadas ?? []).map((item) => ({
        code: item.code ?? "",
        description: item.description ?? "",
        total_value: Number(item.total_value ?? 0),
      })),
      serviceCodesRankingTomadas: (payload.serviceCodesRankingTomadas ?? []).map((item) => ({
        code: item.code ?? "",
        description: item.description ?? "",
        total_value: Number(item.total_value ?? 0),
      })),
      serviceCodesRanking: [],
    }
  } catch {
    // Fallback local enquanto a migration ainda não foi aplicada.
  }

  const normalizedCompanyIds = normalizeCompanyIds(companyIds)
  let q = supabase
    .from("nfs_stats")
    .select("company_id, period, qty_emitidas, qty_recebidas, valor_emitidas, valor_recebidas, service_codes, service_codes_emitidas, service_codes_recebidas")
    .in("period", months)
  if (normalizedCompanyIds.length === 1) {
    q = q.eq("company_id", normalizedCompanyIds[0])
  } else if (normalizedCompanyIds.length > 1) {
    q = q.in("company_id", normalizedCompanyIds)
  }
  const data = await fetchAllPages<{
    company_id: string
    period: string
    qty_emitidas: number
    qty_recebidas: number
    valor_emitidas: number
    valor_recebidas: number
    service_codes: unknown
    service_codes_emitidas: unknown
    service_codes_recebidas: unknown
  }>((from, to) => q.range(from, to))
  const rows = data.filter((row) => {
    const companyId = String(row.company_id || "").trim().toLowerCase()
    const period = String(row.period || "").trim().slice(0, 7)
    const matchesCompany = normalizedCompanyIds.length === 0 || normalizedCompanyIds.includes(companyId)
    return matchesCompany && activePairs.has(`${companyId}:${period}`)
  })
  let totalQty = 0
  let valorEmitidas = 0
  let valorRecebidas = 0
  const codeIndex = new Map<string, ServiceCodeStat>()
  const codeIndexEmitidas = new Map<string, ServiceCodeStat>()
  const codeIndexRecebidas = new Map<string, ServiceCodeStat>()
  for (const r of rows) {
    totalQty += parseMoneyLike(r.qty_emitidas) + parseMoneyLike(r.qty_recebidas)
    const codes = normalizeServiceCodes(r.service_codes)
    const codesEmitidas = normalizeServiceCodes(r.service_codes_emitidas)
    const codesRecebidas = normalizeServiceCodes(r.service_codes_recebidas)
    const valorEmitidasRaw = parseMoneyLike(r.valor_emitidas)
    const valorRecebidasRaw = parseMoneyLike(r.valor_recebidas)
    valorEmitidas += valorEmitidasRaw > 0 ? valorEmitidasRaw : sumServiceCodes(codesEmitidas)
    valorRecebidas += valorRecebidasRaw > 0 ? valorRecebidasRaw : sumServiceCodes(codesRecebidas)
    mergeServiceCodeMaps(codeIndex, codes)
    mergeServiceCodeMaps(codeIndexEmitidas, codesEmitidas)
    mergeServiceCodeMaps(codeIndexRecebidas, codesRecebidas)
  }
  const serviceCodesRanking = mapToSortedServiceCodes(codeIndex)
  const serviceCodesRankingPrestadas = mapToSortedServiceCodes(codeIndexEmitidas)
  const serviceCodesRankingTomadas = mapToSortedServiceCodes(codeIndexRecebidas)
  return {
    period: months.length === 1 ? months[0] : `${months[0]} a ${months[months.length - 1]}`,
    totalQty,
    valorEmitidas,
    valorRecebidas,
    serviceCodesRanking,
    serviceCodesRankingPrestadas,
    serviceCodesRankingTomadas,
  }
}

/** Lista todos os documentos (XML/PDF) para a página Documentos. */
export async function getAllFiscalDocuments(companyIds: string[] | null) {
  let q = supabase
    .from("fiscal_documents")
    .select("id, company_id, type, chave, periodo, status, document_date, file_path, created_at")
    .order("created_at", { ascending: false })
  if (companyIds && companyIds.length > 0) {
    q = q.in("company_id", companyIds)
  }
  const list = await fetchAllPages<{
    id: string
    company_id: string
    type: string
    chave: string | null
    periodo: string
    status: string
    document_date: string | null
    file_path?: string | null
    created_at: string
  }>((from, to) => q.range(from, to))
  const companyIdsList = [...new Set(list.map((d) => d.company_id))]
  if (companyIdsList.length === 0) return []
  const companies = await fetchCompaniesByIds(companyIdsList, true)
  const names = new Map(companies.map((c) => [c.id, c.name]))
  const documents = new Map(companies.map((c) => [c.id, c.document ?? null]))
  return list.map((d) => ({
    ...d,
    empresa: names.get(d.company_id) ?? "",
    cnpj: documents.get(d.company_id) ?? "",
    file_path: (d as { file_path?: string | null }).file_path ?? null,
  }))
}

export type UnifiedDocumentRow = {
  id: string
  company_id: string
  empresa: string
  cnpj: string | null
  source: "fiscal" | "certidoes" | "dp_guias" | "municipal_taxes"
  type: string
  origem: "recebidas" | "emitidas" | null
  status: string | null
  periodo: string | null
  document_date: string | null
  created_at: string
  file_path: string | null
  chave?: string | null
}

/** Lista unificada de TODOS os documentos do hub (XML/PDF): notas, certidões, guias/taxas e impostos. */
export async function getAllHubDocuments(companyIds: string[] | null): Promise<UnifiedDocumentRow[]> {
  const companyFilter = companyIds && companyIds.length > 0 ? companyIds : null

  const [fiscalDocs, certidoes, dpGuiasRes, municipalRes] = await Promise.all([
    getAllFiscalDocuments(companyFilter),
    getCertidoesDocuments(companyFilter),
    (async () => {
      let q = supabase
        .from("dp_guias")
        .select("id, company_id, tipo, data, created_at, file_path")
        .order("created_at", { ascending: false })
      if (companyFilter) q = q.in("company_id", companyFilter)
      return fetchAllPages<{
        id: string
        company_id: string
        tipo: string
        data: string
        created_at: string
        file_path?: string | null
      }>((from, to) => q.range(from, to))
    })(),
    (async () => {
      let q = supabase
        .from("municipal_tax_debts")
        .select("id, company_id, tributo, numero_documento, data_vencimento, valor, guia_pdf_path, fetched_at, created_at")
        .order("created_at", { ascending: false })
      if (companyFilter) q = q.in("company_id", companyFilter)
      return fetchAllPages<{
        id: string
        company_id: string
        tributo: string
        numero_documento?: string | null
        data_vencimento?: string | null
        valor?: number | null
        guia_pdf_path?: string | null
        fetched_at?: string | null
        created_at?: string | null
      }>((from, to) => q.range(from, to))
    })(),
  ])

  const companyIdsList = [
    ...new Set(
      [
        ...fiscalDocs.map((d) => d.company_id),
        ...certidoes.map((d) => d.company_id),
        ...dpGuiasRes.map((d) => d.company_id),
        ...municipalRes.map((d) => d.company_id),
      ].filter(Boolean)
    ),
  ]
  const companies = companyIdsList.length ? await fetchCompaniesByIds(companyIdsList, true) : []
  const names = new Map(companies.map((c) => [c.id, c.name]))
  const documents = new Map(companies.map((c) => [c.id, c.document ?? null]))

  const unified: UnifiedDocumentRow[] = []

  for (const d of fiscalDocs) {
    const fp = String((d as { file_path?: string | null }).file_path ?? "")
    const origem = /\/Recebidas\//i.test(fp) ? "recebidas" : /\/Emitidas\//i.test(fp) ? "emitidas" : null
    unified.push({
      id: d.id,
      company_id: d.company_id,
      empresa: d.empresa ?? names.get(d.company_id) ?? "",
      cnpj: d.cnpj ?? documents.get(d.company_id) ?? null,
      source: "fiscal",
      type: String(d.type || "NFS"),
      origem,
      status: String(d.status || "") || null,
      periodo: String(d.periodo || "") || null,
      document_date: String(d.document_date || "") || null,
      created_at: String(d.created_at || ""),
      file_path: d.file_path ?? null,
      chave: (d as { chave?: string | null }).chave ?? null,
    })
  }

  for (const d of certidoes) {
    const rawStatus = String((d as { status?: string | null }).status || "").trim().toLowerCase()
    const normalizedStatus = rawStatus === "regular" ? "negativa" : rawStatus
    unified.push({
      id: d.id,
      company_id: d.company_id,
      empresa: d.empresa ?? names.get(d.company_id) ?? "",
      cnpj: d.cnpj ?? documents.get(d.company_id) ?? null,
      source: "certidoes",
      type: `CERTIDÃO - ${String((d as { tipo_certidao?: string }).tipo_certidao || "").trim() || "OUTRA"}`,
      origem: null,
      status: normalizedStatus || null,
      periodo: String((d as { periodo?: string | null }).periodo || "") || null,
      document_date: String((d as { document_date?: string | null }).document_date || "") || null,
      created_at: String((d as { created_at?: string }).created_at || ""),
      file_path: (d as { file_path?: string | null }).file_path ?? null,
    })
  }

  for (const g of dpGuiasRes) {
    unified.push({
      id: g.id,
      company_id: g.company_id,
      empresa: names.get(g.company_id) ?? "",
      cnpj: documents.get(g.company_id) ?? null,
      source: "dp_guias",
      type: `GUIA - ${String(g.tipo || "OUTROS")}`,
      origem: null,
      status: null,
      periodo: String(g.data || "").slice(0, 7) || null,
      document_date: String(g.data || "").slice(0, 10) || null,
      created_at: String(g.created_at || ""),
      file_path: (g as { file_path?: string | null }).file_path ?? null,
    })
  }

  for (const m of municipalRes) {
    const valorNum = Number((m as { valor?: unknown }).valor ?? 0)
    const venc = String((m as { data_vencimento?: string | null }).data_vencimento || "").slice(0, 10)
    const today = new Date()
    const base = new Date(today.getFullYear(), today.getMonth(), today.getDate()).getTime()
    let statusClass: "vencido" | "a_vencer" | "regular" = "regular"
    if (valorNum === 0) {
      statusClass = "regular"
    } else if (venc) {
      const due = new Date(`${venc}T00:00:00`).getTime()
      const diffDays = Math.ceil((due - base) / (24 * 60 * 60 * 1000))
      if (diffDays < 0) statusClass = "vencido"
      else if (diffDays <= 30) statusClass = "a_vencer"
      else statusClass = "regular"
    }
    unified.push({
      id: m.id,
      company_id: m.company_id,
      empresa: names.get(m.company_id) ?? "",
      cnpj: documents.get(m.company_id) ?? null,
      source: "municipal_taxes",
      type: `IMPOSTO/TAXA - ${String(m.tributo || "OUTROS")}`,
      origem: null,
      status: statusClass,
      periodo: String(m.data_vencimento || "").slice(0, 7) || null,
      document_date: String(m.data_vencimento || "").slice(0, 10) || null,
      created_at: String(m.fetched_at || m.created_at || ""),
      file_path: String(m.guia_pdf_path || "") || null,
    })
  }

  // Só mantém entradas com arquivo (objetivo da tela: XML/PDF no disco)
  return unified
    .filter((d) => d.file_path && String(d.file_path).trim())
    .sort((a, b) => String(b.created_at || "").localeCompare(String(a.created_at || "")))
}

async function getFiscalOverviewAnalyticsLegacy(companyIds: string[] | null, dateFrom: string, dateTo: string) {
  let docsQuery = supabase
    .from("fiscal_documents")
    .select("id, company_id, type, status, periodo, document_date, created_at, file_path")
    .order("created_at", { ascending: false })
  if (companyIds && companyIds.length > 0) docsQuery = docsQuery.in("company_id", companyIds)

  const docs = await fetchAllPages<{
    id: string
    company_id: string
    type: string
    status: string
    periodo?: string | null
    document_date?: string | null
    created_at?: string | null
    file_path?: string | null
  }>((from, to) => docsQuery.range(from, to))
  const companyIdsList = [...new Set(docs.map((doc) => doc.company_id))]
  const companies = companyIdsList.length ? await fetchCompaniesByIds(companyIdsList) : []
  const companyMap = new Map(companies.map((company) => [company.id, company.name]))
  const filteredDocs = docs.filter((doc) => isWithinDateRange(resolveDocumentReferenceDate(doc), dateFrom, dateTo))
  const today = new Date().toISOString().slice(0, 10)
  const typeMap = new Map<string, number>()
  const statusMap = new Map<string, number>()
  const companyVolumeMap = new Map<string, number>()
  const monthMap = new Map(buildMonthsBetween(dateFrom, dateTo).map((item) => [item.key, 0]))
  const emissionDay = (doc: { document_date?: string | null }) => {
    const d = doc.document_date
    if (d == null || String(d).trim() === "") return ""
    return String(d).slice(0, 10)
  }

  for (const doc of filteredDocs) {
    const type = String(doc.type || "OUTROS").toUpperCase()
    typeMap.set(type, (typeMap.get(type) ?? 0) + 1)
    const status = String(doc.status || "pendente").toLowerCase()
    statusMap.set(status, (statusMap.get(status) ?? 0) + 1)
    companyVolumeMap.set(doc.company_id, (companyVolumeMap.get(doc.company_id) ?? 0) + 1)
    const month = resolveDocumentReferenceDate(doc).slice(0, 7)
    if (monthMap.has(month)) monthMap.set(month, (monthMap.get(month) ?? 0) + 1)
  }

  const periodMonths = buildMonthsBetween(dateFrom, dateTo)
  return {
    cards: {
      totalDocumentos: filteredDocs.length,
      documentosHoje: filteredDocs.filter((doc) => {
        const day = emissionDay(doc)
        return day !== "" && day === today
      }).length,
      documentosPendentes: filteredDocs.filter((doc) => ["pendente", "processando", "divergente"].includes(String(doc.status || "").toLowerCase())).length,
      documentosRejeitados: filteredDocs.filter((doc) => ["rejeitado", "rejected", "cancelado", "cancelada"].includes(String(doc.status || "").toLowerCase())).length,
      empresasComEmissao: companyVolumeMap.size,
    },
    byType: [...typeMap.entries()].map(([name, value]) => ({ name, value })).sort((a, b) => b.value - a.value),
    byMonth: periodMonths.map((month) => ({ name: month.label, value: monthMap.get(month.key) ?? 0 })),
    byCompany: [...companyVolumeMap.entries()]
      .map(([companyId, value]) => ({ name: companyMap.get(companyId) ?? "Empresa sem nome", value }))
      .sort((a, b) => b.value - a.value)
      .slice(0, 8),
    byStatus: [...statusMap.entries()].map(([name, value]) => ({ name, value })).sort((a, b) => b.value - a.value),
    byTypeSummary: {
      NFS: typeMap.get("NFS") ?? 0,
      NFE: typeMap.get("NFE") ?? 0,
      NFC: typeMap.get("NFC") ?? 0,
      outros: [...typeMap.entries()].filter(([key]) => !["NFS", "NFE", "NFC"].includes(key)).reduce((sum, [, value]) => sum + value, 0),
    },
  }
}

async function getDashboardOverviewLegacy(companyIds: string[] | null) {
  const filterByCompany = companyIds && companyIds.length > 0
  const companyFilter = filterByCompany ? companyIds : undefined

  let docsQuery = supabase
    .from("fiscal_documents")
    .select("id, company_id, type, status, periodo, document_date, created_at, file_path, chave")
    .order("created_at", { ascending: false })
  if (companyFilter) docsQuery = docsQuery.in("company_id", companyFilter)

  let syncQuery = supabase
    .from("sync_events")
    .select("id, company_id, tipo, status, created_at")
    .order("created_at", { ascending: false })
    .limit(8)
  if (companyFilter) syncQuery = syncQuery.in("company_id", companyFilter)

  let fiscalPendenciasQuery = supabase
    .from("fiscal_pendencias")
    .select("id, company_id, status", { count: "exact" })
  if (companyFilter) fiscalPendenciasQuery = fiscalPendenciasQuery.in("company_id", companyFilter)

  let dpChecklistQuery = supabase
    .from("dp_checklist")
    .select("id, company_id, tarefa, competencia, status", { count: "exact" })
  if (companyFilter) dpChecklistQuery = dpChecklistQuery.in("company_id", companyFilter)

  let dpGuiasQuery = supabase
    .from("dp_guias")
    .select("id, company_id, tipo, data, created_at")
  if (companyFilter) dpGuiasQuery = dpGuiasQuery.in("company_id", companyFilter)

  let financialRecordsQuery = supabase
    .from("financial_records")
    .select("id, company_id, periodo, status, pendencias_count, created_at")
  if (companyFilter) financialRecordsQuery = financialRecordsQuery.in("company_id", companyFilter)

  const [companiesCountRes, docsRaw, syncRes, fiscalPendencias, dpChecklist, dpGuias, financialRecords] = await Promise.all([
    supabase.from("companies").select("id", { count: "exact", head: true }),
    fetchAllPages<{
      id: string
      company_id: string
      type: string
      status: string
      periodo?: string | null
      document_date?: string | null
      created_at?: string | null
      file_path?: string | null
      chave?: string | null
    }>((from, to) => docsQuery.range(from, to)),
    syncQuery,
    fetchAllPages<{
      id: string
      company_id: string
      status: string
    }>((from, to) => fiscalPendenciasQuery.range(from, to)),
    fetchAllPages<{
      id: string
      company_id: string
      tarefa: string
      competencia: string
      status: string
    }>((from, to) => dpChecklistQuery.range(from, to)),
    fetchAllPages<{
      id: string
      company_id: string
      tipo: string
      data: string
      created_at: string
    }>((from, to) => dpGuiasQuery.range(from, to)),
    fetchAllPages<{
      id: string
      company_id: string
      periodo: string
      status: string
      pendencias_count: number
      created_at: string
    }>((from, to) => financialRecordsQuery.range(from, to)),
  ])

  if (companiesCountRes.error) throw companiesCountRes.error
  if (syncRes.error) throw syncRes.error
  const docKey = (d: { id: string; chave?: string | null }) =>
    (d.chave && String(d.chave).trim() ? String(d.chave).trim() : d.id) as string
  const seenDocs = new Set<string>()
  const docs = docsRaw.filter((d) => {
    const key = docKey(d)
    if (seenDocs.has(key)) return false
    seenDocs.add(key)
    return true
  })
  const companyIdsUsed = [...new Set([
    ...docs.map((doc) => doc.company_id),
    ...dpGuias.map((item) => item.company_id),
    ...dpChecklist.map((item) => item.company_id),
    ...syncRes.data?.map((item) => item.company_id).filter(Boolean) ?? [],
    ...financialRecords.map((item) => item.company_id),
  ])]
  const companies = companyIdsUsed.length ? await fetchCompaniesByIds(companyIdsUsed) : []
  const companyNameById = new Map(companies.map((company) => [company.id, company.name]))
  const docsByTypeMap = { NFS: 0, NFE: 0, NFC: 0 }
  const statusMap = new Map<string, number>()
  const perCompanyMap = new Map<string, number>()
  const monthKeys = buildMonthKeys(6)
  const monthCountMap = new Map(monthKeys.map((month) => [month.key, 0]))

  for (const doc of docs) {
    const type = String(doc.type || "").toUpperCase() as "NFS" | "NFE" | "NFC"
    if (type in docsByTypeMap) docsByTypeMap[type] += 1
    const status = String(doc.status || "pendente")
    statusMap.set(status, (statusMap.get(status) ?? 0) + 1)
    perCompanyMap.set(doc.company_id, (perCompanyMap.get(doc.company_id) ?? 0) + 1)
    const refDate = String(doc.document_date || doc.created_at || "").slice(0, 7)
    if (monthCountMap.has(refDate)) monthCountMap.set(refDate, (monthCountMap.get(refDate) ?? 0) + 1)
  }

  const totalNotasFiscais = docs.length
  const totalArquivosFisicos = new Set(docs.map((d) => d.file_path).filter((p): p is string => Boolean(p && String(p).trim()))).size
  const processedDocuments = totalNotasFiscais
  const importedDocuments = totalArquivosFisicos
  const totalDocuments = totalArquivosFisicos
  const today = new Date().toISOString().slice(0, 10)
  const currentMonth = today.slice(0, 7)
  const syncEvents = syncRes.data ?? []
  const fiscalPendenciasAbertas = fiscalPendencias.filter((item) => String(item.status || "").toLowerCase() !== "concluido")
  const dpPendencias = dpChecklist.filter((item) => String(item.status || "").toLowerCase() !== "concluido")
  const dpGuideTypeMap = new Map<string, number>()
  for (const guia of dpGuias) {
    const tipo = String(guia.tipo || "Outros")
    dpGuideTypeMap.set(tipo, (dpGuideTypeMap.get(tipo) ?? 0) + 1)
  }
  const contabilMonthKeys = buildMonthKeys(6)
  const contabilMonthMap = new Map(contabilMonthKeys.map((month) => [month.key, 0]))
  const contabilUpdatedStatuses = ["concluido", "completed", "validado", "atualizado", "pago"]
  for (const record of financialRecords) {
    const periodo = String(record.periodo || "").slice(0, 7)
    if (contabilMonthMap.has(periodo)) contabilMonthMap.set(periodo, (contabilMonthMap.get(periodo) ?? 0) + 1)
  }
  const fiscalCompanyCount = new Set(docs.map((doc) => doc.company_id)).size
  const dpCompanyCount = new Set([...dpGuias.map((item) => item.company_id), ...dpChecklist.map((item) => item.company_id)]).size

  const topCompanies = [...perCompanyMap.entries()]
    .map(([companyId, total]) => ({
      companyId,
      companyName: companyNameById.get(companyId) ?? "Empresa sem nome",
      total,
    }))
    .sort((a, b) => b.total - a.total)
    .slice(0, 6)

  return {
    companiesCount: companiesCountRes.count ?? 0,
    documentsCount: totalNotasFiscais,
    totalNotasFiscais,
    totalArquivosFisicos,
    importedDocuments,
    totalDocuments,
    docsByType: [
      { name: "NFS-e", value: docsByTypeMap.NFS },
      { name: "NF-e", value: docsByTypeMap.NFE },
      { name: "NFC-e", value: docsByTypeMap.NFC },
    ],
    documentsPerMonth: monthKeys.map((month) => ({
      name: month.label,
      value: monthCountMap.get(month.key) ?? 0,
    })),
    processingStatus: [...statusMap.entries()].map(([name, value]) => ({ name, value })),
    topCompanies,
    fiscalSummary: {
      totalPendencias: fiscalPendenciasAbertas.length,
      totalDocumentos: totalNotasFiscais,
    },
    dpSummary: {
      totalPendencias: dpPendencias.length,
      totalChecklist: dpChecklist.length,
      totalGuias: dpGuias.length,
      folhaProcessadaMes: dpChecklist.filter((item) => String(item.tarefa || "").toLowerCase().includes("folha") && String(item.status || "").toLowerCase() === "concluido" && String(item.competencia || "").slice(0, 7) === currentMonth).length,
      empresasAtivas: dpCompanyCount,
      guiasPorTipo: [...dpGuideTypeMap.entries()].map(([name, value]) => ({ name, value })),
    },
    contabilSummary: {
      balancosGerados: financialRecords.length,
      empresasAtualizadas: new Set(financialRecords.filter((item) => contabilUpdatedStatuses.includes(String(item.status || "").toLowerCase())).map((item) => item.company_id)).size,
      empresasPendentes: new Set(financialRecords.filter((item) => !contabilUpdatedStatuses.includes(String(item.status || "").toLowerCase()) || Number(item.pendencias_count || 0) > 0).map((item) => item.company_id)).size,
      lancamentosNoPeriodo: financialRecords.filter((item) => String(item.periodo || "").slice(0, 7) === currentMonth).length,
      lancamentosPorMes: contabilMonthKeys.map((month) => ({ name: month.label, value: contabilMonthMap.get(month.key) ?? 0 })),
    },
    pendingTabs: {
      fiscal: fiscalPendenciasAbertas.length,
      dp: dpPendencias.length,
      total: fiscalPendenciasAbertas.length + dpPendencias.length,
    },
    executiveSummary: {
      fiscal: {
        totalDocumentos: totalNotasFiscais,
        totalNotasFiscais,
        totalArquivosFisicos,
        processadosHoje: docs.filter((doc) => resolveDocumentReferenceDate(doc) === today).length,
        empresasAtivas: fiscalCompanyCount,
      },
      dp: {
        guiasGeradas: dpGuias.length,
        guiasPendentes: dpPendencias.length,
        folhaProcessadaMes: dpChecklist.filter((item) => String(item.tarefa || "").toLowerCase().includes("folha") && String(item.status || "").toLowerCase() === "concluido" && String(item.competencia || "").slice(0, 7) === currentMonth).length,
      },
      contabil: {
        balancosGerados: financialRecords.length,
        empresasAtualizadas: new Set(financialRecords.filter((item) => contabilUpdatedStatuses.includes(String(item.status || "").toLowerCase())).map((item) => item.company_id)).size,
        pendentes: new Set(financialRecords.filter((item) => !contabilUpdatedStatuses.includes(String(item.status || "").toLowerCase()) || Number(item.pendencias_count || 0) > 0).map((item) => item.company_id)).size,
      },
    },
    syncSummary: {
      totalEventos: syncEvents.length,
      falhas: syncEvents.filter((event) => String(event.status || "").toLowerCase() === "failed").length,
      sucessos: syncEvents.filter((event) => String(event.status || "").toLowerCase() === "completed").length,
    },
    syncEvents: syncEvents.map((event) => ({
      ...event,
      companyName: event.company_id ? companyNameById.get(event.company_id) ?? "Empresa sem nome" : "Sistema",
    })),
  }
}

function normalizeRpcMonthSeries(
  rows: Array<{ key?: string; value?: number }> | undefined,
  labelByKey: Map<string, string>,
) {
  return (rows ?? []).map((row) => ({
    name: labelByKey.get(String(row.key || "")) ?? String(row.key || ""),
    value: Number(row.value ?? 0),
  }))
}

export async function getFiscalOverviewAnalytics(companyIds: string[] | null, dateFrom: string, dateTo: string) {
  try {
    if (!canUseFiscalOverviewAnalyticsRpc) throw new Error("Fiscal overview analytics RPC disabled for this session")
    const { data, error } = await supabase.rpc("get_fiscal_overview_analytics_summary", {
      company_ids: companyIds && companyIds.length > 0 ? companyIds : null,
      date_from: dateFrom,
      date_to: dateTo,
    })
    if (error) throw error

    const payload = (data ?? {}) as {
      cards?: {
        totalDocumentos?: number
        documentosHoje?: number
        empresasComEmissao?: number
      }
      byType?: Array<{ name?: string; value?: number }>
      byMonth?: Array<{ key?: string; value?: number }>
      byCompany?: Array<{ name?: string; value?: number }>
      byStatus?: Array<{ name?: string; value?: number }>
      byTypeSummary?: { NFS?: number; NFE?: number; NFC?: number; outros?: number }
    }
    if (Number(payload.cards?.totalDocumentos ?? 0) === 0) throw new Error("Fiscal overview analytics RPC returned empty payload")

    const monthLabels = new Map(buildMonthsBetween(dateFrom, dateTo).map((item) => [item.key, item.label]))
    return {
      cards: {
        totalDocumentos: Number(payload.cards?.totalDocumentos ?? 0),
        documentosHoje: Number(payload.cards?.documentosHoje ?? 0),
        empresasComEmissao: Number(payload.cards?.empresasComEmissao ?? 0),
      },
      byType: (payload.byType ?? []).map((item) => ({
        name: String(item.name ?? "OUTROS"),
        value: Number(item.value ?? 0),
      })),
      byMonth: normalizeRpcMonthSeries(payload.byMonth, monthLabels),
      byCompany: (payload.byCompany ?? []).map((item) => ({
        name: String(item.name ?? "Empresa sem nome"),
        value: Number(item.value ?? 0),
      })),
      byStatus: (payload.byStatus ?? []).map((item) => ({
        name: String(item.name ?? "pendente"),
        value: Number(item.value ?? 0),
      })),
      byTypeSummary: {
        NFS: Number(payload.byTypeSummary?.NFS ?? 0),
        NFE: Number(payload.byTypeSummary?.NFE ?? 0),
        NFC: Number(payload.byTypeSummary?.NFC ?? 0),
        outros: Number(payload.byTypeSummary?.outros ?? 0),
      },
    }
  } catch {
    canUseFiscalOverviewAnalyticsRpc = false
    return getFiscalOverviewAnalyticsLegacy(companyIds, dateFrom, dateTo)
  }
}

export async function getDashboardOverview(companyIds: string[] | null) {
  try {
    if (!canUseDashboardOverviewRpc) throw new Error("Dashboard overview RPC disabled for this session")
    const { data, error } = await supabase.rpc("get_dashboard_overview_summary", {
      company_ids: companyIds && companyIds.length > 0 ? companyIds : null,
    })
    if (error) throw error

    const payload = (data ?? {}) as {
      companiesCount?: number
      documentsCount?: number
      totalNotasFiscais?: number
      totalArquivosFisicos?: number
      importedDocuments?: number
      totalDocuments?: number
      docsByType?: Array<{ name?: string; value?: number }>
      documentsPerMonth?: Array<{ key?: string; value?: number }>
      processingStatus?: Array<{ name?: string; value?: number }>
      topCompanies?: Array<{ companyId?: string; companyName?: string; total?: number }>
      fiscalSummary?: { totalPendencias?: number; totalDocumentos?: number }
      dpSummary?: {
        totalPendencias?: number
        totalChecklist?: number
        totalGuias?: number
        folhaProcessadaMes?: number
        empresasAtivas?: number
        guiasPorTipo?: Array<{ name?: string; value?: number }>
      }
      contabilSummary?: {
        balancosGerados?: number
        empresasAtualizadas?: number
        empresasPendentes?: number
        lancamentosNoPeriodo?: number
        lancamentosPorMes?: Array<{ key?: string; value?: number }>
      }
      pendingTabs?: { fiscal?: number; dp?: number; total?: number }
      executiveSummary?: {
        fiscal?: {
          totalDocumentos?: number
          totalNotasFiscais?: number
          totalArquivosFisicos?: number
          processadosHoje?: number
          empresasAtivas?: number
        }
        dp?: { guiasGeradas?: number; guiasPendentes?: number; folhaProcessadaMes?: number }
        contabil?: { balancosGerados?: number; empresasAtualizadas?: number; pendentes?: number }
      }
      syncSummary?: { totalEventos?: number; falhas?: number; sucessos?: number }
      syncEvents?: Array<{ id?: string; company_id?: string | null; tipo?: string; status?: string; created_at?: string; companyName?: string }>
    }
    if (Number(payload.totalNotasFiscais ?? payload.documentsCount ?? 0) === 0) throw new Error("Dashboard overview RPC returned empty payload")

    const monthLabels = new Map(buildMonthKeys(6).map((item) => [item.key, item.label]))
    const totalNotasFiscais = Number(payload.totalNotasFiscais ?? payload.documentsCount ?? 0)
    const totalArquivosFisicos = Number(payload.totalArquivosFisicos ?? payload.totalDocuments ?? 0)
    return {
      companiesCount: Number(payload.companiesCount ?? 0),
      documentsCount: totalNotasFiscais,
      totalNotasFiscais,
      totalArquivosFisicos,
      importedDocuments: totalArquivosFisicos,
      totalDocuments: totalArquivosFisicos,
      docsByType: (payload.docsByType ?? []).map((item) => ({
        name: String(item.name ?? "Outros"),
        value: Number(item.value ?? 0),
      })),
      documentsPerMonth: normalizeRpcMonthSeries(payload.documentsPerMonth, monthLabels),
      processingStatus: (payload.processingStatus ?? []).map((item) => ({
        name: String(item.name ?? "pendente"),
        value: Number(item.value ?? 0),
      })),
      topCompanies: (payload.topCompanies ?? []).map((item) => ({
        companyId: String(item.companyId ?? ""),
        companyName: String(item.companyName ?? "Empresa sem nome"),
        total: Number(item.total ?? 0),
      })),
      fiscalSummary: {
        totalPendencias: Number(payload.fiscalSummary?.totalPendencias ?? 0),
        totalDocumentos: Number(payload.fiscalSummary?.totalDocumentos ?? 0),
      },
      dpSummary: {
        totalPendencias: Number(payload.dpSummary?.totalPendencias ?? 0),
        totalChecklist: Number(payload.dpSummary?.totalChecklist ?? 0),
        totalGuias: Number(payload.dpSummary?.totalGuias ?? 0),
        folhaProcessadaMes: Number(payload.dpSummary?.folhaProcessadaMes ?? 0),
        empresasAtivas: Number(payload.dpSummary?.empresasAtivas ?? 0),
        guiasPorTipo: (payload.dpSummary?.guiasPorTipo ?? []).map((item) => ({
          name: String(item.name ?? "Outros"),
          value: Number(item.value ?? 0),
        })),
      },
      contabilSummary: {
        balancosGerados: Number(payload.contabilSummary?.balancosGerados ?? 0),
        empresasAtualizadas: Number(payload.contabilSummary?.empresasAtualizadas ?? 0),
        empresasPendentes: Number(payload.contabilSummary?.empresasPendentes ?? 0),
        lancamentosNoPeriodo: Number(payload.contabilSummary?.lancamentosNoPeriodo ?? 0),
        lancamentosPorMes: normalizeRpcMonthSeries(payload.contabilSummary?.lancamentosPorMes, monthLabels),
      },
      pendingTabs: {
        fiscal: Number(payload.pendingTabs?.fiscal ?? 0),
        dp: Number(payload.pendingTabs?.dp ?? 0),
        total: Number(payload.pendingTabs?.total ?? 0),
      },
      executiveSummary: {
        fiscal: {
          totalDocumentos: Number(payload.executiveSummary?.fiscal?.totalDocumentos ?? totalNotasFiscais),
          totalNotasFiscais: Number(payload.executiveSummary?.fiscal?.totalNotasFiscais ?? totalNotasFiscais),
          totalArquivosFisicos: Number(payload.executiveSummary?.fiscal?.totalArquivosFisicos ?? totalArquivosFisicos),
          processadosHoje: Number(payload.executiveSummary?.fiscal?.processadosHoje ?? 0),
          empresasAtivas: Number(payload.executiveSummary?.fiscal?.empresasAtivas ?? 0),
        },
        dp: {
          guiasGeradas: Number(payload.executiveSummary?.dp?.guiasGeradas ?? 0),
          guiasPendentes: Number(payload.executiveSummary?.dp?.guiasPendentes ?? 0),
          folhaProcessadaMes: Number(payload.executiveSummary?.dp?.folhaProcessadaMes ?? 0),
        },
        contabil: {
          balancosGerados: Number(payload.executiveSummary?.contabil?.balancosGerados ?? 0),
          empresasAtualizadas: Number(payload.executiveSummary?.contabil?.empresasAtualizadas ?? 0),
          pendentes: Number(payload.executiveSummary?.contabil?.pendentes ?? 0),
        },
      },
      syncSummary: {
        totalEventos: Number(payload.syncSummary?.totalEventos ?? 0),
        falhas: Number(payload.syncSummary?.falhas ?? 0),
        sucessos: Number(payload.syncSummary?.sucessos ?? 0),
      },
      syncEvents: (payload.syncEvents ?? []).map((event) => ({
        id: String(event.id ?? ""),
        company_id: event.company_id ?? null,
        tipo: String(event.tipo ?? ""),
        status: String(event.status ?? ""),
        created_at: String(event.created_at ?? ""),
        companyName: String(event.companyName ?? "Sistema"),
      })),
    }
  } catch {
    canUseDashboardOverviewRpc = false
    return getDashboardOverviewLegacy(companyIds)
  }
}
