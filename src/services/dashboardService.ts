import { supabase } from "./supabaseClient"

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

function resolveDocumentReferenceDate(document: { document_date?: string | null; created_at?: string | null }) {
  return String(document.document_date || document.created_at || "").slice(0, 10)
}

function isWithinDateRange(value: string, dateFrom?: string, dateTo?: string) {
  if (!value) return false
  if (dateFrom && value < dateFrom) return false
  if (dateTo && value > dateTo) return false
  return true
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

function normalizeCompanyIds(companyIds: string[] | null) {
  if (!companyIds?.length) return []
  return [...new Set(
    companyIds
      .map((id) => String(id || "").trim().toLowerCase())
      .filter(Boolean)
  )]
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

export async function getDashboardCounts(companyIds: string[] | null) {
  const filterByCompany = companyIds && companyIds.length > 0
  const companyFilter = filterByCompany ? companyIds : undefined

  const [companiesRes, docsRes] = await Promise.all([
    supabase.from("companies").select("id", { count: "exact", head: true }),
    companyFilter
      ? supabase.from("fiscal_documents").select("id", { count: "exact", head: true }).in("company_id", companyFilter)
      : supabase.from("fiscal_documents").select("id", { count: "exact", head: true }),
  ])

  return {
    companiesCount: companiesRes.count ?? 0,
    documentsCount: docsRes.count ?? 0,
  }
}

export async function getRecentFiscalDocuments(companyIds: string[] | null, limit: number) {
  let q = supabase
    .from("fiscal_documents")
    .select("id, company_id, type, status, created_at")
    .order("created_at", { ascending: false })
    .limit(limit)
  if (companyIds && companyIds.length > 0) {
    q = q.in("company_id", companyIds)
  }
  const { data, error } = await q
  if (error) throw error
  const list = data ?? []
  const companyIdsList = [...new Set(list.map((d) => d.company_id))]
  if (companyIdsList.length === 0) return []
  const { data: companies } = await supabase.from("companies").select("id, name").in("id", companyIdsList)
  const names = new Map((companies ?? []).map((c) => [c.id, c.name]))
  return list.map((d) => ({
    ...d,
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
  const { data, error } = await q
  if (error) throw error
  const list = data ?? []
  const companyIdsList = [...new Set(list.map((d) => d.company_id))]
  if (companyIdsList.length === 0) return []
  const { data: companies } = await supabase.from("companies").select("id, name, document").in("id", companyIdsList)
  const names = new Map((companies ?? []).map((c) => [c.id, c.name]))
  const documents = new Map((companies ?? []).map((c) => [c.id, c.document]))
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
  const { data, error } = await q
  if (error) throw error
  const list = data ?? []
  const companyIdsList = [...new Set(list.map((d) => d.company_id))]
  if (companyIdsList.length === 0) return []
  const { data: companies } = await supabase.from("companies").select("id, name, document").in("id", companyIdsList)
  const names = new Map((companies ?? []).map((c) => [c.id, c.name]))
  const documents = new Map((companies ?? []).map((c) => [c.id, c.document]))
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
  const { data, error } = await q
  if (error) throw error
  const list = (data ?? []).map((row) => {
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
  const { data: companies } = await supabase.from("companies").select("id, name, document").in("id", companyIdsList)
  const names = new Map((companies ?? []).map((c) => [c.id, c.name]))
  const documents = new Map((companies ?? []).map((c) => [c.id, c.document]))
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
    const key = `${d.company_id}:${tipoCertidao}`
    const current = latestByCompanyAndType.get(key)
    const candidate = {
      id: d.id,
      company_id: String(d.company_id || ""),
      periodo: String(payload.periodo || "") || null,
      status: String(payload.status || "") || null,
      document_date: String(payload.document_date || payload.data_consulta || "").slice(0, 10) || null,
      tipo_certidao: tipoCertidao,
      file_path: String(payload.arquivo_pdf || "") || null,
      created_at: String(d.created_at || ""),
    }
    if (!current || candidate.created_at > current.created_at) {
      latestByCompanyAndType.set(key, candidate)
    }
  }
  return [...latestByCompanyAndType.values()].map((d) => {
    return {
      ...d,
      empresa: names.get(d.company_id) ?? "",
      cnpj: documents.get(d.company_id) ?? "",
      tipo_certidao: tipoLabel[d.tipo_certidao] ?? d.tipo_certidao,
    }
  })
}

/** Resumo fiscal para a visão geral: totais por tipo (NFS, NFE, NFC) com métricas (total, disponíveis, este mês). Opcional: period YYYY-MM para filtrar por período. */
export async function getFiscalSummary(companyIds: string[] | null, period?: string) {
  let q = supabase.from("fiscal_documents").select("type, file_path, created_at, periodo")
  if (companyIds && companyIds.length > 0) {
    q = q.in("company_id", companyIds)
  }
  const { data, error } = await q
  if (error) throw error
  const rows = data ?? []
  const periodFilter = period && /^\d{4}-\d{2}$/.test(period) ? period : null
  const rowsFiltered = periodFilter
    ? rows.filter((r) => (r.periodo || "").trim() === periodFilter)
    : rows
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
  let q = supabase
    .from("nfs_stats")
    .select("company_id, qty_emitidas, qty_recebidas, valor_emitidas, valor_recebidas, service_codes")
    .eq("period", p)
  if (normalizedCompanyIds.length === 1) {
    q = q.eq("company_id", normalizedCompanyIds[0])
  } else if (normalizedCompanyIds.length > 1) {
    q = q.in("company_id", normalizedCompanyIds)
  }
  const { data, error } = await q
  if (error) throw error
  const rows = (data ?? []).filter((row) => {
    if (normalizedCompanyIds.length === 0) return true
    return normalizedCompanyIds.includes(String(row.company_id || "").trim().toLowerCase())
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
  }
}

/** Agrega nfs_stats para todos os meses entre dateFrom e dateTo (YYYY-MM-DD). */
export async function getNfsStatsByDateRange(companyIds: string[] | null, dateFrom: string, dateTo: string) {
  const normalizedCompanyIds = normalizeCompanyIds(companyIds)
  const from = dateFrom.slice(0, 7)
  const to = dateTo.slice(0, 7)
  if (from > to) return getNfsStatsByDateRange(companyIds, dateTo, dateFrom)
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
  let q = supabase
    .from("nfs_stats")
    .select("company_id, qty_emitidas, qty_recebidas, valor_emitidas, valor_recebidas, service_codes, service_codes_emitidas, service_codes_recebidas")
    .in("period", months)
  if (normalizedCompanyIds.length === 1) {
    q = q.eq("company_id", normalizedCompanyIds[0])
  } else if (normalizedCompanyIds.length > 1) {
    q = q.in("company_id", normalizedCompanyIds)
  }
  const { data, error } = await q
  if (error) throw error
  const rows = (data ?? []).filter((row) => {
    if (normalizedCompanyIds.length === 0) return true
    return normalizedCompanyIds.includes(String(row.company_id || "").trim().toLowerCase())
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
  const { data, error } = await q
  if (error) throw error
  const list = data ?? []
  const companyIdsList = [...new Set(list.map((d) => d.company_id))]
  if (companyIdsList.length === 0) return []
  const { data: companies } = await supabase.from("companies").select("id, name, document").in("id", companyIdsList)
  const names = new Map((companies ?? []).map((c) => [c.id, c.name]))
  const documents = new Map((companies ?? []).map((c) => [c.id, c.document]))
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
      const { data, error } = await q
      if (error) throw error
      return data ?? []
    })(),
    (async () => {
      let q = supabase
        .from("municipal_tax_debts")
        .select("id, company_id, tributo, numero_documento, data_vencimento, valor, guia_pdf_path, fetched_at, created_at")
        .order("created_at", { ascending: false })
      if (companyFilter) q = q.in("company_id", companyFilter)
      const { data, error } = await q
      if (error) throw error
      return data ?? []
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
  const { data: companies } = companyIdsList.length
    ? await supabase.from("companies").select("id, name, document").in("id", companyIdsList)
    : { data: [] as Array<{ id: string; name: string; document: string | null }> }
  const names = new Map((companies ?? []).map((c) => [c.id, c.name]))
  const documents = new Map((companies ?? []).map((c) => [c.id, c.document]))

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

export async function getFiscalOverviewAnalytics(companyIds: string[] | null, dateFrom: string, dateTo: string) {
  let docsQuery = supabase
    .from("fiscal_documents")
    .select("id, company_id, type, status, periodo, document_date, created_at, file_path")
    .order("created_at", { ascending: false })
  if (companyIds && companyIds.length > 0) docsQuery = docsQuery.in("company_id", companyIds)

  const { data: docs, error } = await docsQuery
  if (error) throw error
  const companyIdsList = [...new Set((docs ?? []).map((doc) => doc.company_id))]
  const { data: companies } = companyIdsList.length
    ? await supabase.from("companies").select("id, name").in("id", companyIdsList)
    : { data: [] as Array<{ id: string; name: string }> }
  const companyMap = new Map((companies ?? []).map((company) => [company.id, company.name]))
  const filteredDocs = (docs ?? []).filter((doc) => isWithinDateRange(resolveDocumentReferenceDate(doc), dateFrom, dateTo))
  const today = new Date().toISOString().slice(0, 10)
  const typeMap = new Map<string, number>()
  const statusMap = new Map<string, number>()
  const companyVolumeMap = new Map<string, number>()
  const monthMap = new Map(buildMonthsBetween(dateFrom, dateTo).map((item) => [item.key, 0]))

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
      documentosHoje: filteredDocs.filter((doc) => resolveDocumentReferenceDate(doc) === today).length,
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

export async function getDashboardOverview(companyIds: string[] | null) {
  const filterByCompany = companyIds && companyIds.length > 0
  const companyFilter = filterByCompany ? companyIds : undefined

  let docsQuery = supabase
    .from("fiscal_documents")
    .select("id, company_id, type, status, periodo, document_date, created_at, file_path")
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

  const [companiesRes, docsRes, syncRes, fiscalPendenciasRes, dpChecklistRes, dpGuiasRes, financialRecordsRes] = await Promise.all([
    supabase.from("companies").select("id, name", { count: "exact" }),
    docsQuery,
    syncQuery,
    fiscalPendenciasQuery,
    dpChecklistQuery,
    dpGuiasQuery,
    financialRecordsQuery,
  ])

  if (companiesRes.error) throw companiesRes.error
  if (docsRes.error) throw docsRes.error
  if (syncRes.error) throw syncRes.error
  if (fiscalPendenciasRes.error) throw fiscalPendenciasRes.error
  if (dpChecklistRes.error) throw dpChecklistRes.error
  if (dpGuiasRes.error) throw dpGuiasRes.error
  if (financialRecordsRes.error) throw financialRecordsRes.error

  const docs = docsRes.data ?? []
  const companies = companiesRes.data ?? []
  const dpGuias = dpGuiasRes.data ?? []
  const financialRecords = financialRecordsRes.data ?? []
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

  const processedDocuments = docs.length
  const importedDocuments = docs.filter((doc) => Boolean(doc.file_path)).length
  const totalDocuments = docs.length
  const today = new Date().toISOString().slice(0, 10)
  const currentMonth = today.slice(0, 7)
  const syncEvents = syncRes.data ?? []
  const fiscalPendencias = (fiscalPendenciasRes.data ?? []).filter((item) => String(item.status || "").toLowerCase() !== "concluido")
  const dpPendencias = (dpChecklistRes.data ?? []).filter((item) => String(item.status || "").toLowerCase() !== "concluido")
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
  const dpCompanyCount = new Set([...dpGuias.map((item) => item.company_id), ...dpChecklistRes.data?.map((item) => item.company_id) ?? []]).size

  const topCompanies = [...perCompanyMap.entries()]
    .map(([companyId, total]) => ({
      companyId,
      companyName: companyNameById.get(companyId) ?? "Empresa sem nome",
      total,
    }))
    .sort((a, b) => b.total - a.total)
    .slice(0, 6)

  return {
    companiesCount: companiesRes.count ?? companies.length,
    documentsCount: processedDocuments,
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
      totalPendencias: fiscalPendencias.length,
      totalDocumentos: docs.length,
    },
    dpSummary: {
      totalPendencias: dpPendencias.length,
      totalChecklist: dpChecklistRes.count ?? dpChecklistRes.data?.length ?? 0,
      totalGuias: dpGuias.length,
      folhaProcessadaMes: (dpChecklistRes.data ?? []).filter((item) => String(item.tarefa || "").toLowerCase().includes("folha") && String(item.status || "").toLowerCase() === "concluido" && String(item.competencia || "").slice(0, 7) === currentMonth).length,
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
      fiscal: fiscalPendencias.length,
      dp: dpPendencias.length,
      total: fiscalPendencias.length + dpPendencias.length,
    },
    executiveSummary: {
      fiscal: {
        totalDocumentos: docs.length,
        processadosHoje: docs.filter((doc) => resolveDocumentReferenceDate(doc) === today).length,
        empresasAtivas: fiscalCompanyCount,
      },
      dp: {
        guiasGeradas: dpGuias.length,
        guiasPendentes: dpPendencias.length,
        folhaProcessadaMes: (dpChecklistRes.data ?? []).filter((item) => String(item.tarefa || "").toLowerCase().includes("folha") && String(item.status || "").toLowerCase() === "concluido" && String(item.competencia || "").slice(0, 7) === currentMonth).length,
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
