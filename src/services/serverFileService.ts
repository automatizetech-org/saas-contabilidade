import JSZip from "jszip"
import { supabase } from "./supabaseClient"

const SUPABASE_URL = (import.meta.env.SUPABASE_URL ?? "").toString().trim().replace(/\/$/, "")
const SUPABASE_ANON_KEY = (import.meta.env.SUPABASE_ANON_KEY ?? "").toString().trim()
const INVALID_DOWNLOAD_NAME_PATTERN = new RegExp(
  '[<>:"/\\\\|?*' + String.fromCharCode(0) + "-" + String.fromCharCode(31) + "]",
  "g"
)

function triggerBlobDownload(blob: Blob, filename: string) {
  const a = document.createElement("a")
  const blobUrl = URL.createObjectURL(blob)
  a.href = blobUrl
  const safeName = String(filename || "")
    .split(/[\\/]/)
    .pop()
    ?.replace(INVALID_DOWNLOAD_NAME_PATTERN, "")
    .trim()
  a.download = safeName || "arquivo"
  a.style.display = "none"
  document.body.appendChild(a)
  a.click()
  a.remove()
  window.setTimeout(() => URL.revokeObjectURL(blobUrl), 30_000)
}

function openBlobInNewTab(blob: Blob, filename?: string) {
  const blobUrl = URL.createObjectURL(blob)
  const opened = window.open(blobUrl, "_blank", "noopener,noreferrer")
  if (!opened) {
    URL.revokeObjectURL(blobUrl)
    throw new Error("Nao foi possivel abrir a visualizacao do documento.")
  }

  const safeTitle = String(filename || "")
    .split(/[\\/]/)
    .pop()
    ?.replace(INVALID_DOWNLOAD_NAME_PATTERN, "")
    .trim()

  window.setTimeout(() => {
    try {
      if (safeTitle && opened.document) {
        opened.document.title = safeTitle
      }
    } catch {
      // Ignore cross-document title failures.
    }
  }, 500)

  window.setTimeout(() => URL.revokeObjectURL(blobUrl), 120_000)
}

/** Uma renovação em voo por vez (vários polls em paralelo não disparam vários refresh_token). */
let sessionRefreshPromise: Promise<string> | null = null
let sessionValidationPromise: Promise<void> | null = null

async function refreshAccessTokenOrThrow(): Promise<string> {
  if (!sessionRefreshPromise) {
    sessionRefreshPromise = (async () => {
      try {
        const { data: ref, error: refErr } = await supabase.auth.refreshSession()
        if (refErr || !ref.session?.access_token) {
          throw new Error("Sessão expirada. Faça login novamente.")
        }
        return ref.session.access_token
      } finally {
        sessionRefreshPromise = null
      }
    })()
  }
  return sessionRefreshPromise
}

/** Repara sessão corrompida/stale (token inválido em assinatura) antes de chamar a Edge. */
async function ensureSessionValidOrThrow(): Promise<void> {
  if (!sessionValidationPromise) {
    sessionValidationPromise = (async () => {
      try {
        const first = await supabase.auth.getUser()
        if (!first.error && first.data?.user?.id) return
        await refreshAccessTokenOrThrow()
        const second = await supabase.auth.getUser()
        if (second.error || !second.data?.user?.id) {
          throw new Error("Sessão não autorizada na API. Atualize a página ou faça login novamente.")
        }
      } finally {
        sessionValidationPromise = null
      }
    })()
  }
  return sessionValidationPromise
}

async function getAuthHeaders(contentType?: string) {
  const { data: { session }, error: sessErr } = await supabase.auth.getSession()
  if (sessErr || !session?.access_token) throw new Error("Faça login para continuar.")

  const nowSec = Math.floor(Date.now() / 1000)
  const exp = session.expires_at
  let accessToken = session.access_token
  if (typeof exp === "number" && exp <= nowSec + 120) {
    accessToken = await refreshAccessTokenOrThrow()
  }

  const headers: Record<string, string> = {
    Authorization: `Bearer ${accessToken}`,
    apikey: SUPABASE_ANON_KEY,
  }
  if (contentType) headers["Content-Type"] = contentType
  return headers
}

async function fetchOfficeServer(
  action: string,
  init?: RequestInit & { body?: BodyInit | null }
): Promise<Response> {
  if (!SUPABASE_URL || !SUPABASE_ANON_KEY) {
    throw new Error("Supabase não configurado.")
  }
  const url = `${SUPABASE_URL}/functions/v1/office-server?action=${encodeURIComponent(action)}`
  return fetch(url, init)
}

async function fetchOfficeServerWithAuthRetry(
  action: string,
  buildInit: (headers: Record<string, string>) => RequestInit & { body?: BodyInit | null },
): Promise<Response> {
  await ensureSessionValidOrThrow()
  let headers = await getAuthHeaders("application/json")
  let response = await fetchOfficeServer(action, buildInit(headers))
  if (response.status !== 401) return response

  try {
    await refreshAccessTokenOrThrow()
  } catch {
    return response
  }

  headers = await getAuthHeaders("application/json")
  response = await fetchOfficeServer(action, buildInit(headers))
  return response
}

async function readError(res: Response) {
  const text = await res.text().catch(() => "")
  try {
    const json = JSON.parse(text)
    return json.error || json.detail || `Erro ${res.status}`
  } catch {
    return text || `Erro ${res.status}`
  }
}

export async function postOfficeServerJson<T>(
  action: string,
  payload: Record<string, unknown>
): Promise<T> {
  const res = await fetchOfficeServerWithAuthRetry(action, (headers) => ({
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  }))
  if (!res.ok) throw new Error(await readError(res))
  return (await res.json().catch(() => ({}))) as T
}

/** Baixa a resposta em stream e chama onProgress(0-100). Retorna o blob. */
async function fetchBlobWithProgress(
  response: Response,
  onProgress?: (percent: number) => void
): Promise<Blob> {
  if (!response.ok) throw new Error(await readError(response))
  if (!response.body) throw new Error("Resposta sem corpo.")
  const contentLength = response.headers.get("Content-Length")
  const total = contentLength ? Number(contentLength) : 0
  const reader = response.body.getReader()
  const chunks: Uint8Array[] = []
  let received = 0
  let chunkCount = 0
  let displayedPercent = 0
  let targetPercent = 0
  const startedAt = Date.now()

  const emitProgress = (next: number) => {
    if (!onProgress) return
    const safe = Math.min(100, Math.max(displayedPercent, Math.round(next)))
    if (safe === displayedPercent) return
    displayedPercent = safe
    onProgress(safe)
  }

  const setTargetProgress = (next: number) => {
    targetPercent = Math.min(99, Math.max(targetPercent, Math.round(next)))
  }

  const progressTimer = onProgress
    ? window.setInterval(() => {
        if (displayedPercent >= targetPercent) return
        const step = Math.max(1, Math.ceil((targetPercent - displayedPercent) / 4))
        emitProgress(displayedPercent + step)
      }, 120)
    : null

  if (onProgress) onProgress(0)
  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      if (value) {
        chunks.push(value)
        received += value.length
        chunkCount += 1
        if (onProgress && total > 0) {
          const percent = Math.min(99, Math.round((received / total) * 100))
          setTargetProgress(percent)
        } else if (onProgress) {
          const elapsedSeconds = (Date.now() - startedAt) / 1000
          const estimated = Math.min(
            95,
            Math.max(
              displayedPercent + 1,
              6 + chunkCount * 3 + Math.floor(elapsedSeconds * 4)
            )
          )
          setTargetProgress(estimated)
        }
      }
    }
  } finally {
    if (progressTimer !== null) window.clearInterval(progressTimer)
    reader.releaseLock()
  }
  if (onProgress) onProgress(100)
  return new Blob(chunks)
}

export function getServerApiUrl(): string {
  return ""
}

export function hasServerApi(): boolean {
  return Boolean(SUPABASE_URL && SUPABASE_ANON_KEY)
}

/** Após 401 persistente, não ficar em loop com refresh_token a cada poll. */
let officeWhatsapp401CooldownUntil = 0

/** Uma chamada `action=whatsapp` de cada vez — evita 10–20 pedidos em paralelo no Network. */
let whatsappOfficeQueue: Promise<unknown> = Promise.resolve()

function enqueueWhatsAppOfficeCall<T>(fn: () => Promise<T>): Promise<T> {
  const run = whatsappOfficeQueue.then(fn, fn)
  whatsappOfficeQueue = run.then(
    () => undefined,
    () => undefined,
  )
  return run
}

/**
 * WhatsApp via Edge — usa `supabase.functions.invoke` (mesmo fetch com Bearer da sessão que o SDK usa no PostgREST).
 * Evita `fetch` manual com headers que podem divergir do cliente.
 */
async function invokeOfficeServerWhatsappEdge(
  body: Record<string, unknown>,
): Promise<Response> {
  const fn = `office-server?action=${encodeURIComponent("whatsapp")}`
  const { data, error, response } = await supabase.functions.invoke(fn, {
    method: "POST",
    body,
  })

  // Sempre honrar o status HTTP real da Edge (401/403/502). O SDK já pode ter lido o body
  // para preencher `data` — nesse caso `response.text()` falha (stream já consumido).
  if (response instanceof Response) {
    let text: string
    if (response.bodyUsed) {
      if (data !== undefined && data !== null) {
        text = typeof data === "string" ? data : JSON.stringify(data)
      } else {
        text = JSON.stringify({
          error:
            error instanceof Error ? error.message : "Falha ao chamar office-server",
        })
      }
    } else {
      text = await response.text()
    }
    return new Response(text, {
      status: response.status,
      headers: {
        "Content-Type":
          response.headers.get("Content-Type") || "application/json",
      },
    })
  }

  if (error) {
    return new Response(
      JSON.stringify({
        error: error instanceof Error ? error.message : "Falha ao chamar office-server",
      }),
      { status: 502, headers: { "Content-Type": "application/json" } },
    )
  }

  return new Response(JSON.stringify(data ?? {}), {
    status: 200,
    headers: { "Content-Type": "application/json" },
  })
}

/** WhatsApp via Edge `office-server` → VM `/api/whatsapp/*` (escritório + JWT). */
export async function invokeOfficeServerWhatsApp(
  call: string,
  options?: { query?: string; payload?: Record<string, unknown> },
): Promise<Response> {
  const body: Record<string, unknown> = { call }
  if (options?.query) body.query = options.query
  if (options?.payload && Object.keys(options.payload).length > 0) {
    body.payload = options.payload
  }

  return enqueueWhatsAppOfficeCall(async () => {
    try {
      await ensureSessionValidOrThrow()
    } catch (e) {
      return new Response(
        JSON.stringify({
          error: e instanceof Error ? e.message : "Sessão não autorizada na API.",
        }),
        { status: 401, headers: { "Content-Type": "application/json" } },
      )
    }

    let res = await invokeOfficeServerWhatsappEdge(body)
    if (res.status !== 401) {
      if (res.ok) officeWhatsapp401CooldownUntil = 0
      return res
    }

    const now = Date.now()
    if (now < officeWhatsapp401CooldownUntil) return res

    try {
      await refreshAccessTokenOrThrow()
    } catch {
      officeWhatsapp401CooldownUntil = now + 60_000
      return res
    }
    res = await invokeOfficeServerWhatsappEdge(body)
    if (res.status === 401) officeWhatsapp401CooldownUntil = now + 60_000
    else officeWhatsapp401CooldownUntil = 0
    return res
  })
}

async function fetchServerFileByPath(filePath: string): Promise<{ blob: Blob; filename: string }> {
  const normalizedPath = String(filePath || "").trim()
  if (!normalizedPath) throw new Error("Caminho do arquivo não informado.")

  const res = await fetchOfficeServerWithAuthRetry("download-file", (headers) => ({
    method: "POST",
    headers,
    body: JSON.stringify({ file_path: normalizedPath }),
  }))

  if (!res.ok) throw new Error(await readError(res))

  const blob = await res.blob()
  const disposition = res.headers.get("Content-Disposition")
  const rawName =
    disposition?.match(/filename="?([^";]+)"?/)?.[1]?.trim() ||
    normalizedPath.split(/[\\/]/).pop() ||
    "arquivo.pdf"
  const filename = String(rawName).split(/[\\/]/).pop()?.trim() || "arquivo.pdf"
  return { blob, filename }
}

export async function probeServerFileByPath(
  filePath: string,
): Promise<{ exists: boolean; filename?: string | null }> {
  const normalizedPath = String(filePath || "").trim()
  if (!normalizedPath) return { exists: false, filename: null }

  const res = await fetchOfficeServerWithAuthRetry("download-file", (headers) => ({
    method: "POST",
    headers,
    body: JSON.stringify({ file_path: normalizedPath }),
  }))

  if (!res.ok) {
    try {
      await res.body?.cancel()
    } catch {
      // Ignore probe cleanup failures.
    }
    return { exists: false, filename: null }
  }

  const disposition = res.headers.get("Content-Disposition")
  const rawName =
    disposition?.match(/filename=\"?([^\";]+)\"?/)?.[1]?.trim() ||
    normalizedPath.split(/[\\/]/).pop() ||
    "arquivo"

  try {
    await res.body?.cancel()
  } catch {
    // Ignore probe cleanup failures.
  }

  return {
    exists: true,
    filename: String(rawName).split(/[\\/]/).pop()?.trim() || "arquivo",
  }
}

export async function downloadFiscalDocument(documentId: string, suggestedName?: string): Promise<void> {
  const res = await fetchOfficeServerWithAuthRetry("download-fiscal-document", (headers) => ({
    method: "POST",
    headers,
    body: JSON.stringify({ document_id: documentId }),
  }))
  if (!res.ok) throw new Error(await readError(res))
  const blob = await res.blob()
  const disposition = res.headers.get("Content-Disposition")
  const filename =
    disposition?.match(/filename="?([^";]+)"?/)?.[1]?.trim() ||
    suggestedName ||
    `documento-${documentId}.xml`
  triggerBlobDownload(blob, filename)
}

export async function downloadServerFileByPath(filePath: string, suggestedName?: string): Promise<void> {
  const { blob, filename } = await fetchServerFileByPath(filePath)
  triggerBlobDownload(blob, suggestedName || filename)
}

export async function openServerFileByPath(filePath: string, suggestedName?: string): Promise<void> {
  const { blob, filename } = await fetchServerFileByPath(filePath)
  openBlobInNewTab(blob, suggestedName || filename)
}

export async function downloadOfficeServerAction(
  action: string,
  payload: Record<string, unknown>,
  suggestedName?: string
): Promise<void> {
  const res = await fetchOfficeServerWithAuthRetry(action, (headers) => ({
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  }))
  if (!res.ok) throw new Error(await readError(res))
  const blob = await res.blob()
  const disposition = res.headers.get("Content-Disposition")
  const filename =
    disposition?.match(/filename="?([^";]+)"?/)?.[1]?.trim() ||
    suggestedName ||
    "arquivo"
  triggerBlobDownload(blob, filename)
}

export async function openOfficeServerAction(
  action: string,
  payload: Record<string, unknown>,
  suggestedName?: string,
): Promise<void> {
  const res = await fetchOfficeServerWithAuthRetry(action, (headers) => ({
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  }))
  if (!res.ok) throw new Error(await readError(res))
  const blob = await res.blob()
  const disposition = res.headers.get("Content-Disposition")
  const filename =
    disposition?.match(/filename="?([^";]+)"?/)?.[1]?.trim() ||
    suggestedName ||
    "arquivo"
  openBlobInNewTab(blob, filename)
}

export async function downloadServerFilesZip(filePaths: string[], suggestedName = "guias-municipais"): Promise<void> {
  const paths = filePaths.map((filePath) => String(filePath || "").trim()).filter(Boolean)
  if (paths.length === 0) throw new Error("Nenhum arquivo selecionado para baixar.")
  if (paths.length > MAX_FILES_CLIENT_ZIP) {
    throw new Error(`Limite de ${MAX_FILES_CLIENT_ZIP} arquivos por download. Selecione menos itens na lista.`)
  }

  const zip = new JSZip()
  const usedPaths = new Set<string>()
  const makeUniqueZipPath = (zipPath: string) => {
    let candidate = zipPath
    let i = 0
    while (usedPaths.has(candidate)) {
      i += 1
      const dotIndex = zipPath.lastIndexOf(".")
      const base = dotIndex >= 0 ? zipPath.slice(0, dotIndex) : zipPath
      const ext = dotIndex >= 0 ? zipPath.slice(dotIndex) : ""
      candidate = `${base} (${i})${ext}`
    }
    usedPaths.add(candidate)
    return candidate
  }

  await Promise.all(
    paths.map(async (filePath) => {
      const { blob, filename } = await fetchServerFileByPath(filePath)
      const normalized = String(filePath || "").replace(/\\/g, "/").replace(/^\/+/, "")
      const parts = normalized.split("/").filter(Boolean)
      const company = parts[0] || "EMPRESA"
      zip.file(makeUniqueZipPath(`${company}/${filename}`), blob)
    })
  )

  const zipBlob = await zip.generateAsync({ type: "blob", compression: "STORE" })
  triggerBlobDownload(zipBlob, `${suggestedName}.zip`)
}

const MAX_FILES_CLIENT_ZIP = 50000

/** Alinha com server-api: pastas 55/65, ou nome só com 44 dígitos (chave.xml do robô), ou NFe+chave. */
function inferNfeNfcModeloFromFilePath(filePath: string): "" | "55" | "65" {
  const norm = filePath.replace(/\\/g, "/")
  if (/\/65(\/|$)/i.test(norm) || /(^|\/|_)65(\/|_|\.)/i.test(norm)) return "65"
  if (/\/55(\/|$)/i.test(norm) || /(^|\/|_)55(\/|_|\.)/i.test(norm)) return "55"

  const tail = norm.match(/([^/]+\.xml)$/i)?.[1] ?? norm
  const baseNoExt = tail.replace(/\.xml$/i, "").trim()

  if (/^\d{44}$/.test(baseNoExt)) {
    const mod = baseNoExt.slice(20, 22)
    if (mod === "55" || mod === "65") return mod
  }

  const prefixed =
    tail.match(/nfe[_-]?(\d{44})/i) || tail.match(/nfc[_-]?(\d{44})/i)
  if (prefixed) {
    const mod = prefixed[1].slice(20, 22)
    if (mod === "55" || mod === "65") return mod
  }

  const tail44 = tail.match(/(\d{44})\.xml$/i)
  if (tail44) {
    const mod = tail44[1].slice(20, 22)
    if (mod === "55" || mod === "65") return mod
  }

  const embedded = baseNoExt.match(/(\d{44})/)
  if (embedded) {
    const mod = embedded[1].slice(20, 22)
    if (mod === "55" || mod === "65") return mod
  }

  return ""
}

/**
 * Download rápido: um único request ao servidor, que monta o ZIP em stream no disco.
 * Com dezenas de milhares de arquivos é muito mais rápido que N requests no cliente.
 * onProgress(0-100) opcional para barra de progresso.
 */
export type ZipDownloadItem = {
  companyName: string
  category: string
  filePath: string
  /** Subpasta opcional dentro de category (ex.: 55 / 65 para NF-e e NFC-e). */
  zipInnerSegment?: string | null
}

export async function downloadListedFilesZipWithCategory(
  items: ZipDownloadItem[],
  suggestedName = "documentos",
  onProgress?: (percent: number) => void
): Promise<void> {
  const normalizedItems = items
    .map((it) => {
      const inner = String(it.zipInnerSegment ?? "").trim()
      let zipInnerSegment: "" | "55" | "65" = inner === "55" || inner === "65" ? inner : ""
      const categoryNorm = String(it.category || "")
        .toLowerCase()
        .replace(/_/g, "-")
      const fp = String(it.filePath || "").trim()
      if (!zipInnerSegment && categoryNorm === "nfe-nfc") {
        zipInnerSegment = inferNfeNfcModeloFromFilePath(fp)
      }
      return {
        companyName: String(it.companyName || "EMPRESA").trim() || "EMPRESA",
        category: String(it.category || "outros").trim() || "outros",
        filePath: fp,
        zipInnerSegment,
      }
    })
    .filter((it) => it.filePath.length > 0)

  if (normalizedItems.length === 0) throw new Error("Nenhum arquivo selecionado para baixar.")
  if (normalizedItems.length > MAX_FILES_CLIENT_ZIP) {
    throw new Error(`Limite de ${MAX_FILES_CLIENT_ZIP} arquivos por download. Selecione menos itens na lista.`)
  }

  if (hasServerApi()) {
    const res = await fetchOfficeServerWithAuthRetry("download-zip-by-paths", (headers) => ({
      method: "POST",
      headers,
      body: JSON.stringify({
        items: normalizedItems.map((it) => ({
          file_path: it.filePath,
          company_name: it.companyName,
          category: it.category,
          ...(it.zipInnerSegment ? { zip_inner_segment: it.zipInnerSegment } : {}),
        })),
        filename_suffix: suggestedName !== "documentos" ? suggestedName.replace(/[^a-z0-9-]/gi, "-").slice(0, 32) : undefined,
      }),
    }))
    const blob = await fetchBlobWithProgress(res, onProgress)
    triggerBlobDownload(blob, `${suggestedName}.zip`)
    return
  }

  const zip = new JSZip()
  const usedPaths = new Set<string>()
  let completedFiles = 0
  const makeUniqueZipPath = (zipPath: string) => {
    let candidate = zipPath
    let i = 0
    while (usedPaths.has(candidate)) {
      i += 1
      const dotIndex = zipPath.lastIndexOf(".")
      const base = dotIndex >= 0 ? zipPath.slice(0, dotIndex) : zipPath
      const ext = dotIndex >= 0 ? zipPath.slice(dotIndex) : ""
      candidate = `${base} (${i})${ext}`
    }
    usedPaths.add(candidate)
    return candidate
  }

  await Promise.all(
    normalizedItems.map(async ({ companyName, category, filePath, zipInnerSegment }) => {
      const { blob, filename } = await fetchServerFileByPath(filePath)
      const safeCompany = companyName.replace(INVALID_DOWNLOAD_NAME_PATTERN, "").replace(/\s+/g, " ").trim() || "EMPRESA"
      const safeCategory = category.replace(INVALID_DOWNLOAD_NAME_PATTERN, "").replace(/\s+/g, " ").trim() || "outros"
      const catNorm = safeCategory.toLowerCase().replace(/_/g, "-")
      let inner = zipInnerSegment === "55" || zipInnerSegment === "65" ? zipInnerSegment : ""
      if (!inner && catNorm === "nfe-nfc") inner = inferNfeNfcModeloFromFilePath(filePath)
      const inZip = inner
        ? `${safeCompany}/${safeCategory}/${inner}/${filename}`
        : `${safeCompany}/${safeCategory}/${filename}`
      zip.file(makeUniqueZipPath(inZip), blob)
      completedFiles += 1
      if (onProgress) {
        onProgress(Math.min(90, Math.round((completedFiles / normalizedItems.length) * 90)))
      }
    })
  )

  const zipBlob = await zip.generateAsync(
    { type: "blob", compression: "STORE" },
    (metadata) => {
      if (!onProgress) return
      const zipPercent = 90 + Math.round((metadata.percent / 100) * 9)
      onProgress(Math.min(99, Math.max(90, zipPercent)))
    }
  )
  if (onProgress) onProgress(100)
  triggerBlobDownload(zipBlob, `${suggestedName}.zip`)
}

export async function markFiscalDocumentDownloaded(documentId: string): Promise<void> {
  const { error } = await supabase
    .from("fiscal_documents")
    .update({ last_downloaded_at: new Date().toISOString() })
    .eq("id", documentId)
  if (error) console.warn("Não foi possível atualizar last_downloaded_at:", error.message)
}

async function downloadZipAction(
  action: string,
  payload: Record<string, unknown>,
  filename: string,
  onProgress?: (percent: number) => void
): Promise<void> {
  const res = await fetchOfficeServerWithAuthRetry(action, (headers) => ({
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  }))
  const blob = await fetchBlobWithProgress(res, onProgress)
  triggerBlobDownload(blob, filename)
}

export async function downloadFiscalDocumentsZip(
  ids: string[],
  filenameSuffix?: string,
  onProgress?: (percent: number) => void
): Promise<void> {
  const idsFiltered = ids.filter((id) => id && String(id).trim())
  if (idsFiltered.length === 0) throw new Error("Nenhum documento selecionado para baixar.")
  const safeSuffix = filenameSuffix && /^[a-z0-9-]+$/i.test(filenameSuffix) ? `-${filenameSuffix}` : ""
  await downloadZipAction("download-fiscal-documents-zip", { ids: idsFiltered }, `documentos-fiscais${safeSuffix}.zip`, onProgress)
  await Promise.all(idsFiltered.map((id) => markFiscalDocumentDownloaded(id).catch(() => {})))
}

export async function downloadFiscalCompaniesZip(companyIds: string[], filenameSuffix?: string, types?: string[]): Promise<void> {
  const companyIdsFiltered = [...new Set(companyIds.map((id) => String(id || "").trim()).filter(Boolean))]
  if (companyIdsFiltered.length === 0) throw new Error("Nenhuma empresa selecionada para baixar.")
  const safeSuffix = filenameSuffix && /^[a-z0-9-]+$/i.test(filenameSuffix) ? `-${filenameSuffix}` : ""
  await downloadZipAction(
    "download-fiscal-companies-zip",
    { company_ids: companyIdsFiltered, types: Array.isArray(types) ? types : [] },
    `documentos-fiscais${safeSuffix}.zip`
  )
}

export async function downloadHubCompaniesZip(companyIds: string[], filenameSuffix?: string, categories?: string[]): Promise<void> {
  const companyIdsFiltered = [...new Set(companyIds.map((id) => String(id || "").trim()).filter(Boolean))]
  if (companyIdsFiltered.length === 0) throw new Error("Nenhuma empresa selecionada para baixar.")
  const safeSuffix = filenameSuffix && /^[a-z0-9-]+$/i.test(filenameSuffix) ? `-${filenameSuffix}` : ""
  await downloadZipAction(
    "download-hub-companies-zip",
    { company_ids: companyIdsFiltered, categories: Array.isArray(categories) ? categories : [] },
    `documentos-hub${safeSuffix}.zip`
  )
}

export async function fiscalSyncAll(): Promise<{ ok: boolean; inserted: number; skipped: number; deleted: number; errors?: Array<{ file: string; error: string }> }> {
  const res = await fetchOfficeServerWithAuthRetry("fiscal-sync-all", (headers) => ({
    method: "POST",
    headers,
    body: JSON.stringify({}),
  }))
  const body = await res.json().catch(() => ({}))
  if (!res.ok) {
    const msg = body?.error ?? body?.detail ?? `Erro ${res.status} ao sincronizar`
    throw new Error(msg)
  }
  return {
    ok: true,
    inserted: body.inserted ?? 0,
    skipped: body.skipped ?? 0,
    deleted: body.deleted ?? 0,
    errors: body.errors,
  }
}
