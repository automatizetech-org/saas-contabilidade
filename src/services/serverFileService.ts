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
  a.href = URL.createObjectURL(blob)
  const safeName = String(filename || "")
    .split(/[\\/]/)
    .pop()
    ?.replace(INVALID_DOWNLOAD_NAME_PATTERN, "")
    .trim()
  a.download = safeName || "arquivo"
  a.click()
  URL.revokeObjectURL(a.href)
}

async function getAuthHeaders(contentType?: string) {
  const { data: { session } } = await supabase.auth.getSession()
  if (!session?.access_token) throw new Error("Faça login para continuar.")
  const headers: Record<string, string> = {
    Authorization: `Bearer ${session.access_token}`,
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
  const headers = await getAuthHeaders("application/json")
  const res = await fetchOfficeServer(action, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  })
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

async function fetchServerFileByPath(filePath: string): Promise<{ blob: Blob; filename: string }> {
  const normalizedPath = String(filePath || "").trim()
  if (!normalizedPath) throw new Error("Caminho do arquivo não informado.")

  const headers = await getAuthHeaders("application/json")
  const res = await fetchOfficeServer("download-file", {
    method: "POST",
    headers,
    body: JSON.stringify({ file_path: normalizedPath }),
  })

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

export async function downloadFiscalDocument(documentId: string, suggestedName?: string): Promise<void> {
  const headers = await getAuthHeaders("application/json")
  const res = await fetchOfficeServer("download-fiscal-document", {
    method: "POST",
    headers,
    body: JSON.stringify({ document_id: documentId }),
  })
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

export async function downloadOfficeServerAction(
  action: string,
  payload: Record<string, unknown>,
  suggestedName?: string
): Promise<void> {
  const headers = await getAuthHeaders("application/json")
  const res = await fetchOfficeServer(action, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  })
  if (!res.ok) throw new Error(await readError(res))
  const blob = await res.blob()
  const disposition = res.headers.get("Content-Disposition")
  const filename =
    disposition?.match(/filename="?([^";]+)"?/)?.[1]?.trim() ||
    suggestedName ||
    "arquivo"
  triggerBlobDownload(blob, filename)
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

/**
 * Download rápido: um único request ao servidor, que monta o ZIP em stream no disco.
 * Com dezenas de milhares de arquivos é muito mais rápido que N requests no cliente.
 * onProgress(0-100) opcional para barra de progresso.
 */
export async function downloadListedFilesZipWithCategory(
  items: Array<{ companyName: string; category: string; filePath: string }>,
  suggestedName = "documentos",
  onProgress?: (percent: number) => void
): Promise<void> {
  const normalizedItems = items
    .map((it) => ({
      companyName: String(it.companyName || "EMPRESA").trim() || "EMPRESA",
      category: String(it.category || "outros").trim() || "outros",
      filePath: String(it.filePath || "").trim(),
    }))
    .filter((it) => it.filePath.length > 0)

  if (normalizedItems.length === 0) throw new Error("Nenhum arquivo selecionado para baixar.")
  if (normalizedItems.length > MAX_FILES_CLIENT_ZIP) {
    throw new Error(`Limite de ${MAX_FILES_CLIENT_ZIP} arquivos por download. Selecione menos itens na lista.`)
  }

  if (hasServerApi()) {
    const headers = await getAuthHeaders("application/json")
    const res = await fetchOfficeServer("download-zip-by-paths", {
      method: "POST",
      headers,
      body: JSON.stringify({
        items: normalizedItems.map((it) => ({
          file_path: it.filePath,
          company_name: it.companyName,
          category: it.category,
        })),
        filename_suffix: suggestedName !== "documentos" ? suggestedName.replace(/[^a-z0-9-]/gi, "-").slice(0, 32) : undefined,
      }),
    })
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
    normalizedItems.map(async ({ companyName, category, filePath }) => {
      const { blob, filename } = await fetchServerFileByPath(filePath)
      const safeCompany = companyName.replace(INVALID_DOWNLOAD_NAME_PATTERN, "").replace(/\s+/g, " ").trim() || "EMPRESA"
      const safeCategory = category.replace(INVALID_DOWNLOAD_NAME_PATTERN, "").replace(/\s+/g, " ").trim() || "outros"
      zip.file(makeUniqueZipPath(`${safeCompany}/${safeCategory}/${filename}`), blob)
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
  const headers = await getAuthHeaders("application/json")
  const res = await fetchOfficeServer(action, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  })
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
  const headers = await getAuthHeaders("application/json")
  const res = await fetchOfficeServer("fiscal-sync-all", {
    method: "POST",
    headers,
    body: JSON.stringify({}),
  })
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
