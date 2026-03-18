import JSZip from "jszip"
import { supabase } from "./supabaseClient"

const SUPABASE_URL = (import.meta.env.SUPABASE_URL ?? "").toString().trim().replace(/\/$/, "")
const SUPABASE_ANON_KEY = (import.meta.env.SUPABASE_ANON_KEY ?? "").toString().trim()

function triggerBlobDownload(blob: Blob, filename: string) {
  const a = document.createElement("a")
  a.href = URL.createObjectURL(blob)
  const safeName = String(filename || "")
    .split(/[\\/]/)
    .pop()
    ?.replace(/[<>:"/\\|?*\u0000-\u001F]/g, "")
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

export async function downloadServerFilesZip(filePaths: string[], suggestedName = "guias-municipais"): Promise<void> {
  const paths = filePaths.map((filePath) => String(filePath || "").trim()).filter(Boolean)
  if (paths.length === 0) throw new Error("Nenhum arquivo selecionado para baixar.")

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

export async function downloadListedFilesZipWithCategory(
  items: Array<{ companyName: string; category: string; filePath: string }>,
  suggestedName = "documentos"
): Promise<void> {
  const normalizedItems = items
    .map((it) => ({
      companyName: String(it.companyName || "EMPRESA").trim() || "EMPRESA",
      category: String(it.category || "outros").trim() || "outros",
      filePath: String(it.filePath || "").trim(),
    }))
    .filter((it) => it.filePath.length > 0)

  if (normalizedItems.length === 0) throw new Error("Nenhum arquivo selecionado para baixar.")

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
    normalizedItems.map(async ({ companyName, category, filePath }) => {
      const { blob, filename } = await fetchServerFileByPath(filePath)
      const safeCompany = companyName.replace(/[<>:"/\\|?*\u0000-\u001F]/g, "").replace(/\s+/g, " ").trim() || "EMPRESA"
      const safeCategory = category.replace(/[<>:"/\\|?*\u0000-\u001F]/g, "").replace(/\s+/g, " ").trim() || "outros"
      zip.file(makeUniqueZipPath(`${safeCompany}/${safeCategory}/${filename}`), blob)
    })
  )

  const zipBlob = await zip.generateAsync({ type: "blob", compression: "STORE" })
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
  filename: string
): Promise<void> {
  const headers = await getAuthHeaders("application/json")
  const res = await fetchOfficeServer(action, {
    method: "POST",
    headers,
    body: JSON.stringify(payload),
  })
  if (!res.ok) throw new Error(await readError(res))
  const blob = await res.blob()
  triggerBlobDownload(blob, filename)
}

export async function downloadFiscalDocumentsZip(ids: string[], filenameSuffix?: string): Promise<void> {
  const idsFiltered = ids.filter((id) => id && String(id).trim())
  if (idsFiltered.length === 0) throw new Error("Nenhum documento selecionado para baixar.")
  const safeSuffix = filenameSuffix && /^[a-z0-9-]+$/i.test(filenameSuffix) ? `-${filenameSuffix}` : ""
  await downloadZipAction("download-fiscal-documents-zip", { ids: idsFiltered }, `documentos-fiscais${safeSuffix}.zip`)
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
