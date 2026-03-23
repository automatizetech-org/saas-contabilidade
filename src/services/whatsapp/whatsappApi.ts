/**
 * Cliente WhatsApp — dois modos:
 * 1) Recomendado: via Supabase Edge `office-server` (escritório + JWT + conector).
 *    Ative com Supabase configurado e VITE_WHATSAPP_VIA_OFFICE_SERVER !== "false".
 * 2) Legado: URL direta WHATSAPP_API (túnel até a porta 3010) — exige token no emissor.
 */

import {
  hasServerApi,
  invokeOfficeServerWhatsApp,
} from "@/services/serverFileService"

const DIRECT_BASE = (
  import.meta as unknown as { env?: { WHATSAPP_API?: string } }
).env?.WHATSAPP_API ?? ""

const VITE_WA_OFFICE = (
  import.meta as unknown as { env?: { VITE_WHATSAPP_VIA_OFFICE_SERVER?: string } }
).env?.VITE_WHATSAPP_VIA_OFFICE_SERVER

function useOfficeWhatsapp(): boolean {
  if (VITE_WA_OFFICE === "false" || VITE_WA_OFFICE === "0") return false
  return hasServerApi()
}

function getHeaders(extra?: HeadersInit): HeadersInit {
  const base = DIRECT_BASE.toLowerCase()
  const isNgrok = base.includes("ngrok")
  return {
    ...(extra as object),
    ...(isNgrok ? { "ngrok-skip-browser-warning": "true" } : {}),
  }
}

function joinUrl(base: string, path: string): string {
  const b = base.replace(/\/$/, "")
  const p = path.startsWith("/") ? path : `/${path}`
  return `${b}${p}`
}

async function fetchWithApiFallback(
  path: string,
  init: RequestInit,
): Promise<Response> {
  const res = await fetch(joinUrl(DIRECT_BASE, path), init)
  if (res.status !== 404) return res
  return fetch(
    joinUrl(DIRECT_BASE, `/api${path.startsWith("/") ? path : `/${path}`}`),
    init,
  )
}

export interface WhatsAppGroup {
  id: string
  name: string
}

export interface ConnectionStatus {
  connected: boolean
  /** office-server 401 — não insistir em polling */
  sessionUnauthorized?: boolean
}

async function officeJson(
  call: string,
  options?: { query?: string; payload?: Record<string, unknown> },
): Promise<{ res: Response; data: unknown }> {
  const res = await invokeOfficeServerWhatsApp(call, options)
  const data = await res.json().catch(() => null)
  return { res, data }
}

export async function getConnectionStatus(): Promise<ConnectionStatus> {
  if (useOfficeWhatsapp()) {
    try {
      const { res, data } = await officeJson("status")
      if (!res.ok) {
        if (res.status === 401) {
          return { connected: false, sessionUnauthorized: true }
        }
        return { connected: false }
      }
      const d = data as { connected?: boolean } | null
      const connected =
        d != null && (d.connected === true || (d as { connected?: string }).connected === "true")
      return { connected: !!connected }
    } catch {
      return { connected: false }
    }
  }
  if (!DIRECT_BASE) return { connected: false }
  try {
    const res = await fetchWithApiFallback("/status", {
      method: "GET",
      headers: getHeaders(),
      cache: "no-store",
    })
    if (!res.ok) return { connected: false }
    const contentType = res.headers.get("content-type") || ""
    if (!contentType.includes("application/json")) return { connected: false }
    const data = await res.json().catch(() => null)
    const connected =
      data != null &&
      (data.connected === true || data.connected === "true")
    return { connected: !!connected }
  } catch {
    return { connected: false }
  }
}

export async function getQrImage(): Promise<string | null> {
  if (useOfficeWhatsapp()) {
    try {
      const { res, data } = await officeJson("qr")
      if (!res.ok) return null
      const d = data as {
        connected?: boolean
        qr?: string
        image?: string
      } | null
      if (d?.connected) return null
      const qr = d?.qr ?? d?.image ?? null
      if (typeof qr === "string" && qr.startsWith("data:image/") && qr.length >= 200)
        return qr
      return null
    } catch {
      return null
    }
  }
  if (!DIRECT_BASE) return null
  try {
    const res = await fetchWithApiFallback("/qr", {
      method: "GET",
      cache: "no-store",
      headers: getHeaders(),
    })
    if (!res.ok) return null
    const data = await res.json()
    if (data?.connected) return null
    const qr = data?.qr ?? data?.image ?? null
    if (typeof qr === "string" && qr.startsWith("data:image/") && qr.length >= 200)
      return qr
    return null
  } catch {
    return null
  }
}

export function getQrImageUrl(): string {
  if (useOfficeWhatsapp()) return ""
  if (!DIRECT_BASE) return ""
  return `${DIRECT_BASE.replace(/\/$/, "")}/qr.png?t=${Date.now()}`
}

export function getQrImageUrlWithTimestamp(ts: number): string {
  if (useOfficeWhatsapp()) return ""
  if (!DIRECT_BASE) return ""
  return `${DIRECT_BASE.replace(/\/$/, "")}/qr.png?t=${ts}`
}

export async function getGroups(forceRefresh = false): Promise<WhatsAppGroup[]> {
  if (useOfficeWhatsapp()) {
    try {
      const { res, data } = await officeJson("groups", {
        query: forceRefresh ? "refresh=1" : undefined,
      })
      if (!res.ok) return []
      const d = data as { groups?: WhatsAppGroup[] } | null
      return Array.isArray(d?.groups) ? d.groups : []
    } catch {
      return []
    }
  }
  if (!DIRECT_BASE) return []
  try {
    const path = forceRefresh ? "/groups?refresh=1" : "/groups"
    const res = await fetchWithApiFallback(path, {
      method: "GET",
      headers: getHeaders(),
    })
    if (!res.ok) return []
    const data = await res.json()
    return Array.isArray(data?.groups) ? data.groups : []
  } catch {
    return []
  }
}

export interface WhatsAppAttachment {
  filename: string
  mimetype: string
  dataBase64: string
}

export async function sendToGroup(
  groupId: string,
  message: string,
  attachments?: WhatsAppAttachment[],
): Promise<{ ok: boolean; error?: string }> {
  if (useOfficeWhatsapp()) {
    try {
      const res = await invokeOfficeServerWhatsApp("send", {
        payload: {
          groupId,
          message,
          ...(attachments && attachments.length > 0 ? { attachments } : {}),
        },
      })
      const data = (await res.json().catch(() => ({}))) as {
        ok?: boolean
        error?: string
      }
      if (!res.ok) {
        const msg =
          data.error ??
          (res.status === 408
            ? "Demorou para conectar na API. Tente de novo."
            : "Falha ao enviar")
        return { ok: false, error: msg }
      }
      if (data.ok === false)
        return { ok: false, error: data.error ?? "Falha ao enviar" }
      return { ok: true }
    } catch (e) {
      return {
        ok: false,
        error: e instanceof Error ? e.message : "Erro de conexão",
      }
    }
  }

  if (!DIRECT_BASE) return { ok: false, error: "API não configurada" }
  const controller = new AbortController()
  const timeoutMs = 90_000
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs)
  try {
    const body: {
      groupId: string
      message: string
      attachments?: WhatsAppAttachment[]
    } = { groupId, message }
    if (attachments && attachments.length > 0) body.attachments = attachments
    const res = await fetchWithApiFallback("/send", {
      method: "POST",
      headers: getHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(body),
      signal: controller.signal,
    })
    if (!res.ok) {
      const err = await res.json().catch(() => ({}))
      const msg =
        (err as { error?: string }).error ?? "Falha ao enviar"
      return {
        ok: false,
        error: res.status === 408 ? "Demorou para conectar na API. Tente de novo." : msg,
      }
    }
    return { ok: true }
  } catch (e) {
    if (e instanceof Error) {
      if (e.name === "AbortError")
        return {
          ok: false,
          error:
            "Tempo esgotado. Verifique a conexão com a API WhatsApp e tente novamente.",
        }
      return { ok: false, error: e.message }
    }
    return { ok: false, error: "Erro de conexão" }
  } finally {
    clearTimeout(timeoutId)
  }
}

export async function connectWhatsApp(): Promise<{ ok: boolean; error?: string }> {
  if (useOfficeWhatsapp()) {
    try {
      const { res, data } = await officeJson("connect")
      const d = data as {
        ok?: boolean
        error?: string
        detail?: string
      } | null
      if (!res.ok) {
        const raw = d?.error ?? ""
        const detail = typeof d?.detail === "string" ? d.detail.trim() : ""
        if (res.status === 401) {
          return {
            ok: false,
            error:
              detail ||
              raw ||
              "Sessão não autorizada na API. Atualize a página ou faça login novamente.",
          }
        }
        if (res.status === 403) {
          return {
            ok: false,
            error:
              raw && raw.length > 0
                ? raw
                : "Sem permissão para WhatsApp (alteração empresarial) neste escritório.",
          }
        }
        return { ok: false, error: raw || "Falha ao conectar" }
      }
      return { ok: true }
    } catch (e) {
      return { ok: false, error: e instanceof Error ? e.message : "Erro de conexão" }
    }
  }
  if (!DIRECT_BASE) return { ok: false, error: "API não configurada" }
  try {
    const res = await fetchWithApiFallback("/connect", {
      method: "POST",
      headers: getHeaders(),
    })
    const data = await res.json().catch(() => ({}))
    if (!res.ok)
      return { ok: false, error: (data as { error?: string }).error ?? "Falha ao conectar" }
    return { ok: true }
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Erro de conexão" }
  }
}

export async function disconnectWhatsApp(): Promise<{ ok: boolean; error?: string }> {
  if (useOfficeWhatsapp()) {
    try {
      const { res, data } = await officeJson("disconnect")
      const d = data as { error?: string } | null
      if (!res.ok) return { ok: false, error: d?.error ?? "Falha ao desconectar" }
      return { ok: true }
    } catch (e) {
      return { ok: false, error: e instanceof Error ? e.message : "Erro de conexão" }
    }
  }
  if (!DIRECT_BASE) return { ok: false, error: "API não configurada" }
  try {
    const res = await fetchWithApiFallback("/disconnect", {
      method: "POST",
      headers: getHeaders(),
    })
    if (!res.ok) {
      const data = await res.json().catch(() => ({}))
      return {
        ok: false,
        error: (data as { error?: string }).error ?? "Falha ao desconectar",
      }
    }
    return { ok: true }
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Erro de conexão" }
  }
}

/**
 * Envio genérico (futuras features no SaaS): channel + targets.
 * O emissor aceita POST /deliver com o mesmo payload normalizado.
 */
export async function deliverWhatsApp(
  payload: Record<string, unknown>,
): Promise<{ ok: boolean; error?: string }> {
  if (!useOfficeWhatsapp()) {
    if (!DIRECT_BASE)
      return { ok: false, error: "Configure o escritório (office-server) ou WHATSAPP_API" }
    try {
      const res = await fetchWithApiFallback("/deliver", {
        method: "POST",
        headers: getHeaders({ "Content-Type": "application/json" }),
        body: JSON.stringify(payload),
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        return { ok: false, error: (err as { error?: string }).error ?? "Falha" }
      }
      return { ok: true }
    } catch (e) {
      return { ok: false, error: e instanceof Error ? e.message : "Erro" }
    }
  }
  try {
    const res = await invokeOfficeServerWhatsApp("deliver", { payload })
    const data = (await res.json().catch(() => ({}))) as {
      ok?: boolean
      error?: string
    }
    if (!res.ok)
      return { ok: false, error: data.error ?? "Falha" }
    if (data.ok === false) return { ok: false, error: data.error }
    return { ok: true }
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Erro" }
  }
}
