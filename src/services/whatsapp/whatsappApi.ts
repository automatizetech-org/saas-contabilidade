/**
 * Cliente da API WhatsApp (QR, grupos, envio).
 * Servidor: Servidor/whatsapp-emissor/server.js — mesma lógica de conexão do WhatsApp_emissor.
 * Base URL: WHATSAPP_API (ex: http://localhost:3010)
 * Endpoints: GET /status, GET /qr, GET /groups, POST /send { groupId, message } — envia apenas para o grupo selecionado.
 */

const BASE = (import.meta as unknown as { env?: { WHATSAPP_API?: string } }).env?.WHATSAPP_API ?? "";

/** Headers para requisições. Inclui header do ngrok para pular página de aviso quando a base for ngrok. */
function getHeaders(extra?: HeadersInit): HeadersInit {
  const base = BASE.toLowerCase();
  const isNgrok = base.includes("ngrok");
  return {
    ...(extra as object),
    ...(isNgrok ? { "ngrok-skip-browser-warning": "true" } : {}),
  };
}

function joinUrl(base: string, path: string): string {
  const b = base.replace(/\/$/, "");
  const p = path.startsWith("/") ? path : `/${path}`;
  return `${b}${p}`;
}

async function fetchWithApiFallback(path: string, init: RequestInit): Promise<Response> {
  const res = await fetch(joinUrl(BASE, path), init);
  if (res.status !== 404) return res;
  // Compat: quando o túnel só libera /api/*
  return fetch(joinUrl(BASE, `/api${path.startsWith("/") ? path : `/${path}`}`), init);
}

export interface WhatsAppGroup {
  id: string;
  name: string;
}

export interface ConnectionStatus {
  connected: boolean;
}

export async function getConnectionStatus(): Promise<ConnectionStatus> {
  if (!BASE) return { connected: false };
  try {
    const res = await fetchWithApiFallback("/status", { method: "GET", headers: getHeaders(), cache: "no-store" });
    if (!res.ok) return { connected: false };
    const contentType = res.headers.get("content-type") || "";
    if (!contentType.includes("application/json")) return { connected: false };
    const data = await res.json().catch(() => null);
    const connected = data != null && (data.connected === true || data.connected === "true");
    return { connected: !!connected };
  } catch {
    return { connected: false };
  }
}

/** Retorna a imagem do QR atual (base64 data URL). Retorna null se já conectado ou se o backend ainda não tem QR. Não usa qr.png para evitar bloqueio e excesso de requisições. */
export async function getQrImage(): Promise<string | null> {
  if (!BASE) return null;
  try {
    const res = await fetchWithApiFallback("/qr", { method: "GET", cache: "no-store", headers: getHeaders() });
    if (!res.ok) return null;
    const data = await res.json();
    if (data?.connected) return null;
    const qr = data?.qr ?? data?.image ?? null;
    if (typeof qr === "string" && qr.startsWith("data:image/") && qr.length >= 200) return qr;
    return null;
  } catch {
    return null;
  }
}

/** URL do QR em PNG (sem cache). */
export function getQrImageUrl(): string {
  if (!BASE) return "";
  return `${BASE.replace(/\/$/, "")}/qr.png?t=${Date.now()}`;
}

/** URL do QR com timestamp controlado (para refresh periódico no img). */
export function getQrImageUrlWithTimestamp(ts: number): string {
  if (!BASE) return "";
  return `${BASE.replace(/\/$/, "")}/qr.png?t=${ts}`;
}

export async function getGroups(forceRefresh = false): Promise<WhatsAppGroup[]> {
  if (!BASE) return [];
  try {
    const path = forceRefresh ? "/groups?refresh=1" : "/groups";
    const res = await fetchWithApiFallback(path, { method: "GET", headers: getHeaders() });
    if (!res.ok) return [];
    const data = await res.json();
    return Array.isArray(data?.groups) ? data.groups : [];
  } catch {
    return [];
  }
}

export interface WhatsAppAttachment {
  filename: string;
  mimetype: string;
  dataBase64: string;
}

export async function sendToGroup(
  groupId: string,
  message: string,
  attachments?: WhatsAppAttachment[]
): Promise<{ ok: boolean; error?: string }> {
  if (!BASE) return { ok: false, error: "API não configurada" };
  const controller = new AbortController();
  const timeoutMs = 90_000; // 90s para envio com anexos
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const body: { groupId: string; message: string; attachments?: WhatsAppAttachment[] } = {
      groupId,
      message,
    };
    if (attachments && attachments.length > 0) body.attachments = attachments;
    const res = await fetchWithApiFallback("/send", {
      method: "POST",
      headers: getHeaders({ "Content-Type": "application/json" }),
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      const msg = (err as { error?: string }).error ?? "Falha ao enviar";
      return { ok: false, error: res.status === 408 ? "Demorou para conectar na API. Tente de novo." : msg };
    }
    return { ok: true };
  } catch (e) {
    if (e instanceof Error) {
      if (e.name === "AbortError") return { ok: false, error: "Tempo esgotado. Verifique a conexão com a API WhatsApp e tente novamente." };
      return { ok: false, error: e.message };
    }
    return { ok: false, error: "Erro de conexão" };
  } finally {
    clearTimeout(timeoutId);
  }
}

/** Conectar WhatsApp (inicia o cliente; sessão é mantida). */
export async function connectWhatsApp(): Promise<{ ok: boolean; error?: string }> {
  if (!BASE) return { ok: false, error: "API não configurada" };
  try {
    const res = await fetchWithApiFallback("/connect", { method: "POST", headers: getHeaders() });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) return { ok: false, error: (data as { error?: string }).error ?? "Falha ao conectar" };
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Erro de conexão" };
  }
}

/** Desconectar WhatsApp (mantém a sessão para reconectar depois). */
export async function disconnectWhatsApp(): Promise<{ ok: boolean; error?: string }> {
  if (!BASE) return { ok: false, error: "API não configurada" };
  try {
    const res = await fetchWithApiFallback("/disconnect", { method: "POST", headers: getHeaders() });
    if (!res.ok) {
      const data = await res.json().catch(() => ({}));
      return { ok: false, error: (data as { error?: string }).error ?? "Falha ao desconectar" };
    }
    return { ok: true };
  } catch (e) {
    return { ok: false, error: e instanceof Error ? e.message : "Erro de conexão" };
  }
}
