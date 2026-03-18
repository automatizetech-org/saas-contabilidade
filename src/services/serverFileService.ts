/**
 * Serviço para download de arquivos (XMLs, guias, etc.) via API do seu servidor.
 * Os arquivos ficam no servidor (onde os robôs rodam); o Supabase guarda só metadados.
 * Ver docs/SERVER_FILES_API.md para o contrato da API no servidor.
 */

import JSZip from "jszip";
import { supabase } from "./supabaseClient";

// Normaliza: remove trailing "/" e também um "/api" no final se o usuário configurou assim.
const SERVER_API_URL = (import.meta.env.SERVER_API_URL ?? "")
  .toString()
  .trim()
  .replace(/\/$/, "")
  .replace(/\/api$/i, "");

export function getServerApiUrl(): string {
  return SERVER_API_URL;
}

export function hasServerApi(): boolean {
  return SERVER_API_URL.length > 0;
}

function triggerBlobDownload(blob: Blob, filename: string) {
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  // Nunca deixar o browser interpretar "caminho" como nome do arquivo.
  const safeName = String(filename || "")
    .split(/[\\/]/)
    .pop()
    ?.replace(/[<>:"/\\|?*\u0000-\u001F]/g, "")
    .trim();
  a.download = safeName || "arquivo";
  a.click();
  URL.revokeObjectURL(a.href);
}

async function fetchServerFileByPath(filePath: string): Promise<{ blob: Blob; filename: string }> {
  if (!SERVER_API_URL) {
    throw new Error("SERVER_API_URL não configurada.");
  }
  const normalizedPath = String(filePath || "").trim();
  if (!normalizedPath) {
    throw new Error("Caminho do arquivo não informado.");
  }
  const { data: { session } } = await supabase.auth.getSession();
  if (!session?.access_token) {
    throw new Error("Faça login para baixar o arquivo.");
  }
  const url = new URL(`${SERVER_API_URL}/api/files/download`);
  url.searchParams.set("path", normalizedPath);
  const headers: Record<string, string> = { Authorization: `Bearer ${session.access_token}` };
  if (SERVER_API_URL.toLowerCase().includes("ngrok")) {
    headers["ngrok-skip-browser-warning"] = "true";
  }
  const res = await fetch(url.toString(), {
    method: "GET",
    headers,
  });
  if (!res.ok) {
    if (res.status === 404) throw new Error("Arquivo não encontrado no servidor.");
    if (res.status === 403) throw new Error("Sem permissão para baixar este arquivo.");
    throw new Error(`Erro ao baixar arquivo: ${res.status}`);
  }
  const blob = await res.blob();
  const disposition = res.headers.get("Content-Disposition");
  const rawName =
    disposition?.match(/filename="?([^";]+)"?/)?.[1]?.trim() ||
    normalizedPath.split(/[\\/]/).pop() ||
    "arquivo.pdf";
  const filename = String(rawName).split(/[\\/]/).pop()?.trim() || "arquivo.pdf";
  return { blob, filename };
}

/**
 * Baixa o XML de um documento fiscal via API do servidor.
 * O servidor valida o JWT e devolve o arquivo do disco.
 */
export async function downloadFiscalDocument(documentId: string, suggestedName?: string): Promise<void> {
  if (!SERVER_API_URL) {
    console.warn("SERVER_API_URL não configurada; download não disponível.");
    return;
  }
  const { data: { session } } = await supabase.auth.getSession();
  if (!session?.access_token) {
    throw new Error("Faça login para baixar o arquivo.");
  }
  const url = `${SERVER_API_URL}/api/fiscal-documents/${encodeURIComponent(documentId)}/download`;
  const headers: Record<string, string> = { Authorization: `Bearer ${session.access_token}` };
  if (SERVER_API_URL.toLowerCase().includes("ngrok")) headers["ngrok-skip-browser-warning"] = "true";
  const res = await fetch(url, {
    method: "GET",
    headers,
  });
  if (!res.ok) {
    if (res.status === 403) throw new Error("Sem permissão para baixar este documento.");
    if (res.status === 404) throw new Error("Documento ou arquivo não encontrado.");
    throw new Error(`Erro ao baixar: ${res.status}`);
  }
  const blob = await res.blob();
  const disposition = res.headers.get("Content-Disposition");
  const filename =
    disposition?.match(/filename="?([^";]+)"?/)?.[1]?.trim() ||
    suggestedName ||
    `documento-${documentId}.xml`;
  triggerBlobDownload(blob, filename);
}

export async function downloadServerFileByPath(filePath: string, suggestedName?: string): Promise<void> {
  const { blob, filename } = await fetchServerFileByPath(filePath);
  triggerBlobDownload(blob, suggestedName || filename);
}

export async function downloadServerFilesZip(filePaths: string[], suggestedName = "guias-municipais"): Promise<void> {
  const paths = filePaths.map((filePath) => String(filePath || "").trim()).filter(Boolean);
  if (paths.length === 0) {
    throw new Error("Nenhum arquivo selecionado para baixar.");
  }

  const zip = new JSZip();
  const usedPaths = new Set<string>();
  const makeUniqueZipPath = (zipPath: string) => {
    let candidate = zipPath;
    let i = 0;
    while (usedPaths.has(candidate)) {
      i += 1;
      const dotIndex = zipPath.lastIndexOf(".");
      const base = dotIndex >= 0 ? zipPath.slice(0, dotIndex) : zipPath;
      const ext = dotIndex >= 0 ? zipPath.slice(dotIndex) : "";
      candidate = `${base} (${i})${ext}`;
    }
    usedPaths.add(candidate);
    return candidate;
  };

  await Promise.all(
    paths.map(async (filePath) => {
      const { blob, filename } = await fetchServerFileByPath(filePath);
      const normalized = String(filePath || "").replace(/\\/g, "/").replace(/^\/+/, "");
      const parts = normalized.split("/").filter(Boolean);
      let company = "EMPRESA";
      let restParts: string[] = [];
      if (parts.length >= 2 && parts[0].toLowerCase() === "empresas") {
        company = parts[1] || company;
        restParts = parts.slice(2);
      } else if (parts.length >= 1) {
        company = parts[0] || company;
        restParts = parts.slice(1);
      }
      // Regra do produto: dentro da pasta da empresa ficam os arquivos (sem replicar árvore da VM).
      const zipPath = `${company}/${filename}`;
      zip.file(makeUniqueZipPath(zipPath), blob);
    })
  );

  const zipBlob = await zip.generateAsync({ type: "blob", compression: "STORE" });
  triggerBlobDownload(zipBlob, `${suggestedName}.zip`);
}

/** ZIP no browser com subpasta por empresa + categoria (não depende do server). */
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
    .filter((it) => it.filePath.length > 0);
  if (normalizedItems.length === 0) {
    throw new Error("Nenhum arquivo selecionado para baixar.");
  }

  const zip = new JSZip();
  const usedPaths = new Set<string>();
  const makeUniqueZipPath = (zipPath: string) => {
    let candidate = zipPath;
    let i = 0;
    while (usedPaths.has(candidate)) {
      i += 1;
      const dotIndex = zipPath.lastIndexOf(".");
      const base = dotIndex >= 0 ? zipPath.slice(0, dotIndex) : zipPath;
      const ext = dotIndex >= 0 ? zipPath.slice(dotIndex) : "";
      candidate = `${base} (${i})${ext}`;
    }
    usedPaths.add(candidate);
    return candidate;
  };

  await Promise.all(
    normalizedItems.map(async ({ companyName, category, filePath }) => {
      const { blob, filename } = await fetchServerFileByPath(filePath);
      const safeCompany = companyName.replace(/[<>:"/\\|?*\u0000-\u001F]/g, "").replace(/\s+/g, " ").trim() || "EMPRESA";
      const safeCategory = category.replace(/[<>:"/\\|?*\u0000-\u001F]/g, "").replace(/\s+/g, " ").trim() || "outros";
      zip.file(makeUniqueZipPath(`${safeCompany}/${safeCategory}/${filename}`), blob);
    })
  );

  const zipBlob = await zip.generateAsync({ type: "blob", compression: "STORE" });
  triggerBlobDownload(zipBlob, `${suggestedName}.zip`);
}

/** Marca o documento fiscal como baixado (atualiza last_downloaded_at para retenção). */
export async function markFiscalDocumentDownloaded(documentId: string): Promise<void> {
  const { error } = await supabase
    .from("fiscal_documents")
    .update({ last_downloaded_at: new Date().toISOString() })
    .eq("id", documentId);
  if (error) console.warn("Não foi possível atualizar last_downloaded_at:", error.message);
}

/**
 * Baixa vários documentos fiscais em um único ZIP.
 * A VM cria um ZIP temporário com os arquivos da lista solicitada, envia na resposta e apaga o temp em seguida.
 * @param ids - IDs dos documentos
 * @param filenameSuffix - Sufixo do nome do arquivo (ex.: "nfs", "nfe-nfc"); o download será documentos-fiscais-{suffix}.zip
 */
export async function downloadFiscalDocumentsZip(ids: string[], filenameSuffix?: string): Promise<void> {
  if (!SERVER_API_URL) {
    throw new Error("SERVER_API_URL não configurada.");
  }
  const idsFiltered = ids.filter((id) => id && String(id).trim());
  if (idsFiltered.length === 0) {
    throw new Error("Nenhum documento selecionado para baixar.");
  }
  const { data: { session } } = await supabase.auth.getSession();
  if (!session?.access_token) {
    throw new Error("Faça login para baixar.");
  }
  const url = `${SERVER_API_URL}/api/fiscal-documents/download-zip`;
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${session.access_token}`,
  };
  if (SERVER_API_URL.toLowerCase().includes("ngrok")) {
    headers["ngrok-skip-browser-warning"] = "true";
  }
  const res = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify({ ids: idsFiltered, filename_suffix: filenameSuffix ?? "" }),
  });

  const contentType = res.headers.get("content-type") || "";
  if (contentType.includes("text/html")) {
    const text = await res.text();
    const msg = text.length > 200 ? text.slice(0, 200) + "…" : text;
    throw new Error(
      "A resposta veio em HTML em vez do ZIP. Verifique se SERVER_API_URL no .env aponta para a URL da API (ex.: do ngrok), não para a página do app. Detalhe: " + msg
    );
  }

  if (!res.ok) {
    let message = `Erro ${res.status} ao baixar ZIP`;
    const text = await res.text();
    try {
      const json = JSON.parse(text);
      if (json && typeof json.error === "string") message = json.error;
    } catch {
      if (text && !text.startsWith("<")) message = text.slice(0, 150);
    }
    throw new Error(message);
  }

  if (!contentType.includes("application/zip") && !contentType.includes("application/octet-stream")) {
    throw new Error("Resposta não é um ZIP (content-type: " + contentType + "). Verifique SERVER_API_URL.");
  }

  const safeSuffix = filenameSuffix && /^[a-z0-9-]+$/i.test(filenameSuffix) ? filenameSuffix : "";
  const zipFilename = safeSuffix ? `documentos-fiscais-${safeSuffix}.zip` : "documentos-fiscais.zip";

  const blob = await res.blob();
  triggerBlobDownload(blob, zipFilename);

  for (const id of idsFiltered) {
    markFiscalDocumentDownloaded(id).catch(() => {});
  }
}

/**
 * Baixa todos os documentos fiscais de uma lista de empresas em um único ZIP.
 * Isso evita payload gigante com milhares de IDs de documentos.
 */
export async function downloadFiscalCompaniesZip(companyIds: string[], filenameSuffix?: string, types?: string[]): Promise<void> {
  if (!SERVER_API_URL) {
    throw new Error("SERVER_API_URL não configurada.");
  }
  const companyIdsFiltered = [...new Set(companyIds.map((id) => String(id || "").trim()).filter(Boolean))];
  if (companyIdsFiltered.length === 0) {
    throw new Error("Nenhuma empresa selecionada para baixar.");
  }
  const { data: { session } } = await supabase.auth.getSession();
  if (!session?.access_token) {
    throw new Error("Faça login para baixar.");
  }
  const url = `${SERVER_API_URL}/api/fiscal-documents/download-zip`;
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${session.access_token}`,
  };
  if (SERVER_API_URL.toLowerCase().includes("ngrok")) {
    headers["ngrok-skip-browser-warning"] = "true";
  }
  const typesFiltered = Array.isArray(types)
    ? [...new Set(types.map((t) => String(t || "").trim().toUpperCase()).filter(Boolean))]
    : [];

  const res = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify({
      company_ids: companyIdsFiltered,
      types: typesFiltered,
      filename_suffix: filenameSuffix ?? "",
    }),
  });

  const contentType = res.headers.get("content-type") || "";
  if (!res.ok) {
    let message = `Erro ${res.status} ao baixar ZIP`;
    const text = await res.text();
    try {
      const json = JSON.parse(text);
      if (json && typeof json.error === "string") message = json.error;
    } catch {
      if (text && !text.startsWith("<")) message = text.slice(0, 150);
    }
    throw new Error(message);
  }
  if (!contentType.includes("application/zip") && !contentType.includes("application/octet-stream")) {
    throw new Error("Resposta não é um ZIP (content-type: " + contentType + "). Verifique SERVER_API_URL.");
  }
  const safeSuffix = filenameSuffix && /^[a-z0-9-]+$/i.test(filenameSuffix) ? filenameSuffix : "";
  const zipFilename = safeSuffix ? `documentos-fiscais-${safeSuffix}.zip` : "documentos-fiscais.zip";
  const blob = await res.blob();
  triggerBlobDownload(blob, zipFilename);
}

/** Baixa ZIP unificado do Hub (Empresa/<categoria>/<arquivo>). */
export async function downloadHubCompaniesZip(companyIds: string[], filenameSuffix?: string, categories?: string[]): Promise<void> {
  if (!SERVER_API_URL) {
    throw new Error("SERVER_API_URL não configurada.");
  }
  const companyIdsFiltered = [...new Set(companyIds.map((id) => String(id || "").trim()).filter(Boolean))];
  if (companyIdsFiltered.length === 0) {
    throw new Error("Nenhuma empresa selecionada para baixar.");
  }
  const { data: { session } } = await supabase.auth.getSession();
  if (!session?.access_token) {
    throw new Error("Faça login para baixar.");
  }
  const url = `${SERVER_API_URL}/api/hub-documents/download-zip`;
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${session.access_token}`,
  };
  if (SERVER_API_URL.toLowerCase().includes("ngrok")) {
    headers["ngrok-skip-browser-warning"] = "true";
  }
  const categoriesFiltered = Array.isArray(categories)
    ? [...new Set(categories.map((c) => String(c || "").trim()).filter(Boolean))]
    : [];

  const res = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify({
      company_ids: companyIdsFiltered,
      categories: categoriesFiltered,
      filename_suffix: filenameSuffix ?? "",
    }),
  });

  const contentType = res.headers.get("content-type") || "";
  if (!res.ok) {
    let message = `Erro ${res.status} ao baixar ZIP`;
    const text = await res.text();
    try {
      const json = JSON.parse(text);
      if (json && typeof json.error === "string") message = json.error;
    } catch {
      if (text && !text.startsWith("<")) message = text.slice(0, 150);
    }
    throw new Error(message);
  }
  if (!contentType.includes("application/zip") && !contentType.includes("application/octet-stream")) {
    throw new Error("Resposta não é um ZIP (content-type: " + contentType + "). Verifique SERVER_API_URL.");
  }
  const safeSuffix = filenameSuffix && /^[a-z0-9-]+$/i.test(filenameSuffix) ? filenameSuffix : "";
  const zipFilename = safeSuffix ? `documentos-hub-${safeSuffix}.zip` : "documentos-hub.zip";
  const blob = await res.blob();
  triggerBlobDownload(blob, zipFilename);
}

/**
 * Sincroniza todos os arquivos fiscais da pasta EMPRESAS na VM para fiscal_documents (Supabase).
 * Usa o JWT da sessão atual. Chamar ao abrir Fiscal/Documentos ou ao clicar em "Sincronizar".
 */
export async function fiscalSyncAll(): Promise<{ ok: boolean; inserted: number; skipped: number; deleted: number; errors?: Array<{ file: string; error: string }> }> {
  if (!SERVER_API_URL) {
    throw new Error("SERVER_API_URL não configurada.");
  }
  const { data: { session } } = await supabase.auth.getSession();
  if (!session?.access_token) {
    throw new Error("Faça login para sincronizar.");
  }
  const headers: Record<string, string> = { "Content-Type": "application/json", Authorization: `Bearer ${session.access_token}` };
  if (SERVER_API_URL.toLowerCase().includes("ngrok")) headers["ngrok-skip-browser-warning"] = "true";
  const res = await fetch(`${SERVER_API_URL}/api/fiscal-sync-all`, { method: "POST", headers });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = body?.error ?? body?.message ?? `Erro ${res.status} ao sincronizar`;
    throw new Error(msg);
  }
  return { ok: true, inserted: body.inserted ?? 0, skipped: body.skipped ?? 0, deleted: body.deleted ?? 0, errors: body.errors };
}
