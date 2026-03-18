/**
 * API unificada — Fleury Insights Hub
 * Roda na VM na porta 3001. Atende rotas de arquivos e repassa o restante ao backend WhatsApp.
 * BASE_PATH: lido do Supabase (admin_settings.base_path) na inicialização; fallback para .env BASE_PATH.
 * Configure WHATSAPP_BACKEND_URL, SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY no .env
 */

import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: path.join(__dirname, ".env") });

import express from "express";
import cors from "cors";
import helmet from "helmet";
import rateLimit from "express-rate-limit";
import fs from "fs";
import os from "os";
import archiver from "archiver";
import { createProxyMiddleware } from "http-proxy-middleware";
import { createClient } from "@supabase/supabase-js";

const app = express();
const PORT = process.env.PORT || 3001;

// Necessário quando roda atrás de proxy (ngrok) para rate-limit e IP correto
// (ngrok envia X-Forwarded-For)
app.set("trust proxy", 1);

// Base path: Supabase (admin) na inicialização; fallback para .env
let BASE_PATH = (process.env.BASE_PATH || "C:\\Users\\ROBO\\Documents").trim();

async function loadBasePathFromSupabase() {
  const url = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !serviceKey) return;
  try {
    const supabase = createClient(url, serviceKey);
    const { data } = await supabase.from("admin_settings").select("value").eq("key", "base_path").maybeSingle();
    if (data?.value && String(data.value).trim()) BASE_PATH = String(data.value).trim();
  } catch (_) {}
}

/** Dado path lógico (ex.: FISCAL/NFS), encontra date_rule no nó folha da árvore. */
function findDateRuleByPath(nodes, pathLogical) {
  const parts = pathLogical.split("/").map((p) => p.trim()).filter(Boolean);
  if (parts.length === 0) return null;
  const byParentAndSlug = new Map();
  for (const n of nodes) {
    const slug = (n.slug || n.name || "").toLowerCase();
    const key = `${n.parent_id ?? "root"}:${slug}`;
    byParentAndSlug.set(key, n);
  }
  let parentId = null;
  let node = null;
  for (const part of parts) {
    const key = `${parentId ?? "root"}:${part.toLowerCase()}`;
    node = byParentAndSlug.get(key) ?? null;
    if (!node) return null;
    parentId = node.id;
  }
  return node?.date_rule ?? null;
}

app.disable("x-powered-by");

const allowedOrigins = (process.env.CORS_ORIGINS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const corsOptions = {
  origin: (origin, callback) => {
    // requests sem Origin (curl/health-check) são permitidas
    if (!origin) return callback(null, true);
    if (allowedOrigins.length === 0) return callback(null, false);
    if (allowedOrigins.includes(origin)) return callback(null, true);
    return callback(null, false);
  },
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization", "ngrok-skip-browser-warning"],
};
app.use(cors(corsOptions));

app.use(
  helmet({
    // API JSON — não aplicamos CSP aqui; isso é do frontend (Vercel).
    contentSecurityPolicy: false,
    crossOriginEmbedderPolicy: false,
  })
);

app.use(
  rateLimit({
    windowMs: 60 * 1000,
    limit: 300,
    standardHeaders: true,
    legacyHeaders: false,
  })
);

const heavyLimiter = rateLimit({
  windowMs: 60 * 1000,
  limit: 30,
  standardHeaders: true,
  legacyHeaders: false,
});

function requireBearer(req, res, next) {
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith("Bearer ")) {
    return res.status(401).json({ error: "Token ausente" });
  }
  return next();
}

async function validateSupabaseJwt(req, res, next) {
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_ANON_KEY;
  if (!supabaseUrl || !supabaseKey) {
    return res.status(500).json({ error: "Supabase não configurado" });
  }
  const token = (req.headers.authorization || "").slice(7);
  try {
    const supabase = createClient(supabaseUrl, supabaseKey, {
      global: { headers: { Authorization: `Bearer ${token}` } },
      auth: { persistSession: false, autoRefreshToken: false, detectSessionInUrl: false },
    });
    const { data, error } = await supabase.auth.getUser();
    if (error || !data?.user?.id) {
      return res.status(401).json({ error: "Token inválido" });
    }
    req.user = data.user;
    return next();
  } catch (e) {
    return res.status(401).json({ error: "Token inválido" });
  }
}

// Não consumir o body nas rotas que o proxy repassa ao WhatsApp (senão o backend recebe body vazio e dá 408)
const whatsappPaths = ["/send", "/status", "/groups", "/qr", "/connect", "/disconnect"];
app.use((req, res, next) => {
  const p = req.path || "/";
  const normalized = p.startsWith("/api/") ? p.slice(4) : p; // compat com túnel que expõe apenas /api/*
  const isWhatsApp = whatsappPaths.includes(normalized) || normalized.startsWith("/qr");
  if (isWhatsApp) return next();
  const limit = process.env.BODY_LIMIT || "25mb";
  express.json({ limit })(req, res, (err) => {
    if (err) return next(err);
    express.urlencoded({ extended: true, limit })(req, res, next);
  });
});

// Header para ngrok não bloquear
app.use((req, res, next) => {
  res.setHeader("ngrok-skip-browser-warning", "true");
  next();
});

/**
 * GET /api/files/list?path=EMPRESAS/Grupo Fleury/NFS
 * Lista arquivos (XML, PDF) de uma pasta. Path é relativo a BASE_PATH.
 */
app.get("/api/files/list", requireBearer, validateSupabaseJwt, (req, res) => {
  const relPath = req.query.path;
  if (!relPath || typeof relPath !== "string") {
    return res.status(400).json({ error: "Query 'path' é obrigatória" });
  }
  const fullPath = path.join(BASE_PATH, relPath);
  if (!path.resolve(fullPath).startsWith(path.resolve(BASE_PATH))) {
    return res.status(403).json({ error: "Path fora do diretório base" });
  }
  try {
    const entries = fs.readdirSync(fullPath, { withFileTypes: true });
    const files = entries
      .filter((e) => e.isFile())
      .filter((e) => /\.(xml|pdf)$/i.test(e.name))
      .map((e) => ({
        name: e.name,
        ext: path.extname(e.name).toLowerCase(),
        path: path.join(relPath, e.name).replace(/\\/g, "/"),
      }));
    return res.json({ files });
  } catch (err) {
    if (err.code === "ENOENT") return res.status(404).json({ error: "Pasta não encontrada" });
    return res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/files/download?path=EMPRESAS/Grupo Fleury/NFS/arquivo.xml
 * Baixa um arquivo por path direto (para testes, sem JWT).
 */
app.get("/api/files/download", requireBearer, validateSupabaseJwt, (req, res) => {
  const inputPath = req.query.path;
  if (!inputPath || typeof inputPath !== "string") {
    return res.status(400).json({ error: "Query 'path' é obrigatória" });
  }
  const baseResolved = path.resolve(BASE_PATH);
  const normalizedInput = inputPath.trim();
  const fullPath = path.isAbsolute(normalizedInput)
    ? path.resolve(normalizedInput)
    : path.resolve(path.join(BASE_PATH, normalizedInput));
  if (!fullPath.startsWith(baseResolved)) {
    return res.status(403).json({ error: "Path fora do diretório base" });
  }
  try {
    if (!fs.existsSync(fullPath) || !fs.statSync(fullPath).isFile()) {
      return res.status(404).json({ error: "Arquivo não encontrado" });
    }
    const filename = path.basename(fullPath);
    const ext = path.extname(filename).toLowerCase();
    const contentType = ext === ".xml" ? "application/xml" : ext === ".pdf" ? "application/pdf" : "application/octet-stream";
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    res.setHeader("Content-Type", contentType);
    fs.createReadStream(fullPath).pipe(res);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/fiscal-documents/:id/download
 * Baixa arquivo fiscal por ID (busca file_path no Supabase). Requer JWT.
 */
app.get("/api/fiscal-documents/:id/download", async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith("Bearer ")) {
    return res.status(401).json({ error: "Token ausente" });
  }
  const token = authHeader.slice(7);
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_ANON_KEY;
  if (!supabaseUrl || !supabaseKey) {
    return res.status(500).json({ error: "Supabase não configurado" });
  }
  const supabase = createClient(supabaseUrl, supabaseKey, {
    global: { headers: { Authorization: `Bearer ${token}` } },
  });
  const { data: doc, error } = await supabase
    .from("fiscal_documents")
    .select("file_path")
    .eq("id", req.params.id)
    .single();
  if (error || !doc?.file_path) {
    return res.status(404).json({ error: "Documento não encontrado" });
  }
  const fullPath = path.join(BASE_PATH, doc.file_path);
  if (!path.resolve(fullPath).startsWith(path.resolve(BASE_PATH))) {
    return res.status(403).json({ error: "Path inválido" });
  }
  try {
    if (!fs.existsSync(fullPath) || !fs.statSync(fullPath).isFile()) {
      return res.status(404).json({ error: "Arquivo não encontrado no disco" });
    }
    const filename = path.basename(fullPath);
    const ext = path.extname(filename).toLowerCase();
    const contentType = ext === ".xml" ? "application/xml" : ext === ".pdf" ? "application/pdf" : "application/octet-stream";
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    res.setHeader("Content-Type", contentType);
    fs.createReadStream(fullPath).pipe(res);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/**
 * POST /api/fiscal-documents/download-zip
 * Cria um ZIP temporário na VM com os arquivos dos documentos solicitados (mesma lista/filtro da tela),
 * envia o ZIP na resposta e apaga o arquivo temporário em seguida.
 * Body: { ids: string[] }. Requer JWT.
 */
app.post("/api/fiscal-documents/download-zip", requireBearer, validateSupabaseJwt, heavyLimiter, async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith("Bearer ")) {
    return res.status(401).json({ error: "Token ausente" });
  }
  const token = authHeader.slice(7);
  const ids = Array.isArray(req.body?.ids) ? req.body.ids.filter((id) => id && String(id).trim()) : [];
  const companyIds = Array.isArray(req.body?.company_ids)
    ? req.body.company_ids.filter((id) => id && String(id).trim())
    : [];
  const types = Array.isArray(req.body?.types) ? req.body.types.map((t) => String(t || "").trim().toUpperCase()).filter(Boolean) : [];
  if (companyIds.length === 0 && ids.length === 0) {
    return res.status(400).json({ error: "Nenhum documento/empresa selecionado para baixar." });
  }
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_ANON_KEY;
  if (!supabaseUrl || !supabaseKey) {
    return res.status(500).json({ error: "Supabase não configurado" });
  }
  const supabase = createClient(supabaseUrl, supabaseKey, {
    global: { headers: { Authorization: `Bearer ${token}` } },
  });
  // Não depende de relacionamento (FK) fiscal_documents -> companies (pode não existir no schema cache).
  let q = supabase.from("fiscal_documents").select("id, file_path, company_id");
  if (companyIds.length > 0) q = q.in("company_id", companyIds);
  else q = q.in("id", ids);
  if (types.length > 0) q = q.in("type", types);
  const { data: rows, error } = await q;
  if (error) {
    return res.status(500).json({ error: error.message });
  }
  const docs = (rows || []).filter((r) => r?.file_path && String(r.file_path).trim());
  const companyIdsInDocs = [...new Set(docs.map((d) => d.company_id).filter(Boolean))];
  const companyNameById = new Map();
  if (companyIdsInDocs.length > 0) {
    try {
      const { data: companies, error: companiesErr } = await supabase
        .from("companies")
        .select("id, name")
        .in("id", companyIdsInDocs);
      if (!companiesErr && Array.isArray(companies)) {
        for (const c of companies) {
          if (c?.id) companyNameById.set(c.id, String(c.name || "").trim());
        }
      }
    } catch (_) {}
  }
  const baseResolved = path.resolve(BASE_PATH);
  const toAdd = [];
  for (const doc of docs) {
    const fullPath = path.join(BASE_PATH, doc.file_path);
    if (!path.resolve(fullPath).startsWith(baseResolved)) continue;
    if (!fs.existsSync(fullPath) || !fs.statSync(fullPath).isFile()) continue;
    const filePathNormalized = String(doc.file_path || "").replace(/\\/g, "/");
    const parts = filePathNormalized.split("/").filter(Boolean);
    let companyFolder = "EMPRESA";
    let restParts = parts;
    if (parts.length >= 2 && parts[0].toLowerCase() === "empresas") {
      companyFolder = parts[1];
      restParts = parts.slice(2);
    } else if (parts.length >= 1) {
      companyFolder = parts[0];
      restParts = parts.slice(1);
    }
    const companyNameFromDb = companyNameById.get(doc.company_id) || "";
    if (String(companyNameFromDb || "").trim()) companyFolder = String(companyNameFromDb).trim();
    const safeCompany = companyFolder
      .replace(/[<>:"/\\|?*\u0000-\u001F]/g, "")
      .replace(/\s+/g, " ")
      .trim() || "EMPRESA";
    // Dentro da pasta da empresa, separar por tipo (igual ao seletor do app).
    const typeUpper = String(doc.type || "").trim().toUpperCase();
    const categoryFolder = typeUpper === "NFS" ? "nfs" : (typeUpper === "NFE" || typeUpper === "NFC") ? "nfe-nfc" : "fiscal";
    const zipFilename = path.basename(filePathNormalized);
    const zipPath = `${safeCompany}/${categoryFolder}/${zipFilename}`;
    toAdd.push({ fullPath, zipPath });
  }
  if (toAdd.length === 0) {
    return res.status(404).json({ error: "Nenhum arquivo encontrado no disco para os documentos solicitados." });
  }
  const usedNames = new Set();
  const makeUniqueName = (zipPath) => {
    let n = zipPath;
    let i = 0;
    while (usedNames.has(n)) {
      i++;
      const ext = path.posix.extname(zipPath);
      const base = zipPath.slice(0, zipPath.length - ext.length);
      n = `${base} (${i})${ext}`;
    }
    usedNames.add(n);
    return n;
  };
  res.setHeader("Content-Type", "application/zip");
  const suffix = typeof req.body?.filename_suffix === "string" ? req.body.filename_suffix.trim() : "";
  const safeSuffix = suffix && /^[a-z0-9-]+$/i.test(suffix) ? `-${suffix}` : "";
  res.setHeader("Content-Disposition", `attachment; filename="documentos-fiscais${safeSuffix}.zip"`);
  // STORE (level 0): mais rápido, menor CPU. Mantém downloads rápidos.
  const archive = archiver("zip", { zlib: { level: 0 } });
  archive.on("error", (err) => {
    if (!res.headersSent) res.status(500).json({ error: err.message });
  });
  // Se o cliente (ngrok/browser) abortar, interrompe o zip para não “ficar pendurado” até o final.
  res.on("close", () => {
    try {
      archive.abort();
    } catch (_) {}
  });
  archive.pipe(res);
  for (const { fullPath, zipPath } of toAdd) {
    archive.file(fullPath, { name: makeUniqueName(zipPath) });
  }
  archive.finalize();
});

/**
 * POST /api/hub-documents/download-zip
 * Baixa um ZIP unificado com TODOS os documentos do hub (certidões, notas, guias/taxas/impostos),
 * organizando por Empresa/<categoria>/<arquivo>.
 * Body: { company_ids: string[], categories?: string[], filename_suffix?: string }
 */
app.post("/api/hub-documents/download-zip", requireBearer, validateSupabaseJwt, heavyLimiter, async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith("Bearer ")) {
    return res.status(401).json({ error: "Token ausente" });
  }
  const token = authHeader.slice(7);
  const companyIds = Array.isArray(req.body?.company_ids)
    ? req.body.company_ids.filter((id) => id && String(id).trim())
    : [];
  if (companyIds.length === 0) {
    return res.status(400).json({ error: "Nenhuma empresa selecionada para baixar." });
  }
  const categoriesRequested = Array.isArray(req.body?.categories)
    ? req.body.categories.map((c) => String(c || "").trim().toLowerCase()).filter(Boolean)
    : [];
  const allowAll = categoriesRequested.length === 0;
  const allow = (key) => allowAll || categoriesRequested.includes(String(key || "").toLowerCase());

  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_ANON_KEY;
  if (!supabaseUrl || !supabaseKey) {
    return res.status(500).json({ error: "Supabase não configurado" });
  }
  const supabase = createClient(supabaseUrl, supabaseKey, {
    global: { headers: { Authorization: `Bearer ${token}` } },
  });

  // Nome da empresa para pasta
  const companyNameById = new Map();
  try {
    const { data: companies } = await supabase.from("companies").select("id, name").in("id", companyIds);
    for (const c of companies || []) {
      if (c?.id) companyNameById.set(c.id, String(c.name || "").trim());
    }
  } catch (_) {}

  const baseResolved = path.resolve(BASE_PATH);
  const toAdd = [];

  const safeCompanyFolder = (companyId, fallbackFromPath) => {
    let companyFolder = String(companyNameById.get(companyId) || "").trim() || String(fallbackFromPath || "EMPRESA");
    return companyFolder
      .replace(/[<>:"/\\|?*\u0000-\u001F]/g, "")
      .replace(/\s+/g, " ")
      .trim() || "EMPRESA";
  };

  const addFile = (companyId, categoryFolder, fileRelPath) => {
    const filePathNormalized = String(fileRelPath || "").replace(/\\/g, "/").trim();
    if (!filePathNormalized) return;
    const fullPath = path.join(BASE_PATH, filePathNormalized);
    if (!path.resolve(fullPath).startsWith(baseResolved)) return;
    if (!fs.existsSync(fullPath) || !fs.statSync(fullPath).isFile()) return;
    const parts = filePathNormalized.split("/").filter(Boolean);
    let fallbackCompany = "EMPRESA";
    if (parts.length >= 2 && parts[0].toLowerCase() === "empresas") fallbackCompany = parts[1];
    else if (parts.length >= 1) fallbackCompany = parts[0];
    const safeCompany = safeCompanyFolder(companyId, fallbackCompany);
    const zipFilename = path.basename(filePathNormalized);
    const safeCategory = String(categoryFolder || "outros")
      .replace(/[<>:"/\\|?*\u0000-\u001F]/g, "")
      .replace(/\s+/g, " ")
      .trim() || "outros";
    const zipPath = `${safeCompany}/${safeCategory}/${zipFilename}`;
    toAdd.push({ fullPath, zipPath });
  };

  // Notas (fiscal_documents)
  if (allow("nfs") || allow("nfe-nfc") || allow("fiscal")) {
    const { data: fiscalRows } = await supabase
      .from("fiscal_documents")
      .select("company_id, type, file_path")
      .in("company_id", companyIds)
      .not("file_path", "is", null);
    for (const r of fiscalRows || []) {
      const typeUpper = String(r.type || "").trim().toUpperCase();
      const cat = typeUpper === "NFS" ? "nfs" : (typeUpper === "NFE" || typeUpper === "NFC") ? "nfe-nfc" : "fiscal";
      if (!allow(cat) && !allow("fiscal")) continue;
      addFile(r.company_id, cat, r.file_path);
    }
  }

  // Certidões (sync_events payload.arquivo_pdf)
  if (allow("certidoes") || allow("certidões")) {
    const { data: syncRows } = await supabase
      .from("sync_events")
      .select("company_id, payload, created_at")
      .eq("tipo", "certidao_resultado")
      .in("company_id", companyIds)
      .order("created_at", { ascending: false })
      .limit(5000);
    for (const r of syncRows || []) {
      let payload = {};
      try { payload = JSON.parse(r.payload || "{}"); } catch { payload = {}; }
      const p = String(payload.arquivo_pdf || "").trim();
      if (!p) continue;
      addFile(r.company_id, "certidoes", p);
    }
  }

  // Guias / taxas (dp_guias.file_path)
  if (allow("taxas e impostos") || allow("taxas_impostos") || allow("taxas") || allow("impostos")) {
    const { data: guiaRows } = await supabase
      .from("dp_guias")
      .select("company_id, file_path")
      .in("company_id", companyIds)
      .not("file_path", "is", null);
    for (const r of guiaRows || []) {
      addFile(r.company_id, "taxas e impostos", r.file_path);
    }
  }

  // Impostos municipais (municipal_tax_debts.guia_pdf_path)
  if (allow("taxas e impostos") || allow("taxas_impostos") || allow("taxas") || allow("impostos")) {
    const { data: muniRows } = await supabase
      .from("municipal_tax_debts")
      .select("company_id, guia_pdf_path")
      .in("company_id", companyIds)
      .not("guia_pdf_path", "is", null);
    for (const r of muniRows || []) {
      addFile(r.company_id, "taxas e impostos", r.guia_pdf_path);
    }
  }

  if (toAdd.length === 0) {
    return res.status(404).json({ error: "Nenhum arquivo encontrado no disco para as empresas solicitadas." });
  }

  const usedNames = new Set();
  const makeUniqueName = (zipPath) => {
    let n = zipPath;
    let i = 0;
    while (usedNames.has(n)) {
      i++;
      const ext = path.posix.extname(zipPath);
      const base = zipPath.slice(0, zipPath.length - ext.length);
      n = `${base} (${i})${ext}`;
    }
    usedNames.add(n);
    return n;
  };

  res.setHeader("Content-Type", "application/zip");
  const suffix = typeof req.body?.filename_suffix === "string" ? req.body.filename_suffix.trim() : "";
  const safeSuffix = suffix && /^[a-z0-9-]+$/i.test(suffix) ? `-${suffix}` : "";
  res.setHeader("Content-Disposition", `attachment; filename="documentos-hub${safeSuffix}.zip"`);
  const archive = archiver("zip", { zlib: { level: 0 } });
  archive.on("error", (err) => {
    if (!res.headersSent) res.status(500).json({ error: err.message });
  });
  res.on("close", () => {
    try { archive.abort(); } catch (_) {}
  });
  archive.pipe(res);
  for (const { fullPath, zipPath } of toAdd) {
    archive.file(fullPath, { name: makeUniqueName(zipPath) });
  }
  archive.finalize();
});

/**
 * POST /api/fiscal-sync
 * Sincroniza arquivos de uma pasta para fiscal_documents.
 * Body: { path, company_id, type }
 * Requer Authorization: Bearer <jwt_do_usuario> — usa só anon key; RLS valida permissão.
 */
app.post("/api/fiscal-sync", requireBearer, validateSupabaseJwt, heavyLimiter, async (req, res) => {
  const { path: relPath, company_id, type = "NFS" } = req.body || {};
  if (!relPath || !company_id) {
    return res.status(400).json({ error: "path e company_id são obrigatórios" });
  }
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith("Bearer ")) {
    return res.status(401).json({ error: "Token ausente. Envie Authorization: Bearer <jwt>." });
  }
  const token = authHeader.slice(7);
  const fullPath = path.join(BASE_PATH, relPath);
  if (!path.resolve(fullPath).startsWith(path.resolve(BASE_PATH))) {
    return res.status(403).json({ error: "Path fora do diretório base" });
  }
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_ANON_KEY;
  if (!supabaseUrl || !supabaseKey) {
    return res.status(500).json({ error: "Supabase não configurado (SUPABASE_URL e SUPABASE_ANON_KEY)" });
  }
  const supabase = createClient(supabaseUrl, supabaseKey, {
    global: { headers: { Authorization: `Bearer ${token}` } },
  });
  try {
    const entries = fs.readdirSync(fullPath, { withFileTypes: true });
    const files = entries
      .filter((e) => e.isFile())
      .filter((e) => /\.(xml|pdf)$/i.test(e.name));
    const periodo = new Date().toISOString().slice(0, 7);
    const inserted = [];
    const errors = [];
    for (const f of files) {
      const fileRelPath = path.join(relPath, f.name).replace(/\\/g, "/");
      const chave = path.basename(f.name, path.extname(f.name));
      const { data: existingRows } = await supabase
        .from("fiscal_documents")
        .select("id")
        .eq("company_id", company_id)
        .eq("file_path", fileRelPath)
        .limit(1);
      if (existingRows && existingRows.length > 0) continue;
      const { data, error } = await supabase
        .from("fiscal_documents")
        .insert({
          company_id,
          type: type.toUpperCase(),
          chave,
          periodo,
          status: "novo",
          file_path: fileRelPath,
        })
        .select("id");
      const row = Array.isArray(data) ? data[0] : data;
      if (!error && row?.id) inserted.push({ id: row.id, name: f.name });
      else if (error?.code === "23505") { /* duplicata, ignorar */ }
      else if (error) errors.push({ name: f.name, error: error.message });
    }
    return res.json({
      found: files.length,
      inserted: inserted.length,
      files: inserted,
      errors: errors.length ? errors : undefined,
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/**
 * POST /api/fiscal-sync-all
 * Escaneia BASE_PATH/EMPRESAS, associa cada pasta ao company_id pelo nome da empresa,
 * e sincroniza arquivos XML/PDF de FISCAL/NFS/Recebidas e FISCAL/NFS/Emitidas para fiscal_documents.
 * Requer Authorization: Bearer <jwt>.
 * Normaliza nomes (remove acentos/cedilha) para casar pasta "SERVICOS" com empresa "SERVIÇOS".
 */
function normalizeCompanyName(name) {
  if (typeof name !== "string") return "";
  return name
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "");
}

function walkDir(dir, baseDir) {
  const results = [];
  const fullDir = path.join(baseDir, dir);
  if (!fs.existsSync(fullDir) || !fs.statSync(fullDir).isDirectory()) return results;
  const entries = fs.readdirSync(fullDir, { withFileTypes: true });
  for (const e of entries) {
    const rel = path.join(dir, e.name).replace(/\\/g, "/");
    if (e.isDirectory()) {
      results.push(...walkDir(rel, baseDir));
    } else if (e.isFile() && /\.(xml|pdf)$/i.test(e.name)) {
      results.push(rel);
    }
  }
  return results;
}

/**
 * Executa a sincronização completa EMPRESAS -> fiscal_documents.
 * Inclui remoção: registros cujo arquivo não existe mais na pasta são removidos do banco.
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase - Cliente Supabase (JWT do usuário ou service role)
 * @returns {{ inserted: number, skipped: number, deleted: number, errors: Array<{ file: string, error: string }> }}
 */
async function runFiscalSyncAll(supabase) {
  const result = { inserted: 0, skipped: 0, deleted: 0, errors: [] };
  const empresasPath = BASE_PATH;
  const empresasExists = fs.existsSync(empresasPath) && fs.statSync(empresasPath).isDirectory();
  const allPathsOnDisk = empresasExists ? new Set(walkDir("", BASE_PATH)) : new Set();

  const { data: companies } = await supabase.from("companies").select("id, name");
  const nameToId = new Map((companies || []).map((c) => [normalizeCompanyName(c.name), c.id]));
  /** Por company_id: Set de file_path que existem no disco. Inicializa para todas as empresas (vazio se pasta não existir). */
  const pathsOnDiskByCompany = new Map((companies || []).map((c) => [c.id, new Set()]));

  if (!empresasExists) {
    // Pasta base não existe: espelhar “zerando” para qualquer documento salvo no fiscal_documents.
    const { data: rows } = await supabase
      .from("fiscal_documents")
      .select("id, file_path")
      .not("file_path", "is", null);
    const idsToDelete = (rows || []).map((r) => r.id);
    if (idsToDelete.length > 0) {
      const { error: deleteError } = await supabase.from("fiscal_documents").delete().in("id", idsToDelete);
      if (!deleteError) result.deleted += idsToDelete.length;
    }
    return result;
  }

  const companyDirs = fs.readdirSync(empresasPath, { withFileTypes: true }).filter((e) => e.isDirectory());

  for (const companyDir of companyDirs) {
    const companyName = companyDir.name;
    const companyId = nameToId.get(normalizeCompanyName(companyName));
    if (!companyId) continue;
    const pathsOnDisk = pathsOnDiskByCompany.get(companyId);

    for (const sub of ["Recebidas", "Emitidas"]) {
      const segment = path.join(companyName, "FISCAL", "NFS", sub).replace(/\\/g, "/");
      const files = walkDir(segment, BASE_PATH);
      for (const fileRel of files) {
        pathsOnDisk.add(fileRel);
        const chave = path.basename(fileRel, path.extname(fileRel));
        const parts = fileRel.split(/[/\\]/);
        let periodo = new Date().toISOString().slice(0, 7);
        const y = parts.find((p) => /^\d{4}$/.test(p));
        const m = parts.find((p) => /^\d{2}$/.test(p) && parseInt(p, 10) >= 1 && parseInt(p, 10) <= 12);
        if (y && m) periodo = `${y}-${m}`;
        const { data: existingRows } = await supabase
          .from("fiscal_documents")
          .select("id")
          .eq("company_id", companyId)
          .eq("file_path", fileRel)
          .limit(1);
        if (existingRows && existingRows.length > 0) {
          result.skipped++;
          continue;
        }
        const { error } = await supabase.from("fiscal_documents").insert({
          company_id: companyId,
          type: "NFS",
          chave,
          periodo,
          status: "novo",
          file_path: fileRel,
        });
        if (error) {
          if (error.code === "23505") {
            result.skipped++;
          } else {
            result.errors.push({ file: fileRel, error: error.message });
          }
        } else {
          result.inserted++;
        }
      }
    }
  }

  // Espelhamento genérico: remover do banco todo registro cujo arquivo não existe mais no disco.
  const { data: rowsToMirrorDelete } = await supabase
    .from("fiscal_documents")
    .select("id, file_path")
    .not("file_path", "is", null);
  const idsToDelete = (rowsToMirrorDelete || [])
    .filter((r) => !allPathsOnDisk.has(r.file_path))
    .map((r) => r.id);
  if (idsToDelete.length > 0) {
    const { error: deleteError } = await supabase.from("fiscal_documents").delete().in("id", idsToDelete);
    if (!deleteError) result.deleted += idsToDelete.length;
  }

  return result;
}

app.post("/api/fiscal-sync-all", requireBearer, validateSupabaseJwt, heavyLimiter, async (req, res) => {
  const authHeader = req.headers.authorization;
  if (!authHeader?.startsWith("Bearer ")) {
    return res.status(401).json({ error: "Token ausente. Envie Authorization: Bearer <jwt>." });
  }
  const token = authHeader.slice(7);
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_ANON_KEY;
  if (!supabaseUrl || !supabaseKey) {
    return res.status(500).json({
      error: "Supabase não configurado no server-api. No .env da VM defina SUPABASE_URL e SUPABASE_ANON_KEY.",
    });
  }
  const supabase = createClient(supabaseUrl, supabaseKey, {
    global: { headers: { Authorization: `Bearer ${token}` } },
  });
  try {
    const { inserted, skipped, deleted, errors } = await runFiscalSyncAll(supabase);
    return res.json({ ok: true, inserted, skipped, deleted: deleted ?? 0, errors: errors.length ? errors : undefined });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/health", (req, res) => res.json({ ok: true }));

/**
 * GET /api/folder-structure
 * Retorna a árvore de pastas (flat) para robôs montarem o path.
 * Path na VM: BASE_PATH/EMPRESAS/{nome_empresa}/{segmentos do nó}
 * Leitura pública (anon) para robôs sem JWT.
 */
app.get("/api/folder-structure", async (req, res) => {
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_ANON_KEY;
  if (!supabaseUrl || !supabaseKey) {
    return res.status(500).json({ error: "Supabase não configurado" });
  }
  try {
    const supabase = createClient(supabaseUrl, supabaseKey);
    const { data, error } = await supabase
      .from("folder_structure_nodes")
      .select("id, parent_id, name, slug, date_rule, position")
      .order("parent_id", { nullsFirst: true })
      .order("position", { ascending: true });
    if (error) return res.status(500).json({ error: error.message });
    return res.json({ nodes: data ?? [] });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/robot-config?technical_id=xxx
 * Retorna configuração para o robô na VM: base_path (global), segment_path e date_rule do robô.
 * Robôs usam isso em vez de BASE_PATH e ROBOT_SEGMENT_PATH no .env (que passam a ser opcionais).
 */
app.get("/api/robot-config", async (req, res) => {
  const technicalId = (req.query.technical_id || "").toString().trim();
  if (!technicalId) {
    return res.status(400).json({ error: "technical_id é obrigatório" });
  }
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_ANON_KEY;
  if (!supabaseUrl || !supabaseKey) {
    return res.status(500).json({ error: "Supabase não configurado" });
  }
  try {
    const supabase = createClient(supabaseUrl, supabaseKey);
    const { data: robot, error: robotErr } = await supabase
      .from("robots")
      .select("segment_path, notes_mode")
      .eq("technical_id", technicalId)
      .maybeSingle();
    if (robotErr) return res.status(500).json({ error: robotErr.message });
    const segmentPath = (robot?.segment_path || "").trim() || null;
    const notesMode = (robot?.notes_mode || "").trim() || null;
    const { data: nodes, error: nodesErr } = await supabase
      .from("folder_structure_nodes")
      .select("id, parent_id, name, slug, date_rule, position")
      .order("parent_id", { nullsFirst: true })
      .order("position", { ascending: true });
    if (nodesErr) return res.status(500).json({ error: nodesErr.message });
    const dateRule = segmentPath ? findDateRuleByPath(nodes ?? [], segmentPath) : null;
    return res.json({
      base_path: BASE_PATH,
      segment_path: segmentPath,
      date_rule: dateRule,
      notes_mode: notesMode,
      folder_structure: nodes ?? [],
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// Proxy WhatsApp (somente rotas explícitas; evita expor catch-all)
const WHATSAPP_BACKEND_URL = process.env.WHATSAPP_BACKEND_URL || "http://localhost:3010";
const whatsappProxy = createProxyMiddleware({
  target: WHATSAPP_BACKEND_URL,
  changeOrigin: true,
  proxyTimeout: 60_000,
  timeout: 60_000,
  onError: (err, req, res) => {
    res.status(502).json({ error: "Backend WhatsApp indisponível", detail: err.message });
  },
});
const whatsappProxyViaApiPrefix = createProxyMiddleware({
  pathFilter: ["/api/send", "/api/status", "/api/groups", "/api/qr", "/api/connect", "/api/disconnect"],
  target: WHATSAPP_BACKEND_URL,
  changeOrigin: true,
  proxyTimeout: 60_000,
  timeout: 60_000,
  pathRewrite: (pathReq) => (pathReq.startsWith("/api/") ? pathReq.slice(4) : pathReq),
  onError: (err, req, res) => {
    res.status(502).json({ error: "Backend WhatsApp indisponível", detail: err.message });
  },
});
app.use(["/send", "/status", "/groups", "/qr", "/connect", "/disconnect"], whatsappProxy);
// Compat: alguns túneis/reverse-proxy só expõem /api/* para fora
app.use(whatsappProxyViaApiPrefix);

app.use((req, res) => {
  res.status(404).json({ error: "Rota não encontrada" });
});

/** Monitoramento automático: quando novos arquivos chegam em EMPRESAS, sincroniza com Supabase. */
function startFiscalWatcher() {
  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!supabaseUrl || !serviceKey) {
    console.log("[fiscal-watcher] SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY ausente; monitoramento automático desligado.");
    return;
  }
  const empresasPath = BASE_PATH;
  if (!fs.existsSync(empresasPath) || !fs.statSync(empresasPath).isDirectory()) {
    console.log("[fiscal-watcher] Pasta base da VM não encontrada; monitoramento desligado.");
    return;
  }
  const supabase = createClient(supabaseUrl, serviceKey);
  let debounceTimer = null;
  const DEBOUNCE_MS = 4000;

  const runSync = () => {
    runFiscalSyncAll(supabase)
      .then(({ inserted, skipped, deleted, errors }) => {
        if (inserted > 0 || deleted > 0 || errors.length > 0) {
          console.log(`[fiscal-watcher] Sync: ${inserted} inseridos, ${skipped} já existentes${deleted ? `, ${deleted} removidos` : ""}${errors.length ? `, ${errors.length} erros` : ""}`);
        }
      })
      .catch((err) => console.error("[fiscal-watcher] Erro ao sincronizar:", err.message));
  };

  try {
    fs.watch(empresasPath, { recursive: true }, (eventType, filename) => {
      if (!filename || !/\.(xml|pdf)$/i.test(filename)) return;
      if (debounceTimer) clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => {
        debounceTimer = null;
        runSync();
      }, DEBOUNCE_MS);
    });
    console.log("[fiscal-watcher] Monitorando a pasta base da VM — novos XML/PDF serão sincronizados automaticamente.");
  } catch (err) {
    console.error("[fiscal-watcher] Não foi possível monitorar a pasta base da VM:", err.message);
  }
}

loadBasePathFromSupabase().then(() => {
  app.listen(PORT, () => {
    console.log(`API unificada em http://localhost:${PORT}`);
    console.log(`BASE_PATH: ${BASE_PATH}`);
    console.log(`Proxy WhatsApp: ${WHATSAPP_BACKEND_URL}`);
    startFiscalWatcher();
  });
});


