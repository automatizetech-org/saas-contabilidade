/**
 * Servidor HTTP do módulo WhatsApp — conexão igual ao WhatsApp_emissor.
 * Expõe: GET /status, GET /qr, GET /groups, POST /send (envia apenas para o groupId informado).
 * Uso: node server.js (ou npm run dev:wa a partir da raiz).
 * Pastas (tudo dentro de Servidor/whatsapp-emissor): .wwebjs_auth, .wwebjs_cache, data/
 * NUNCA apagar .wwebjs_auth — é a sessão salva; sem ela sempre pede QR de novo.
 * A sessão é sempre preservada: ao reiniciar, o cliente restaura de .wwebjs_auth e /status volta connected: true sem novo QR.
 * Configure no frontend: WHATSAPP_API=http://localhost:3010
 */

const http = require("http");
const path = require("path");
const fs = require("fs");
require("dotenv").config({ path: path.join(__dirname, ".env") });
const { Client, LocalAuth, MessageMedia } = require("whatsapp-web.js");
const qrImage = require("qr-image");
const { spawnSync } = require("child_process");

const PORT = Number(process.env.WA_SERVER_PORT) || 3010;
const APP_ROOT = path.resolve(__dirname);
// Tudo (auth, cache, data) fica dentro de Servidor/whatsapp-emissor
process.chdir(APP_ROOT);
const DATA_ROOT = process.env.WA_APP_DATA_DIR
  ? path.resolve(process.env.WA_APP_DATA_DIR)
  : path.join(APP_ROOT, "data");
const authFolder = path.join(APP_ROOT, ".wwebjs_auth");
const cacheFolder = path.join(APP_ROOT, ".wwebjs_cache");
const sessionFolder = path.join(authFolder, "session");
const qrFile = path.join(DATA_ROOT, "json", "wa_qr.png");
const pwBrowsersDir = path.join(DATA_ROOT, "ms-playwright");

let client = null;
let isReady = false;
let lastQR = null;
let isStarting = false;
/** Só logar QR_READY uma vez por ciclo (até conectar ou desconectar); evita flood no console. */
let qrLoggedInCycle = false;

// Cache de grupos: resposta imediata após o primeiro getChats (que é lento)
const GROUPS_CACHE_TTL_MS = 90 * 1000; // 90s
let groupsCache = { list: [], at: 0 };
let groupsLoadPromise = null;

function clearGroupsCache() {
  groupsCache = { list: [], at: 0 };
  groupsLoadPromise = null;
}

async function fetchGroupsForCache() {
  if (!client || !isReady) return [];
  const existing = groupsLoadPromise;
  if (existing) return existing;
  const promise = (async () => {
    try {
      const chats = await client.getChats();
      const list = chats
        .filter((c) => c && c.isGroup)
        .map((c) => ({
          id: (c.id && c.id._serialized) || c.id || "",
          name: (typeof c.name === "string" && c.name) ? c.name : "Sem nome",
        }))
        .filter((g) => g.id);
      groupsCache = { list, at: Date.now() };
      return list;
    } catch (e) {
      console.error("[ERRO] Cache de grupos:", e && e.message ? e.message : e);
      return groupsCache.list.length ? groupsCache.list : [];
    } finally {
      groupsLoadPromise = null;
    }
  })();
  groupsLoadPromise = promise;
  return promise;
}

function clearChromeLocks() {
  const rootLocks = ["SingletonLock", "SingletonCookie", "SingletonSocket", "DevToolsActivePort"];
  rootLocks.forEach((name) => {
    const p = path.join(sessionFolder, name);
    try {
      if (fs.existsSync(p)) fs.rmSync(p, { force: true });
    } catch (_) {}
  });
  try {
    if (fs.existsSync(sessionFolder)) {
      const stack = [sessionFolder];
      while (stack.length) {
        const dir = stack.pop();
        let entries = [];
        try {
          entries = fs.readdirSync(dir, { withFileTypes: true });
        } catch (_) {
          continue;
        }
        entries.forEach((e) => {
          const full = path.join(dir, e.name);
          if (e.isDirectory()) stack.push(full);
          else {
            const upper = e.name.toUpperCase();
            if (upper === "LOCK" || upper.startsWith("SINGLETON") || e.name === "DevToolsActivePort") {
              try {
                fs.rmSync(full, { force: true });
              } catch (_) {}
            }
          }
        });
      }
    }
  } catch (_) {}
}

function killOrphanChrome() {
  if (process.platform !== "win32") return;
  const sessionPath = sessionFolder.replace(/'/g, "''");
  const authPath = authFolder.replace(/'/g, "''");
  const ps = [
    "Get-CimInstance Win32_Process |",
    "Where-Object { $_.CommandLine -and ( $_.CommandLine -like '*" + sessionPath + "*' -or $_.CommandLine -like '*" + authPath + "*' ) } |",
    "ForEach-Object { Stop-Process -Id $_.ProcessId -Force }",
  ].join(" ");
  try {
    spawnSync("powershell", ["-NoProfile", "-Command", ps], {
      stdio: "ignore",
      timeout: 6000,
      windowsHide: true,
    });
  } catch (_) {}
}

function resolveBrowserExecutable() {
  try {
    if (!fs.existsSync(pwBrowsersDir)) return undefined;
    const found = [];
    const stack = [pwBrowsersDir];
    while (stack.length) {
      const dir = stack.pop();
      let entries = [];
      try {
        entries = fs.readdirSync(dir, { withFileTypes: true });
      } catch (_) {
        continue;
      }
      entries.forEach((e) => {
        const full = path.join(dir, e.name);
        if (e.isDirectory()) stack.push(full);
        else if (
          e.isFile() &&
          e.name.toLowerCase() === "chrome.exe" &&
          full.includes("chromium-")
        ) {
          found.push(full);
        }
      });
    }
    if (!found.length) return undefined;
    found.sort((a, b) => {
      const ma = /chromium-(\d+)/i.exec(a);
      const mb = /chromium-(\d+)/i.exec(b);
      return (mb ? parseInt(mb[1], 10) : 0) - (ma ? parseInt(ma[1], 10) : 0);
    });
    return found[0];
  } catch (_) {
    return undefined;
  }
}

function buildClient() {
  const executablePath = resolveBrowserExecutable();
  const puppeteerOpts = {
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-gpu",
      "--disable-dev-shm-usage",
    ],
  };
  if (executablePath) puppeteerOpts.executablePath = executablePath;
  const c = new Client({
    authStrategy: new LocalAuth({ dataPath: authFolder }),
    webVersionCache: { type: "local", path: cacheFolder },
    puppeteer: puppeteerOpts,
    restartOnAuthFail: true,
  });

  c.on("qr", (qr) => {
    if (isReady || client !== c) return;
    const png = qrImage.imageSync(qr, {
      type: "png",
      size: 12,
      margin: 3,
      ec_level: "M",
    });
    lastQR = Buffer.isBuffer(png) ? png : Buffer.from(png);
    try {
      const qrDir = path.dirname(qrFile);
      if (!fs.existsSync(qrDir)) fs.mkdirSync(qrDir, { recursive: true });
      fs.writeFileSync(qrFile, png);
      if (!qrLoggedInCycle) {
        qrLoggedInCycle = true;
        console.log("QR_READY");
      }
    } catch (_) {}
  });

  c.on("ready", () => {
    isReady = true;
    qrLoggedInCycle = false; // próximo ciclo de QR poderá logar de novo
    lastQR = null;
    clearGroupsCache();
    try {
      if (fs.existsSync(qrFile)) fs.rmSync(qrFile, { force: true });
    } catch (_) {}
    console.log("[OK] WhatsApp conectado.");
    // Pré-carrega grupos em background para /groups responder na hora
    fetchGroupsForCache()
      .then((list) => console.log("[OK] Grupos em cache:", list.length))
      .catch(() => {});
  });

  c.on("disconnected", async (reason) => {
    isReady = false;
    qrLoggedInCycle = false;
    lastQR = null;
    clearGroupsCache();
    if (client) {
      try {
        await client.destroy();
      } catch (_) {}
      client = null;
    }
    console.log("[!] Desconectado:", reason || "");
  });

  c.on("auth_failure", () => {
    console.log("[!] Falha de autenticação.");
  });

  return c;
}

async function startClient() {
  if (client) return;
  isStarting = true;
  console.log("[INFO] Inicializando cliente WhatsApp...");
  if (fs.existsSync(authFolder)) {
    console.log("[INFO] Sessão salva encontrada — tentando restaurar (sem novo QR se válida).");
  }
  try {
    if (fs.existsSync(qrFile)) fs.rmSync(qrFile, { force: true });
  } catch (_) {}
  // Primeira tentativa: não matar Chrome nem limpar locks — deixa a sessão (LocalAuth) restaurar
  await new Promise((r) => setTimeout(r, 3000));
  client = buildClient();
  try {
    await client.initialize();
  } catch (err) {
    const msg = err && err.message ? err.message : String(err);
    console.error("[ERRO] Falha ao inicializar:", msg);
    client = null;
    const isBrowserAlreadyRunning = /browser is already running|userDataDir/i.test(msg);
    const isContextDestroyed = /Execution context|context was destroyed/i.test(msg);
    if (isBrowserAlreadyRunning || isContextDestroyed) {
      console.log("[INFO] Encerrando Chrome órfão e tentando novamente em 8s...");
      killOrphanChrome();
      clearChromeLocks();
      await new Promise((r) => setTimeout(r, 8000));
    } else if (fs.existsSync(cacheFolder)) {
      try {
        fs.rmSync(cacheFolder, { recursive: true, force: true });
      } catch (_) {}
      killOrphanChrome();
      await new Promise((r) => setTimeout(r, 3000));
    }
    client = buildClient();
    try {
      await client.initialize();
    } catch (e2) {
      client = null;
      console.error("[ERRO] Segunda tentativa falhou:", e2 && e2.message ? e2.message : e2);
    }
  } finally {
    isStarting = false;
  }
}

async function disconnectClient(opts = {}) {
  const clearSession = opts.clearSession === true;
  if (client) {
    try {
      await client.destroy();
    } catch (_) {}
    client = null;
  }
  isReady = false;
  lastQR = null;
  try {
    if (fs.existsSync(qrFile)) fs.rmSync(qrFile, { force: true });
  } catch (_) {}
  if (clearSession) {
    try {
      if (fs.existsSync(authFolder)) fs.rmSync(authFolder, { recursive: true, force: true });
      if (fs.existsSync(cacheFolder)) fs.rmSync(cacheFolder, { recursive: true, force: true });
      console.log("[OK] Cliente desconectado. Sessão e cache apagados — próximo Conectar gerará novo QR.");
    } catch (e) {
      console.warn("[AVISO] Erro ao apagar sessão:", e && e.message ? e.message : e);
    }
  } else {
    console.log("[OK] Cliente desconectado. Sessão mantida para reconectar.");
  }
}

/** Marca o cliente como quebrado (ex.: Frame detachado, context destroyed) para permitir nova conexão e novo QR. */
async function resetBrokenClient() {
  if (!client) return;
  isReady = false;
  lastQR = null;
  clearGroupsCache();
  try {
    await client.destroy();
  } catch (_) {}
  client = null;
  try {
    if (fs.existsSync(qrFile)) fs.rmSync(qrFile, { force: true });
  } catch (_) {}
  console.log("[!] Cliente reiniciado (sessão quebrada). Use Conectar no site para gerar novo QR.");
}

function sendJson(res, statusCode, data) {
  res.writeHead(statusCode, {
    "Content-Type": "application/json",
  });
  res.end(JSON.stringify(data));
}

function applyCors(req, res) {
  const allowed = (process.env.CORS_ORIGINS || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  const origin = req.headers.origin;
  if (origin && allowed.includes(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
  }
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, ngrok-skip-browser-warning");
  res.setHeader("Access-Control-Max-Age", "86400");
}

function requireToken(req, res) {
  const token = String(process.env.WHATSAPP_API_TOKEN || "").trim();
  if (!token) return true; // se não configurado, não bloqueia (evita quebrar ambiente existente)
  const auth = String(req.headers.authorization || "");
  const ok = auth === `Bearer ${token}`;
  if (ok) return true;
  sendJson(res, 401, { error: "Unauthorized" });
  return false;
}

// Rate limit simples em memória (por IP)
const RATE_WINDOW_MS = 60_000;
const RATE_LIMITS = {
  "/send": 30,
  "/connect": 10,
  "/disconnect": 10,
  "/qr": 120,
  "/groups": 60,
  "/status": 300,
};
const ipCounters = new Map();
function rateLimit(req, res, pathname) {
  const limit = RATE_LIMITS[pathname];
  if (!limit) return true;
  const ip = String((req.headers["x-forwarded-for"] || "").split(",")[0].trim() || req.socket.remoteAddress || "unknown");
  const key = `${ip}:${pathname}`;
  const now = Date.now();
  const prev = ipCounters.get(key) || { resetAt: now + RATE_WINDOW_MS, count: 0 };
  if (now > prev.resetAt) {
    prev.resetAt = now + RATE_WINDOW_MS;
    prev.count = 0;
  }
  prev.count += 1;
  ipCounters.set(key, prev);
  if (prev.count <= limit) return true;
  res.writeHead(429, { "Content-Type": "application/json" });
  res.end(JSON.stringify({ error: "Too Many Requests" }));
  return false;
}

const server = http.createServer(async (req, res) => {
  applyCors(req, res);
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Content-Length": "0",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers": "Content-Type, Authorization, ngrok-skip-browser-warning",
      "Access-Control-Max-Age": "86400",
    });
    res.end();
    return;
  }

  const url = req.url || "/";
  const pathname = url.split("?")[0];

  if (pathname === "/status") {
    if (!rateLimit(req, res, pathname)) return;
    if (!requireToken(req, res)) return;
    sendJson(res, 200, { connected: isReady });
    return;
  }

  if (pathname === "/qr") {
    if (!rateLimit(req, res, pathname)) return;
    if (!requireToken(req, res)) return;
    if (isReady) {
      sendJson(res, 200, { qr: null, connected: true });
      return;
    }
    let qrBuffer = lastQR;
    if (!qrBuffer || !Buffer.isBuffer(qrBuffer)) {
      try {
        if (fs.existsSync(qrFile)) {
          const stat = fs.statSync(qrFile);
          const ageMs = Date.now() - stat.mtimeMs;
          if (ageMs < 90000) {
            qrBuffer = fs.readFileSync(qrFile);
            lastQR = qrBuffer;
          }
        }
      } catch (_) {}
    }
    if (!qrBuffer || !Buffer.isBuffer(qrBuffer)) {
      sendJson(res, 200, { qr: null, connected: false });
      return;
    }
    const base64 = qrBuffer.toString("base64");
    sendJson(res, 200, { qr: `data:image/png;base64,${base64}`, connected: false });
    return;
  }

  if (pathname === "/qr.png") {
    if (!rateLimit(req, res, "/qr")) return;
    if (!requireToken(req, res)) return;
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0");
    res.setHeader("Pragma", "no-cache");
    res.setHeader("Expires", "0");
    if (isReady) {
      res.writeHead(204);
      res.end();
      return;
    }
    let qrBuffer = lastQR;
    if (!qrBuffer || !Buffer.isBuffer(qrBuffer)) {
      try {
        if (fs.existsSync(qrFile)) {
          const stat = fs.statSync(qrFile);
          if (Date.now() - stat.mtimeMs < 90000) {
            qrBuffer = fs.readFileSync(qrFile);
            lastQR = qrBuffer;
          }
        }
      } catch (_) {}
    }
    if (!qrBuffer) {
      res.writeHead(204);
      res.end();
      return;
    }
    res.writeHead(200, { "Content-Type": "image/png" });
    res.end(qrBuffer);
    return;
  }

  if (pathname === "/groups") {
    if (!rateLimit(req, res, pathname)) return;
    if (!requireToken(req, res)) return;
    if (!isReady || !client) {
      sendJson(res, 200, { groups: [] });
      return;
    }
    const urlObj = new URL(req.url || "/", `http://localhost:${PORT}`);
    const forceRefresh = urlObj.searchParams.get("refresh") === "1" || urlObj.searchParams.get("refresh") === "true";
    const useCache = !forceRefresh && groupsCache.list.length > 0 && (Date.now() - groupsCache.at < GROUPS_CACHE_TTL_MS);
    if (useCache) {
      sendJson(res, 200, { groups: groupsCache.list });
      return;
    }
    if (forceRefresh) clearGroupsCache();
    try {
      const groups = await fetchGroupsForCache();
      sendJson(res, 200, { groups });
    } catch (e) {
      const msg = (e && e.message) ? e.message : String(e);
      const isBroken = /detached|Execution context|context was destroyed/i.test(msg);
      if (isBroken) {
        resetBrokenClient().then(() => {
          sendJson(res, 200, { groups: [], error: "Sessão quebrada. Clique em Conectar no site para gerar novo QR." });
        });
      } else {
        console.error("[ERRO] Listar grupos:", e);
        sendJson(res, 500, { groups: groupsCache.list.length ? groupsCache.list : [], error: msg || "Erro ao listar grupos" });
      }
    }
    return;
  }

  if (pathname === "/connect" && req.method === "POST") {
    if (!rateLimit(req, res, pathname)) return;
    if (!requireToken(req, res)) return;
    if (client && isReady) {
      sendJson(res, 200, { ok: true, alreadyConnected: true });
      return;
    }
    if (client || isStarting) {
      sendJson(res, 200, { ok: true, starting: true });
      return;
    }
    startClient()
      .then(() => sendJson(res, 200, { ok: true }))
      .catch((e) => sendJson(res, 500, { ok: false, error: (e && e.message) ? e.message : "Falha ao iniciar" }));
    return;
  }

  if (pathname === "/disconnect" && req.method === "POST") {
    if (!rateLimit(req, res, pathname)) return;
    if (!requireToken(req, res)) return;
    const restartOnDisconnect = process.env.WA_RESTART_ON_DISCONNECT === "1";
    disconnectClient({ clearSession: true })
      .then(() => {
        sendJson(res, 200, { ok: true });
        if (restartOnDisconnect) {
          console.log("[INFO] WA_RESTART_ON_DISCONNECT=1 — encerrando processo para PM2 reiniciar e gerar novo QR.");
          setTimeout(() => process.exit(0), 1500);
        } else {
          console.log("[INFO] Iniciando cliente para gerar novo QR após desconexão.");
          startClient().catch((e) => console.error("[ERRO] startClient após disconnect:", e && e.message ? e.message : e));
        }
      })
      .catch(() => {
        sendJson(res, 200, { ok: true });
        if (!restartOnDisconnect) startClient().catch(() => {});
      });
    return;
  }

  if (pathname === "/send" && req.method === "POST") {
    if (!rateLimit(req, res, pathname)) return;
    if (!requireToken(req, res)) return;
    const contentLength = Math.min(
      parseInt(req.headers["content-length"], 10) || 0,
      10 * 1024 * 1024
    ); // máx 10MB
    let body = "";
    let bodyDone = false;
    const sendOnce = (code, data) => {
      if (res.writableEnded) return;
      sendJson(res, code, data);
    };

    const processBody = () => {
      if (bodyDone) return;
      bodyDone = true;
      clearTimeout(bodyTimeout);
      if (contentLength > 0 && body.length > contentLength) body = body.slice(0, contentLength);
      console.log("[SEND] Body completo,", body.length, "bytes");

      try {
        if (!isReady || !client) {
          sendOnce(503, { ok: false, error: "WhatsApp desconectado" });
          return;
        }
        let data;
        try {
          data = JSON.parse(body || "{}");
        } catch (_) {
          sendOnce(400, { ok: false, error: "JSON inválido" });
          return;
        }
        const groupId = (data.groupId || "").trim();
        const message = typeof data.message === "string" ? data.message : "";
        const attachments = Array.isArray(data.attachments) ? data.attachments : [];
        if (!groupId) {
          sendOnce(400, { ok: false, error: "groupId obrigatório" });
          return;
        }
        const rawId = groupId.includes("@") ? groupId.split("@")[0] : groupId;
        const targetId = rawId ? `${rawId.trim()}@g.us` : groupId;
        console.log("[SEND] Aceito: grupo=" + targetId + ", anexos=" + attachments.length);
        sendOnce(200, { ok: true });
        (async () => {
          const delayMs = (ms) => new Promise((r) => setTimeout(r, ms));
          try {
            await client.sendMessage(targetId, message || " ");
            if (attachments.length > 0) {
              await delayMs(1200);
              for (const att of attachments) {
                const mimetype =
                  att.mimetype && typeof att.mimetype === "string"
                    ? att.mimetype
                    : "application/octet-stream";
                const dataBase64 =
                  att.dataBase64 && typeof att.dataBase64 === "string" ? att.dataBase64 : "";
                const filename =
                  att.filename && typeof att.filename === "string" ? att.filename : "documento";
                if (!dataBase64) continue;
                const media = new MessageMedia(mimetype, dataBase64, filename);
                await client.sendMessage(targetId, media);
                await delayMs(800);
              }
            }
            console.log("[SEND] Enviado com sucesso para", targetId);
          } catch (e) {
            console.error("[ERRO] Enviar para grupo (background):", e && e.message ? e.message : e);
          }
        })();
      } catch (err) {
        console.error("[ERRO] /send inesperado:", err && err.message ? err.message : err);
        sendOnce(500, { ok: false, error: "Erro interno" });
      }
    };

    const BODY_TIMEOUT_MS = 45_000;
    const bodyTimeout = setTimeout(() => {
      if (bodyDone) return;
      bodyDone = true;
      console.warn("[SEND] Timeout: body não recebido em", BODY_TIMEOUT_MS / 1000, "s");
      sendOnce(408, { ok: false, error: "Tempo esgotado ao receber os dados. Tente novamente." });
      req.destroy();
    }, BODY_TIMEOUT_MS);

    console.log("[SEND] POST /send recebido, aguardando body (Content-Length:", contentLength, ")...");
    req.on("data", (chunk) => {
      body += chunk;
      if (contentLength > 0 && body.length >= contentLength) processBody();
    });
    req.on("end", () => processBody());
    req.on("error", () => {
      if (!bodyDone) {
        bodyDone = true;
        clearTimeout(bodyTimeout);
      }
    });
    return;
  }

  sendJson(res, 404, { error: "Not found" });
});

server.listen(PORT, () => {
  console.log(`[API] WhatsApp API em http://localhost:${PORT}`);
  console.log("[API] Configure no .env do frontend: WHATSAPP_API=http://localhost:" + PORT);
  startClient().catch((e) => {
    console.error("[ERRO] Cliente não iniciou:", e);
  });
});
