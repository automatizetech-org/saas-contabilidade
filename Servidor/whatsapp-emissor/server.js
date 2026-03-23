/**
 * API HTTP WhatsApp — genérica para o SaaS (QR, grupos, envio).
 *
 * Contrato estável (evite mudar rotas):
 *   GET  /status | /qr | /qr.png | /groups
 *   POST /connect | /disconnect
 *   POST /send        { groupId, message?, attachments?[] }
 *   POST /deliver     { channel:"whatsapp", targets:[{type:"group",id}], message?, attachments?[] }
 *
 * Auth: Bearer = CONNECTOR_SECRET em claro (Servidor/.env), ou SHA-256 hex UTF-8 desse valor
 * (igual office_server_credentials.secret_hash / header que chega no server-api), ou WHATSAPP_API_TOKEN (legado).
 *
 * Escritório (VM com um conector): sessão única "default" (sem multi-sessão).
 * Várias sessões na mesma máquina: WA_BIND_OFFICE_HEADER=1 e header X-Office-Id: <uuid do escritório>.
 *
 * .env: Servidor/.env (dotenv um nível acima, como antes).
 */

const http = require("http");
const path = require("path");
const fs = require("fs");
const envParent = path.join(__dirname, "..", ".env");
const envServerApi = path.join(__dirname, "..", "server-api", ".env");
// Ver server-api: override só com DOTENV_CONFIG_OVERRIDE=1 (PM2 vs .env).
require("dotenv").config({
  path: fs.existsSync(envParent) ? envParent : envServerApi,
  ...(String(process.env.DOTENV_CONFIG_OVERRIDE || "").trim() === "1"
    ? { override: true }
    : {}),
});
const { Client, LocalAuth, MessageMedia } = require("whatsapp-web.js");
const qrImage = require("qr-image");
const { spawnSync } = require("child_process");
const { timingSafeEqual, createHash } = require("crypto");

const PORT = Number(process.env.WA_SERVER_PORT) || 3010;
const APP_ROOT = path.resolve(__dirname);
process.chdir(APP_ROOT);
const DATA_ROOT = process.env.WA_APP_DATA_DIR
  ? path.resolve(process.env.WA_APP_DATA_DIR)
  : path.join(APP_ROOT, "data");
const authFolder = path.join(APP_ROOT, ".wwebjs_auth");
const cacheFolder = path.join(APP_ROOT, ".wwebjs_cache");
const pwBrowsersDir = path.join(DATA_ROOT, "ms-playwright");

const WA_BIND_OFFICE =
  String(process.env.WA_BIND_OFFICE_HEADER || "").trim() === "1";
const GROUPS_CACHE_TTL_MS = 90 * 1000;

/** @typedef {{ key: string, client: any, isReady: boolean, lastQR: Buffer|null, isStarting: boolean, qrLoggedInCycle: boolean, groupsCache: { list: any[], at: number }, groupsLoadPromise: Promise<any>|null }} WaSlot */

/** @type {Map<string, WaSlot>} */
const slots = new Map();

function uuidOk(s) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(
    String(s || "").trim(),
  );
}

/**
 * Resolve chave da sessão. Sem WA_BIND_OFFICE_HEADER: sempre "default" (um WhatsApp por VM).
 */
function resolveSlotKey(req) {
  if (!WA_BIND_OFFICE) return "default";
  const raw = String(req.headers["x-office-id"] || "").trim();
  if (!uuidOk(raw)) return null;
  return raw.toLowerCase();
}

function qrFileFor(key) {
  const safe =
    key === "default" ? "default" : key.replace(/[^a-f0-9-]/gi, "");
  return path.join(DATA_ROOT, "json", `wa_qr_${safe}.png`);
}

function sessionDirForSlot(slot) {
  if (slot.key === "default") return path.join(authFolder, "session");
  return path.join(authFolder, slot.key, "session");
}

/** @returns {WaSlot} */
function getSlot(req) {
  const key = resolveSlotKey(req);
  if (key === null) return null;
  if (!slots.has(key)) {
    slots.set(key, {
      key,
      client: null,
      isReady: false,
      lastQR: null,
      isStarting: false,
      qrLoggedInCycle: false,
      groupsCache: { list: [], at: 0 },
      groupsLoadPromise: null,
    });
  }
  return slots.get(key);
}

function clearGroupsCache(slot) {
  slot.groupsCache = { list: [], at: 0 };
  slot.groupsLoadPromise = null;
}

function clearChromeLocks(slot) {
  const sessionFolder = sessionDirForSlot(slot);
  const rootLocks = [
    "SingletonLock",
    "SingletonCookie",
    "SingletonSocket",
    "DevToolsActivePort",
  ];
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
            if (
              upper === "LOCK" ||
              upper.startsWith("SINGLETON") ||
              e.name === "DevToolsActivePort"
            ) {
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

function killOrphanChrome(slot) {
  if (process.platform !== "win32") return;
  const sessionPath = sessionDirForSlot(slot).replace(/'/g, "''");
  const authPath = authFolder.replace(/'/g, "''");
  const ps = [
    "Get-CimInstance Win32_Process |",
    "Where-Object { $_.CommandLine -and ( $_.CommandLine -like '*" +
      sessionPath +
      "*' -or $_.CommandLine -like '*" +
      authPath +
      "*' ) } |",
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

async function fetchGroupsForCache(slot) {
  if (!slot.client || !slot.isReady) return [];
  const existing = slot.groupsLoadPromise;
  if (existing) return existing;
  const promise = (async () => {
    try {
      const chats = await slot.client.getChats();
      const list = chats
        .filter((c) => c && c.isGroup)
        .map((c) => ({
          id: (c.id && c.id._serialized) || c.id || "",
          name:
            typeof c.name === "string" && c.name ? c.name : "Sem nome",
        }))
        .filter((g) => g.id);
      slot.groupsCache = { list, at: Date.now() };
      return list;
    } catch (e) {
      console.error(
        "[ERRO] Cache de grupos:",
        e && e.message ? e.message : e,
      );
      return slot.groupsCache.list.length ? slot.groupsCache.list : [];
    } finally {
      slot.groupsLoadPromise = null;
    }
  })();
  slot.groupsLoadPromise = promise;
  return promise;
}

function buildClient(slot) {
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

  const authStrategy =
    slot.key === "default"
      ? new LocalAuth({ dataPath: authFolder })
      : new LocalAuth({ dataPath: authFolder, clientId: slot.key });

  const qrFile = qrFileFor(slot.key);

  const c = new Client({
    authStrategy,
    webVersionCache: { type: "local", path: cacheFolder },
    puppeteer: puppeteerOpts,
    restartOnAuthFail: true,
  });

  c.on("qr", (qr) => {
    if (slot.isReady || slot.client !== c) return;
    const png = qrImage.imageSync(qr, {
      type: "png",
      size: 12,
      margin: 3,
      ec_level: "M",
    });
    slot.lastQR = Buffer.isBuffer(png) ? png : Buffer.from(png);
    try {
      const qrDir = path.dirname(qrFile);
      if (!fs.existsSync(qrDir)) fs.mkdirSync(qrDir, { recursive: true });
      fs.writeFileSync(qrFile, png);
      if (!slot.qrLoggedInCycle) {
        slot.qrLoggedInCycle = true;
        console.log("QR_READY", slot.key);
      }
    } catch (_) {}
  });

  c.on("ready", () => {
    slot.isReady = true;
    slot.qrLoggedInCycle = false;
    slot.lastQR = null;
    clearGroupsCache(slot);
    try {
      if (fs.existsSync(qrFile)) fs.rmSync(qrFile, { force: true });
    } catch (_) {}
    console.log("[OK] WhatsApp conectado.", slot.key);
    fetchGroupsForCache(slot)
      .then((list) => console.log("[OK] Grupos em cache:", slot.key, list.length))
      .catch(() => {});
  });

  c.on("disconnected", async (reason) => {
    slot.isReady = false;
    slot.qrLoggedInCycle = false;
    slot.lastQR = null;
    clearGroupsCache(slot);
    if (slot.client === c) {
      try {
        await slot.client.destroy();
      } catch (_) {}
      slot.client = null;
    }
    console.log("[!] Desconectado:", slot.key, reason || "");
  });

  c.on("auth_failure", () => {
    console.log("[!] Falha de autenticação.", slot.key);
  });

  return c;
}

async function startClient(slot) {
  if (slot.client) return;
  slot.isStarting = true;
  console.log("[INFO] Inicializando cliente WhatsApp...", slot.key);
  const qrFile = qrFileFor(slot.key);
  try {
    if (fs.existsSync(qrFile)) fs.rmSync(qrFile, { force: true });
  } catch (_) {}
  await new Promise((r) => setTimeout(r, 3000));
  slot.client = buildClient(slot);
  try {
    await slot.client.initialize();
  } catch (err) {
    const msg = err && err.message ? err.message : String(err);
    console.error("[ERRO] Falha ao inicializar:", slot.key, msg);
    slot.client = null;
    const isBrowserAlreadyRunning = /browser is already running|userDataDir/i.test(
      msg,
    );
    const isContextDestroyed = /Execution context|context was destroyed/i.test(
      msg,
    );
    if (isBrowserAlreadyRunning || isContextDestroyed) {
      console.log(
        "[INFO] Encerrando Chrome órfão e tentando novamente em 8s...",
        slot.key,
      );
      killOrphanChrome(slot);
      clearChromeLocks(slot);
      await new Promise((r) => setTimeout(r, 8000));
    } else if (fs.existsSync(cacheFolder)) {
      try {
        fs.rmSync(cacheFolder, { recursive: true, force: true });
      } catch (_) {}
      killOrphanChrome(slot);
      await new Promise((r) => setTimeout(r, 3000));
    }
    slot.client = buildClient(slot);
    try {
      await slot.client.initialize();
    } catch (e2) {
      slot.client = null;
      console.error(
        "[ERRO] Segunda tentativa falhou:",
        slot.key,
        e2 && e2.message ? e2.message : e2,
      );
    }
  } finally {
    slot.isStarting = false;
  }
}

async function disconnectClient(slot, opts = {}) {
  const clearSession = opts.clearSession === true;
  const qrFile = qrFileFor(slot.key);
  if (slot.client) {
    try {
      await slot.client.destroy();
    } catch (_) {}
    slot.client = null;
  }
  slot.isReady = false;
  slot.lastQR = null;
  try {
    if (fs.existsSync(qrFile)) fs.rmSync(qrFile, { force: true });
  } catch (_) {}
  if (clearSession) {
    try {
      if (slot.key === "default") {
        if (fs.existsSync(authFolder))
          fs.rmSync(authFolder, { recursive: true, force: true });
      } else {
        const sub = path.join(authFolder, slot.key);
        if (fs.existsSync(sub)) fs.rmSync(sub, { recursive: true, force: true });
      }
      if (fs.existsSync(cacheFolder))
        fs.rmSync(cacheFolder, { recursive: true, force: true });
      console.log(
        "[OK] Sessão apagada — próximo Conectar gerará novo QR.",
        slot.key,
      );
    } catch (e) {
      console.warn(
        "[AVISO] Erro ao apagar sessão:",
        e && e.message ? e.message : e,
      );
    }
  } else {
    console.log("[OK] Cliente desconectado. Sessão mantida.", slot.key);
  }
}

async function resetBrokenClient(slot) {
  if (!slot.client) return;
  slot.isReady = false;
  slot.lastQR = null;
  clearGroupsCache(slot);
  try {
    await slot.client.destroy();
  } catch (_) {}
  slot.client = null;
  const qrFile = qrFileFor(slot.key);
  try {
    if (fs.existsSync(qrFile)) fs.rmSync(qrFile, { force: true });
  } catch (_) {}
  console.log("[!] Cliente reiniciado (sessão quebrada).", slot.key);
}

function sendJson(res, statusCode, data) {
  res.writeHead(statusCode, { "Content-Type": "application/json" });
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
  res.setHeader(
    "Access-Control-Allow-Headers",
    "Content-Type, Authorization, ngrok-skip-browser-warning, X-Office-Id",
  );
  res.setHeader("Access-Control-Max-Age", "86400");
}

function normalizedConnectorSecret() {
  const raw = String(process.env.CONNECTOR_SECRET || "").trim();
  const cleanHex = raw.replace(/\s+/g, "").replace(/[^0-9a-fA-F]/g, "");
  return cleanHex.length === 64 ? cleanHex.toLowerCase() : raw;
}

/** Mesmo algoritmo do server-api / office_server_credentials (secret_hash). */
function sha256HexUtf8(s) {
  return createHash("sha256")
    .update(String(s || ""), "utf8")
    .digest("hex");
}

function requireToken(req, res) {
  const legacy = String(process.env.WHATSAPP_API_TOKEN || "").trim();
  const connector = normalizedConnectorSecret();
  if (!legacy && !connector) return true;
  const auth = String(req.headers.authorization || "");
  const bearer = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
  const candidates = [connector, legacy].filter(Boolean);
  if (connector) {
    const hash = sha256HexUtf8(connector);
    if (hash) candidates.push(hash);
  }
  for (const tok of candidates) {
    try {
      const a = Buffer.from(tok, "utf8");
      const b = Buffer.from(bearer, "utf8");
      if (a.length === b.length && timingSafeEqual(a, b)) return true;
    } catch (_) {}
  }
  sendJson(res, 401, { error: "Unauthorized" });
  return false;
}

const RATE_WINDOW_MS = 60_000;
const RATE_LIMITS = {
  "/send": 30,
  "/deliver": 30,
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
  const ip = String(
    (req.headers["x-forwarded-for"] || "").split(",")[0].trim() ||
      req.socket.remoteAddress ||
      "unknown",
  );
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

function normalizeSendPayload(data) {
  const groupId = (data.groupId || "").trim();
  const message = typeof data.message === "string" ? data.message : "";
  const attachments = Array.isArray(data.attachments) ? data.attachments : [];
  return { groupId, message, attachments };
}

/** /deliver genérico: hoje só channel whatsapp + target group */
function normalizeDeliverPayload(data) {
  if (!data || typeof data !== "object") return null;
  if (String(data.channel || "").toLowerCase() !== "whatsapp") return null;
  const targets = Array.isArray(data.targets) ? data.targets : [];
  const g = targets.find(
    (t) => t && String(t.type || "").toLowerCase() === "group" && t.id,
  );
  if (!g) return null;
  return normalizeSendPayload({
    groupId: String(g.id),
    message: data.message,
    attachments: data.attachments,
  });
}

function runSendInBackground(slot, groupId, message, attachments) {
  const rawId = groupId.includes("@") ? groupId.split("@")[0] : groupId;
  const targetId = rawId ? `${rawId.trim()}@g.us` : groupId;
  const delayMs = (ms) => new Promise((r) => setTimeout(r, ms));
  (async () => {
    try {
      await slot.client.sendMessage(targetId, message || " ");
      if (attachments.length > 0) {
        await delayMs(1200);
        for (const att of attachments) {
          const mimetype =
            att.mimetype && typeof att.mimetype === "string"
              ? att.mimetype
              : "application/octet-stream";
          const dataBase64 =
            att.dataBase64 && typeof att.dataBase64 === "string"
              ? att.dataBase64
              : "";
          const filename =
            att.filename && typeof att.filename === "string"
              ? att.filename
              : "documento";
          if (!dataBase64) continue;
          const media = new MessageMedia(mimetype, dataBase64, filename);
          await slot.client.sendMessage(targetId, media);
          await delayMs(800);
        }
      }
      console.log("[SEND] OK", slot.key, targetId);
    } catch (e) {
      console.error(
        "[ERRO] Enviar (background):",
        slot.key,
        e && e.message ? e.message : e,
      );
    }
  })();
}

const server = http.createServer(async (req, res) => {
  applyCors(req, res);
  if (req.method === "OPTIONS") {
    res.writeHead(204, {
      "Content-Length": "0",
      "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
      "Access-Control-Allow-Headers":
        "Content-Type, Authorization, ngrok-skip-browser-warning, X-Office-Id",
      "Access-Control-Max-Age": "86400",
    });
    res.end();
    return;
  }

  const url = req.url || "/";
  const pathname = url.split("?")[0];

  const slot = getSlot(req);
  if (!slot) {
    sendJson(res, 400, {
      error:
        "X-Office-Id obrigatório (UUID do escritório). Ative WA_BIND_OFFICE_HEADER=1 apenas se várias sessões na mesma VM.",
    });
    return;
  }

  const qrFile = qrFileFor(slot.key);

  if (pathname === "/status") {
    if (!rateLimit(req, res, pathname)) return;
    if (!requireToken(req, res)) return;
    sendJson(res, 200, { connected: slot.isReady, officeScope: slot.key });
    return;
  }

  if (pathname === "/qr") {
    if (!rateLimit(req, res, pathname)) return;
    if (!requireToken(req, res)) return;
    if (slot.isReady) {
      sendJson(res, 200, { qr: null, connected: true });
      return;
    }
    let qrBuffer = slot.lastQR;
    if (!qrBuffer || !Buffer.isBuffer(qrBuffer)) {
      try {
        if (fs.existsSync(qrFile)) {
          const stat = fs.statSync(qrFile);
          if (Date.now() - stat.mtimeMs < 90000) {
            qrBuffer = fs.readFileSync(qrFile);
            slot.lastQR = qrBuffer;
          }
        }
      } catch (_) {}
    }
    if (!qrBuffer || !Buffer.isBuffer(qrBuffer)) {
      sendJson(res, 200, { qr: null, connected: false });
      return;
    }
    const base64 = qrBuffer.toString("base64");
    sendJson(res, 200, {
      qr: `data:image/png;base64,${base64}`,
      connected: false,
    });
    return;
  }

  if (pathname === "/qr.png") {
    if (!rateLimit(req, res, "/qr")) return;
    if (!requireToken(req, res)) return;
    res.setHeader("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0");
    res.setHeader("Pragma", "no-cache");
    res.setHeader("Expires", "0");
    if (slot.isReady) {
      res.writeHead(204);
      res.end();
      return;
    }
    let qrBuffer = slot.lastQR;
    if (!qrBuffer || !Buffer.isBuffer(qrBuffer)) {
      try {
        if (fs.existsSync(qrFile)) {
          const stat = fs.statSync(qrFile);
          if (Date.now() - stat.mtimeMs < 90000) {
            qrBuffer = fs.readFileSync(qrFile);
            slot.lastQR = qrBuffer;
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
    if (!slot.isReady || !slot.client) {
      sendJson(res, 200, { groups: [] });
      return;
    }
    const urlObj = new URL(req.url || "/", `http://localhost:${PORT}`);
    const forceRefresh =
      urlObj.searchParams.get("refresh") === "1" ||
      urlObj.searchParams.get("refresh") === "true";
    const waitForRefresh =
      urlObj.searchParams.get("wait") === "1" ||
      urlObj.searchParams.get("wait") === "true";
    const useCache =
      !forceRefresh &&
      slot.groupsCache.list.length > 0 &&
      Date.now() - slot.groupsCache.at < GROUPS_CACHE_TTL_MS;
    if (useCache) {
      sendJson(res, 200, { groups: slot.groupsCache.list });
      return;
    }
    if (forceRefresh) clearGroupsCache(slot);
    // Evita timeout em escritórios com muitos grupos: responde rápido e carrega em segundo plano.
    if (!waitForRefresh) {
      if (!slot.groupsLoadPromise) {
        fetchGroupsForCache(slot).catch((e) => {
          console.error(
            "[ERRO] Prefetch de grupos:",
            e && e.message ? e.message : e,
          );
        });
      }
      sendJson(res, 200, {
        groups: slot.groupsCache.list.length ? slot.groupsCache.list : [],
        loading: true,
      });
      return;
    }
    try {
      const groups = await fetchGroupsForCache(slot);
      sendJson(res, 200, { groups });
    } catch (e) {
      const msg = e && e.message ? e.message : String(e);
      const isBroken = /detached|Execution context|context was destroyed/i.test(
        msg,
      );
      if (isBroken) {
        resetBrokenClient(slot).then(() => {
          sendJson(res, 200, {
            groups: [],
            error:
              "Sessão quebrada. Clique em Conectar no site para gerar novo QR.",
          });
        });
      } else {
        console.error("[ERRO] Listar grupos:", e);
        sendJson(res, 500, {
          groups: slot.groupsCache.list.length ? slot.groupsCache.list : [],
          error: msg || "Erro ao listar grupos",
        });
      }
    }
    return;
  }

  if (pathname === "/connect" && req.method === "POST") {
    if (!rateLimit(req, res, pathname)) return;
    if (!requireToken(req, res)) return;
    if (slot.client && slot.isReady) {
      sendJson(res, 200, { ok: true, alreadyConnected: true });
      return;
    }
    if (slot.client || slot.isStarting) {
      sendJson(res, 200, { ok: true, starting: true });
      return;
    }
    startClient(slot)
      .then(() => sendJson(res, 200, { ok: true }))
      .catch((e) =>
        sendJson(res, 500, {
          ok: false,
          error: e && e.message ? e.message : "Falha ao iniciar",
        }),
      );
    return;
  }

  if (pathname === "/disconnect" && req.method === "POST") {
    if (!rateLimit(req, res, pathname)) return;
    if (!requireToken(req, res)) return;
    const restartOnDisconnect = process.env.WA_RESTART_ON_DISCONNECT === "1";
    disconnectClient(slot, { clearSession: true })
      .then(() => {
        sendJson(res, 200, { ok: true });
        if (restartOnDisconnect) {
          setTimeout(() => process.exit(0), 1500);
        } else {
          startClient(slot).catch((e) =>
            console.error(
              "[ERRO] startClient após disconnect:",
              e && e.message ? e.message : e,
            ),
          );
        }
      })
      .catch(() => {
        sendJson(res, 200, { ok: true });
        if (!restartOnDisconnect) startClient(slot).catch(() => {});
      });
    return;
  }

  const handleSendBody = (body, sendOnce) => {
    try {
      if (!slot.isReady || !slot.client) {
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
      let norm = normalizeSendPayload(data);
      if (!norm.groupId && pathname === "/deliver") {
        const d = normalizeDeliverPayload(data);
        if (d) norm = d;
      }
      if (!norm.groupId) {
        sendOnce(400, {
          ok: false,
          error:
            pathname === "/deliver"
              ? "deliver: use channel=whatsapp e targets[{type:group,id}] ou POST /send com groupId"
              : "groupId obrigatório",
        });
        return;
      }
      console.log(
        "[SEND]",
        slot.key,
        "anexos=",
        norm.attachments.length,
      );
      sendOnce(200, { ok: true });
      runSendInBackground(
        slot,
        norm.groupId,
        norm.message,
        norm.attachments,
      );
    } catch (err) {
      console.error("[ERRO] send:", err && err.message ? err.message : err);
      sendOnce(500, { ok: false, error: "Erro interno" });
    }
  };

  if (
    (pathname === "/send" || pathname === "/deliver") &&
    req.method === "POST"
  ) {
    if (!rateLimit(req, res, pathname === "/deliver" ? "/deliver" : "/send"))
      return;
    if (!requireToken(req, res)) return;
    const contentLength = Math.min(
      parseInt(req.headers["content-length"], 10) || 0,
      10 * 1024 * 1024,
    );
    let body = "";
    let bodyDone = false;
    const sendOnce = (code, data) => {
      if (res.writableEnded) return;
      sendJson(res, code, data);
    };
    const BODY_TIMEOUT_MS = 45_000;
    const bodyTimeout = setTimeout(() => {
      if (bodyDone) return;
      bodyDone = true;
      sendOnce(408, {
        ok: false,
        error: "Tempo esgotado ao receber os dados. Tente novamente.",
      });
      req.destroy();
    }, BODY_TIMEOUT_MS);
    const processBody = () => {
      if (bodyDone) return;
      bodyDone = true;
      clearTimeout(bodyTimeout);
      if (contentLength > 0 && body.length > contentLength)
        body = body.slice(0, contentLength);
      handleSendBody(body, sendOnce);
    };
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
  console.log(
    "[API] Auth: Bearer CONNECTOR_SECRET (Servidor/.env). WA_BIND_OFFICE_HEADER=1 = várias sessões (sem auto-start).",
  );
  if (!WA_BIND_OFFICE) {
    const boot = getSlot({ headers: {} });
    if (boot) startClient(boot).catch((e) => console.error("[ERRO] boot:", e));
  } else {
    console.log(
      "[API] Auto-start desligado — cada escritório: POST /connect com X-Office-Id.",
    );
  }
});
