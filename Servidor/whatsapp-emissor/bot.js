/**
 * Módulo WhatsApp Emissor — estrutura padronizada do projeto.
 * Local: Servidor/whatsapp-emissor
 * Responsabilidades: (a) conexão/sessão (b) geração de QR (c) listagem de grupos (d) envio via outbox (e) formatação de mensagem no front (services/whatsapp).
 * Frontend consome via WHATSAPP_API (server.js: GET /status, /qr, /groups, POST /send).
 * Envio do formulário: apenas para o grupo selecionado (POST /send com groupId ou outbox com groupId).
 * Execução: node bot.js connect | disconnect | node server.js
 */
const { Client, MessageMedia, LocalAuth } = require('whatsapp-web.js');
const qrImage = require('qr-image');
const fs = require('fs');
const path = require('path');
const os = require('os');
require('dotenv').config({ path: path.join(__dirname, '.env') });
const { spawnSync } = require('child_process');

let client;
let isReady = false;
let isProcessingOutbox = false;
const APP_ROOT = path.resolve(__dirname);
// Tudo (auth, cache, data) fica dentro de Servidor/whatsapp-emissor
process.chdir(APP_ROOT);
const DATA_ROOT = process.env.WA_APP_DATA_DIR
  ? path.resolve(process.env.WA_APP_DATA_DIR)
  : path.join(APP_ROOT, 'data');
const authFolder = path.join(APP_ROOT, '.wwebjs_auth');
const cacheFolder = path.join(APP_ROOT, '.wwebjs_cache');
const sessionFolder = path.join(authFolder, 'session');
const outboxDir = path.join(DATA_ROOT, 'json', 'outbox');
const qrFile = path.join(DATA_ROOT, 'json', 'wa_qr.png');
const pwBrowsersDir = path.join(DATA_ROOT, 'ms-playwright');

if (!fs.existsSync(outboxDir)) {
  try { fs.mkdirSync(outboxDir, { recursive: true }); } catch (_) {}
}

const waitForConnection = async () => {
  const start = Date.now();
  while (!isReady) {
    if (Date.now() - start > 60000) {
      console.error('Tempo de conexao excedido.');
      process.exit(1);
    }
    await new Promise(r => setTimeout(r, 2000));
  }
};

const isGroupMessage = (msg) => {
  const from = msg && msg.from ? msg.from : '';
  return from.endsWith('@g.us') || msg.isGroup === true;
};

const isBroadcastMessage = (msg) => {
  const from = msg && msg.from ? msg.from : '';
  return from.endsWith('@broadcast') || from === 'status@broadcast';
};

const hasMeaningfulIncomingContent = (msg, mediaList) => {
  const data = (msg && msg._data) ? msg._data : {};
  const body = String((msg && msg.body) || '').trim();
  if (body) return true;
  if (Array.isArray(mediaList) && mediaList.length > 0) return true;
  const buttonId = String((msg && msg.selectedButtonId) || data.selectedButtonId || '').trim();
  if (buttonId) return true;
  const listRowId = String((msg && msg.selectedRowId) || data.selectedRowId || '').trim();
  if (listRowId) return true;
  const listRowTitle = String(data.listResponseTitle || data.selectedRowTitle || '').trim();
  if (listRowTitle) return true;
  return false;
};

const pickBestContactName = (msg, contact) => {
  const data = (msg && msg._data) ? msg._data : {};
  const from = (msg && msg.from) ? msg.from : '';
  const fromPhone = String(from).replace(/@c\.us$/i, '').trim();
  const bad = new Set(['null', 'none', 'undefined', 'unknown', 'desconhecido', 'nan']);
  const candidates = [
    contact && contact.name,
    contact && contact.pushname,
    contact && contact.shortName,
    data && data.contact && data.contact.name,
    data && data.contact && data.contact.pushname,
    data && data.notifyName,
    data && data.pushname,
    msg && msg.notifyName
  ];
  for (const raw of candidates) {
    const v = String(raw || '').trim();
    if (!v) continue;
    const low = v.toLowerCase();
    if (bad.has(low)) continue;
    if (!/[\p{L}\p{N}]/u.test(v)) continue;
    const vd = v.replace(/\D+/g, '');
    if (v === from || (fromPhone && vd && vd === fromPhone)) continue;
    return v;
  }
  return '';
};

const extractDigitsCandidate = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  const base = raw.split('@')[0].split(':')[0];
  const digits = base.replace(/\D+/g, '');
  return digits || '';
};

const normalizeBrDigits = (digits) => {
  const d = String(digits || '').replace(/\D+/g, '');
  if (!d) return '';
  if (d.startsWith('55') && d.length >= 12 && d.length <= 13) return d;
  if (d.length === 10 || d.length === 11) return '55' + d;
  return d;
};

const resolveIncomingPhoneDigits = (msg, contact) => {
  const data = (msg && msg._data) ? msg._data : {};
  const candidates = [
    msg && msg.from,
    msg && msg.author,
    data && data.from,
    data && data.author,
    data && data.participant,
    contact && contact.number,
    contact && contact.userid,
    contact && contact.phoneNumber,
    contact && contact.id && contact.id.user,
    contact && contact.id && contact.id._serialized,
  ];
  for (const c of candidates) {
    const digits = extractDigitsCandidate(c);
    if (!digits) continue;
    const norm = normalizeBrDigits(digits);
    if (norm.startsWith('55') && (norm.length === 12 || norm.length === 13)) {
      return norm;
    }
  }
  return '';
};

const serializeMessage = (msg, mediaList, contact) => {
  const from = msg.from || '';
  const phoneDigits = resolveIncomingPhoneDigits(msg, contact);
  const phone = phoneDigits || from.replace(/@c\.us$/, '');
  const data = (msg && msg._data) ? msg._data : {};
  const buttonId = msg.selectedButtonId || data.selectedButtonId || '';
  const listRowId = msg.selectedRowId || data.selectedRowId || '';
  const listRowTitle = data.listResponseTitle || data.selectedRowTitle || '';
  const contactName = pickBestContactName(msg, contact);
  let body = String(msg.body || '').trim();
  if (data && typeof data.body === 'string' && data.body.trim().length > body.length) {
    body = data.body.trim();
  }
  return {
    id: msg.id && msg.id._serialized ? msg.id._serialized : undefined,
    from,
    phone,
    phone_digits: phoneDigits,
    author: msg.author || null,
    timestamp: msg.timestamp || 0,
    body,
    buttonId,
    listRowId,
    listRowTitle,
    contactName,
    contactNumber: String((contact && contact.number) || ''),
    contactUser: String((contact && contact.id && contact.id.user) || ''),
    contactId: String((contact && contact.id && contact.id._serialized) || ''),
    pushName: (contact && contact.pushname) ? contact.pushname : '',
    notifyName: (data && data.notifyName) ? data.notifyName : '',
    fromName: (msg._data && msg._data.notifyName) ? msg._data.notifyName : (msg.from || ''),
    type: msg.type || '',
    media: Array.isArray(mediaList) ? mediaList : []
  };
};

// ——— Envio apenas para o grupo selecionado ———
// Se o payload tiver groupId, envia somente a mensagem (formulário) para esse grupo; ignora to/phone.

const startClient = async () => {
  console.log('[INFO] Inicializando cliente do WhatsApp Web...');
  const INIT_TIMEOUT_MS = 25000;
  const processStartMs = Date.now();
  try {
    if (fs.existsSync(qrFile)) fs.rmSync(qrFile, { force: true });
  } catch (_) {}
  const resolveBrowserExecutable = () => {
    try {
      if (!fs.existsSync(pwBrowsersDir)) return undefined;
      const found = [];
      const stack = [pwBrowsersDir];
      while (stack.length) {
        const dir = stack.pop();
        let entries = [];
        try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch (_) { continue; }
        for (const e of entries) {
          const full = path.join(dir, e.name);
          if (e.isDirectory()) { stack.push(full); continue; }
          if (e.isFile() && e.name.toLowerCase() === 'chrome.exe' && full.includes('chromium-')) found.push(full);
        }
      }
      if (!found.length) return undefined;
      found.sort((a, b) => {
        const ma = /chromium-(\d+)/i.exec(a);
        const mb = /chromium-(\d+)/i.exec(b);
        return (mb ? parseInt(mb[1], 10) : 0) - (ma ? parseInt(ma[1], 10) : 0);
      });
      return found[0];
    } catch (_) { return undefined; }
  };
  const clearChromeLocks = () => {
    ['SingletonLock', 'SingletonCookie', 'SingletonSocket', 'DevToolsActivePort'].forEach(name => {
      try { if (fs.existsSync(path.join(sessionFolder, name))) fs.rmSync(path.join(sessionFolder, name), { force: true }); } catch (_) {}
    });
    try {
      if (fs.existsSync(sessionFolder)) {
        const stack = [sessionFolder];
        while (stack.length) {
          const dir = stack.pop();
          let entries = [];
          try { entries = fs.readdirSync(dir, { withFileTypes: true }); } catch (_) { continue; }
          entries.forEach(e => {
            const full = path.join(dir, e.name);
            if (e.isDirectory()) stack.push(full);
            else if (['LOCK', 'SINGLETON', 'DevToolsActivePort'].some(x => e.name.toUpperCase().startsWith(x) || e.name === x)) {
              try { fs.rmSync(full, { force: true }); } catch (_) {}
            }
          });
        }
      }
    } catch (_) {}
  };
  const resetSessionProfile = () => {
    try { if (fs.existsSync(sessionFolder)) fs.rmSync(sessionFolder, { recursive: true, force: true }); } catch (_) {}
    try { fs.mkdirSync(sessionFolder, { recursive: true }); } catch (_) {}
  };
  const killOrphanChrome = () => {
    if (process.platform !== 'win32') return;
    const sessionPath = sessionFolder.replace(/'/g, "''");
    const ps = `Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -and $_.CommandLine -like '*${sessionPath}*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }`;
    try { spawnSync('powershell', ['-NoProfile', '-Command', ps], { stdio: 'ignore', timeout: 4000, windowsHide: true }); } catch (_) {}
  };

  killOrphanChrome();
  clearChromeLocks();
  await new Promise(r => setTimeout(r, 2500));

  const buildClient = (attempt = 1) => {
    const executablePath = resolveBrowserExecutable();
    const headlessMode = true;
    const puppeteerOpts = { headless: headlessMode, args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-gpu', '--disable-dev-shm-usage'] };
    if (!headlessMode) puppeteerOpts.args.push('--start-minimized', '--window-position=-32000,-32000', '--window-size=300,300');
    if (executablePath) puppeteerOpts.executablePath = executablePath;
    const c = new Client({
      authStrategy: new LocalAuth({ dataPath: authFolder }),
      webVersionCache: { type: 'local', path: cacheFolder },
      puppeteer: puppeteerOpts,
    });
    c.on('qr', qr => {
      const png = qrImage.imageSync(qr, { type: 'png' });
      try { fs.writeFileSync(qrFile, png); console.log('QR_READY'); } catch (_) {}
    });
    c.on('ready', () => { isReady = true; console.log('Bot conectado ao WhatsApp!'); });
    c.on('disconnected', (reason) => { isReady = false; process.exit(); });
    c.on('auth_failure', () => console.log('Falha na autenticacao. Escaneie o QR Code novamente.'));
    c.on('message', async msg => {
      try {
        if (msg.fromMe || isGroupMessage(msg) || isBroadcastMessage(msg)) return;
        let mediaList = [];
        if (msg.hasMedia) try {
          const media = await msg.downloadMedia();
          if (media && media.mimetype && (media.mimetype.startsWith('image/') || media.mimetype.startsWith('audio/')))
            mediaList.push({ mimetype: media.mimetype, data: media.data, filename: media.filename || '' });
        } catch (_) {}
        if (!hasMeaningfulIncomingContent(msg, mediaList)) return;
        let contact = null;
        try { contact = await msg.getContact(); } catch (_) {}
        console.log('INCOMING:' + JSON.stringify(serializeMessage(msg, mediaList, contact)));
      } catch (e) { console.error('Erro ao serializar mensagem:', e); }
    });
    return c;
  };

  client = buildClient(1);

  const processOutbox = async () => {
    if (!isReady || isProcessingOutbox) return;
    isProcessingOutbox = true;
    let files = [];
    try { files = fs.readdirSync(outboxDir).filter(f => f.endsWith('.json')); } catch (_) { isProcessingOutbox = false; return; }
    try {
      for (const f of files) {
        const filePath = path.join(outboxDir, f);
        let claimedPath = '';
        try { claimedPath = filePath + '.processing.' + process.pid + '.' + Date.now(); fs.renameSync(filePath, claimedPath); } catch (_) { continue; }
        let payload;
        try { payload = JSON.parse(fs.readFileSync(claimedPath, 'utf-8')); } catch (_) { try { fs.unlinkSync(claimedPath); } catch (_) {} continue; }
        const createdTs = Number(payload.created_ts || 0);
        if (createdTs > 0 && (Math.floor(Date.now() / 1000) - createdTs) > 3600) { try { fs.unlinkSync(claimedPath); } catch (_) {} continue; }

        const groupId = (payload.groupId || '').trim();
        const mensagem = payload.mensagem || '';

        if (groupId) {
          const targetId = groupId.includes('@') ? groupId : groupId + '@g.us';
          try {
            await client.sendMessage(targetId, mensagem);
          } catch (e) {
            console.error('Falha ao enviar para grupo:', e);
          }
          try { fs.unlinkSync(claimedPath); } catch (_) {}
          continue;
        }

        const to = (payload.to || '').trim();
        const phone = (payload.phone || '').replace(/\D+/g, '');
        const arquivos = Array.isArray(payload.arquivos) ? payload.arquivos : [];
        const buttonsPayload = payload.buttons && typeof payload.buttons === 'object' ? payload.buttons : null;
        const listPayload = payload.list && typeof payload.list === 'object' ? payload.list : null;
        const hasInteractive = !!(buttonsPayload || listPayload);
        if ((!to && !phone) || (!mensagem && arquivos.length === 0 && !hasInteractive)) {
          try { fs.unlinkSync(claimedPath); } catch (_) {}
          continue;
        }
        try {
          let targetId = to;
          if (!targetId && phone) {
            const numberId = await client.getNumberId(phone);
            if (!numberId || !numberId._serialized) {
              console.error('Falha ao enviar: numero invalido:', phone);
              try { fs.unlinkSync(claimedPath); } catch (_) {}
              continue;
            }
            targetId = numberId._serialized;
          }
          if (mensagem) await client.sendMessage(targetId, mensagem);
          for (const fp of arquivos) {
            if (!fs.existsSync(fp)) continue;
            try {
              const media = MessageMedia.fromFilePath(fp);
              await client.sendMessage(targetId, media);
              await new Promise(r => setTimeout(r, 1500));
            } catch (e) { console.error('Falha ao enviar anexo:', e); }
          }
        } catch (e) { console.error('Falha ao enviar mensagem:', e); }
        try { fs.unlinkSync(claimedPath); } catch (_) {}
      }
    } finally { isProcessingOutbox = false; }
  };
  setInterval(() => processOutbox().catch(() => {}), 2000);

  const initWithRetry = async (attempt = 1) => {
    try {
      await Promise.race([
        client.initialize(),
        new Promise((_, rej) => setTimeout(() => rej(new Error('Timeout')), INIT_TIMEOUT_MS))
      ]);
    } catch (err) {
      console.error('Falha ao inicializar:', err && err.message ? err.message : err);
      try { await client.destroy(); } catch (_) {}
      if (attempt >= 3) process.exit(1);
      if (fs.existsSync(cacheFolder)) try { fs.rmSync(cacheFolder, { recursive: true, force: true }); } catch (_) {}
      killOrphanChrome();
      clearChromeLocks();
      await new Promise(r => setTimeout(r, 3000));
      client = buildClient(attempt + 1);
      await initWithRetry(attempt + 1);
    }
  };
  await initWithRetry();
};

const waitForConnection = () => new Promise((resolve, reject) => {
  const start = Date.now();
  const t = setInterval(() => {
    if (isReady) { clearInterval(t); resolve(); return; }
    if (Date.now() - start > 60000) { clearInterval(t); reject(new Error('Timeout')); }
  }, 2000);
});

const cmd = process.argv[2];
if (cmd === 'connect') {
  startClient().catch(e => {
    console.error('Falha ao iniciar cliente:', e && e.message ? e.message : e);
    process.exit(1);
  });
} else if (cmd === 'disconnect') {
  if (process.platform === 'win32') {
    try {
      const sessionPath = sessionFolder.replace(/'/g, "''");
      const ps = `Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -like '*${sessionPath}*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }`;
      spawnSync('powershell', ['-NoProfile', '-Command', ps], { stdio: 'ignore', timeout: 4000, windowsHide: true });
    } catch (_) {}
  }
  [authFolder, cacheFolder].forEach(f => {
    try { if (fs.existsSync(f)) fs.rmSync(f, { recursive: true, force: true }); } catch (e) { console.warn('Nao foi possivel remover', f); }
  });
  console.log('Sessao apagada.');
  process.exit();
} else {
  console.log('Uso: node bot.js connect | disconnect');
  console.log('Para API HTTP (QR, grupos, envio para grupo): node server.js');
  process.exit();
}
