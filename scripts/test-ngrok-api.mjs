/**
 * Script de teste para a API do servidor (acessada via ngrok)
 *
 * Uso:
 *   node scripts/test-ngrok-api.mjs list
 *   node scripts/test-ngrok-api.mjs sync <company_id>
 *
 * Path de teste: EMPRESAS/Grupo Fleury/NFS
 * (relativo a C:\Users\ROBO\Documents na VM)
 */

import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import { dirname, join } from "path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function loadEnv() {
  try {
    const env = readFileSync(join(root, ".env"), "utf8");
    const out = {};
    for (const line of env.split("\n")) {
      const m = line.match(/^([^#=]+)=(.*)$/);
      if (m) out[m[1].trim()] = m[2].trim().replace(/^["']|["']$/g, "");
    }
    return out;
  } catch {
    return {};
  }
}

const env = loadEnv();
const SERVER_API_URL = (env.SERVER_API_URL || process.env.SERVER_API_URL || "https://plagiaristic-elinore-ungloomily.ngrok-free.dev").replace(/\/$/, "");

const TEST_PATH = "EMPRESAS/Grupo Fleury/NFS";

async function listFiles() {
  const url = `${SERVER_API_URL}/api/files/list?path=${encodeURIComponent(TEST_PATH)}`;
  const headers = {
    "ngrok-skip-browser-warning": "1",
    "User-Agent": "fleury-insights-hub/1.0",
  };
  console.log("GET", url);
  const res = await fetch(url, { headers });
  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    if (text.trimStart().startsWith("<")) {
      console.error("A API respondeu com HTML (página do ngrok?). O script já envia ngrok-skip-browser-warning.");
      console.error("Se persistir, abra a URL no navegador uma vez e confirme o acesso, depois rode o script de novo.");
    } else {
      console.error("Resposta não é JSON:", text.slice(0, 200));
    }
    process.exit(1);
  }
  if (!res.ok) {
    console.error("Erro:", data);
    process.exit(1);
  }
  console.log("Arquivos encontrados:", data.files?.length ?? 0);
  (data.files || []).forEach((f) => console.log("  -", f.name, "→", f.path));
  return data.files;
}

async function syncFiscal(companyId, token) {
  if (!token) {
    console.error("Para sync a API exige JWT (anon key só). Passe o token:");
    console.error("  node scripts/test-ngrok-api.mjs sync <company_id> <seu_jwt>");
    console.error("Ou defina SYNC_TOKEN no .env. Token: DevTools → Application → Local Storage → sb-...-auth-token → access_token");
    process.exit(1);
  }
  const url = `${SERVER_API_URL}/api/fiscal-sync`;
  const headers = {
    "Content-Type": "application/json",
    "Authorization": `Bearer ${token}`,
    "ngrok-skip-browser-warning": "1",
    "User-Agent": "fleury-insights-hub/1.0",
  };
  const body = {
    path: TEST_PATH,
    company_id: companyId,
    type: "NFS",
  };
  console.log("POST", url, "(com JWT)");
  const res = await fetch(url, { method: "POST", headers, body: JSON.stringify(body) });
  const text = await res.text();
  let data;
  try {
    data = JSON.parse(text);
  } catch {
    if (text.trimStart().startsWith("<")) {
      console.error("Resposta HTML (página do ngrok?). Tente abrir a URL no navegador uma vez.");
    } else {
      console.error("Resposta não é JSON:", text.slice(0, 200));
    }
    process.exit(1);
  }
  if (!res.ok) {
    console.error("Erro:", data);
    process.exit(1);
  }
  console.log("Sync: encontrados", data.found ?? "?", "arquivo(s), inseridos", data.inserted);
  (data.files || []).forEach((f) => console.log("  -", f.name, "id:", f.id));
  if (data.errors?.length) {
    console.error("Falhas no insert:");
    data.errors.forEach((e) => console.error("  -", e.name, ":", e.error));
  }
  return data;
}

async function main() {
  const cmd = process.argv[2];
  if (cmd === "list") {
    await listFiles();
  } else if (cmd === "sync") {
    const companyId = process.argv[3];
    const token = process.argv[4] || env.SYNC_TOKEN || process.env.SYNC_TOKEN;
    if (!companyId) {
      console.error("Uso: node scripts/test-ngrok-api.mjs sync <company_id> [jwt]");
      console.error("Obtenha o company_id da empresa no Supabase. JWT: usuário logado (ou SYNC_TOKEN no .env).");
      process.exit(1);
    }
    await syncFiscal(companyId, token);
  } else {
    console.log("Uso:");
    console.log("  node scripts/test-ngrok-api.mjs list          - Lista XML/PDF da pasta Grupo Fleury/NFS");
    console.log("  node scripts/test-ngrok-api.mjs sync <uuid> [jwt]  - Sincroniza (JWT obrigatório)");
    console.log("");
    console.log("Path de teste:", TEST_PATH);
    console.log("API:", SERVER_API_URL);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
