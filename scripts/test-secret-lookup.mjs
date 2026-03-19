/**
 * Teste: dado o segredo em texto (64 hex), busca o escritório no Supabase.
 * Uso: node scripts/test-secret-lookup.mjs
 * Segredo fixo do teste: 97d0b2f598d4bb08084728324bcaf78fe6f7d97749027b2f005ef47b16640cc3
 */
import fs from "fs";
import path from "path";
import { createHash } from "crypto";
import { fileURLToPath } from "url";
import { createClient } from "@supabase/supabase-js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const servidorEnv = path.resolve(__dirname, "..", "..", "..", "Documents", "Servidor", ".env");
const fallback = "C:\\Users\\Victor\\Documents\\Servidor\\.env";

function loadEnv(p) {
  const resolved = path.resolve(p);
  if (!fs.existsSync(resolved)) return false;
  const content = fs.readFileSync(resolved, "utf8");
  for (const line of content.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const m = trimmed.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, "").trim();
  }
  return true;
}

if (!loadEnv(servidorEnv) && !loadEnv(fallback)) {
  console.error("Coloque SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY no .env do Servidor ou defina no ambiente.");
  process.exit(1);
}

const secret = "97d0b2f598d4bb08084728324bcaf78fe6f7d97749027b2f005ef47b16640cc3";
const secretHash = createHash("sha256").update(secret, "utf8").digest("hex");

const url = process.env.SUPABASE_URL;
const key = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!url || !key) {
  console.error("SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY são obrigatórios.");
  process.exit(1);
}

const supabase = createClient(url, key);

const { data: cred, error: e1 } = await supabase
  .from("office_server_credentials")
  .select("office_server_id")
  .eq("secret_hash", secretHash)
  .maybeSingle();

if (e1) {
  console.error("Erro credential:", e1.message);
  process.exit(1);
}

if (!cred) {
  console.log("Nenhum credential com esse hash. Hash usado:", secretHash);
  process.exit(0);
}

const { data: server, error: e2 } = await supabase
  .from("office_servers")
  .select("id, office_id")
  .eq("id", cred.office_server_id)
  .maybeSingle();

if (e2 || !server) {
  console.error("Erro office_server:", e2?.message || "não encontrado");
  process.exit(1);
}

const { data: office, error: e3 } = await supabase
  .from("offices")
  .select("id, name, slug, status")
  .eq("id", server.office_id)
  .maybeSingle();

if (e3 || !office) {
  console.error("Erro office:", e3?.message || "não encontrado");
  process.exit(1);
}

console.log("Segredo (texto):", secret.slice(0, 16) + "...");
console.log("Hash (SHA256): ", secretHash);
console.log("---");
console.log("Escritório encontrado:", office.name);
console.log("  slug:  ", office.slug);
console.log("  id:    ", office.id);
console.log("  status:", office.status);
