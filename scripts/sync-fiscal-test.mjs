/**
 * Teste de sync fiscal: EMPRESAS -> fiscal_documents (Supabase).
 * Usa a mesma lógica do server-api (Servidor). Carrega .env do Servidor para CONNECTOR_SECRET e SERVICE_ROLE_KEY.
 *
 * Uso:
 *   node scripts/sync-fiscal-test.mjs "C:\Users\Victor\Documents\Servidor\.env"        — sync normal
 *   node scripts/sync-fiscal-test.mjs "C:\Users\Victor\Documents\Servidor\.env" --rollback — sync e ao final apaga os inseridos nesta execução
 *   node scripts/sync-fiscal-test.mjs "C:\Users\Victor\Documents\Servidor\.env" --delete-synced — só apaga do banco os docs cujo file_path existe em EMPRESAS (para você testar o server do zero)
 */

import fs from "fs";
import path from "path";
import { createHash } from "crypto";
import { createClient } from "@supabase/supabase-js";

const SERVITOR_ENV = process.env.SERVITOR_ENV || process.argv[2] || "C:\\Users\\Victor\\Documents\\Servidor\\.env";

function loadEnv(filePath) {
  const resolved = path.resolve(filePath);
  if (!fs.existsSync(resolved)) {
    console.error("Arquivo .env não encontrado:", resolved);
    process.exit(1);
  }
  const content = fs.readFileSync(resolved, "utf8");
  for (const line of content.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const m = trimmed.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, "").trim();
  }
}

function sha256Hex(value) {
  return createHash("sha256").update(String(value || ""), "utf8").digest("hex");
}

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

async function main() {
  const rollback = process.argv.includes("--rollback");
  const deleteSyncedOnly = process.argv.includes("--delete-synced");
  loadEnv(SERVITOR_ENV);

  const SUPABASE_URL = process.env.SUPABASE_URL;
  const SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY;
  const CONNECTOR_SECRET = String(process.env.CONNECTOR_SECRET || "").trim();
  const BASE_PATH = process.env.BASE_PATH || "C:\\Users\\Victor\\Documents\\EMPRESAS";

  if (!SUPABASE_URL || !SERVICE_ROLE_KEY) {
    console.error("Defina SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY no .env do Servidor.");
    process.exit(1);
  }
  if (!CONNECTOR_SECRET) {
    console.error("Defina CONNECTOR_SECRET no .env do Servidor (segredo do escritório Automatize Tech).");
    process.exit(1);
  }

  const supabase = createClient(SUPABASE_URL, SERVICE_ROLE_KEY);
  const secretHash = sha256Hex(CONNECTOR_SECRET);

  const { data: credential } = await supabase
    .from("office_server_credentials")
    .select("office_server_id")
    .eq("secret_hash", secretHash)
    .maybeSingle();

  if (!credential?.office_server_id) {
    console.error("Nenhum credential encontrado para o CONNECTOR_SECRET. Verifique o segredo no Supabase (office_server_credentials).");
    process.exit(1);
  }

  const { data: officeServer } = await supabase
    .from("office_servers")
    .select("id, office_id")
    .eq("id", credential.office_server_id)
    .maybeSingle();

  if (!officeServer?.office_id) {
    console.error("office_servers não retornou office_id.");
    process.exit(1);
  }

  const OFFICE_ID = officeServer.office_id;
  const { data: office } = await supabase.from("offices").select("name").eq("id", OFFICE_ID).maybeSingle();
  console.log("Escritório:", office?.name ?? OFFICE_ID, "| office_id:", OFFICE_ID);
  console.log("BASE_PATH:", BASE_PATH);

  const empresasPath = BASE_PATH;
  const empresasExists = fs.existsSync(empresasPath) && fs.statSync(empresasPath).isDirectory();
  if (!empresasExists) {
    console.error("Pasta não encontrada:", empresasPath);
    process.exit(1);
  }

  if (deleteSyncedOnly) {
    const allPaths = new Set(walkDir("", BASE_PATH));
    const { data: rows } = await supabase
      .from("fiscal_documents")
      .select("id, file_path")
      .eq("office_id", OFFICE_ID)
      .not("file_path", "is", null);
    const toDelete = (rows || []).filter((r) => allPaths.has(r.file_path)).map((r) => r.id);
    if (toDelete.length === 0) {
      console.log("Nenhum documento sincronizado (disco) para apagar.");
      return;
    }
    const { error } = await supabase.from("fiscal_documents").delete().in("id", toDelete);
    if (error) console.error("Erro ao apagar:", error.message);
    else console.log("Apagados", toDelete.length, "documentos (file_path em EMPRESAS). Pode subir o server para testar o sync.");
    return;
  }

  let { data: companiesRaw } = await supabase
    .from("companies")
    .select("id, name, office_id")
    .eq("office_id", OFFICE_ID);
  let companies = (companiesRaw || []).filter((c) => c && String(c.office_id) === String(OFFICE_ID));

  const companyDirsForCreate = fs.readdirSync(empresasPath, { withFileTypes: true }).filter((e) => e.isDirectory());
  if (companies.length === 0 && companyDirsForCreate.length > 0) {
    console.log("Criando empresas a partir das pastas em EMPRESAS:", companyDirsForCreate.map((d) => d.name).join(", "));
    for (const d of companyDirsForCreate) {
      const { data: created, error } = await supabase
        .from("companies")
        .insert({ office_id: OFFICE_ID, name: d.name })
        .select("id, name")
        .single();
      if (error) {
        console.error("Erro ao criar empresa", d.name, ":", error.message);
      } else {
        console.log("  Criada:", created.name, created.id);
      }
    }
    const { data: afterRaw } = await supabase
      .from("companies")
      .select("id, name, office_id")
      .eq("office_id", OFFICE_ID);
    companies = (afterRaw || []).filter((c) => c && String(c.office_id) === String(OFFICE_ID));
  }

  const nameToId = new Map(companies.map((c) => [normalizeCompanyName(c.name), c.id]));
  console.log("Empresas no escritório:", companies.length, companies.map((c) => c.name).join(", ") || "(nenhuma)");

  if (companies.length === 0) {
    console.error("Nenhuma empresa cadastrada para este escritório e nenhuma pasta em EMPRESAS para criar.");
    process.exit(1);
  }

  const allPathsOnDisk = new Set(walkDir("", BASE_PATH));
  const pathsOnDiskByCompany = new Map(companies.map((c) => [c.id, new Set()]));
  const insertedIds = [];
  let inserted = 0;
  let skipped = 0;
  const errors = [];

  const companyDirs = fs.readdirSync(empresasPath, { withFileTypes: true }).filter((e) => e.isDirectory());

  for (const companyDir of companyDirs) {
    const companyName = companyDir.name;
    const companyId = nameToId.get(normalizeCompanyName(companyName));
    if (!companyId) {
      console.log("Pasta ignorada (sem empresa correspondente):", companyName);
      continue;
    }
    const pathsOnDisk = pathsOnDiskByCompany.get(companyId);
    if (!pathsOnDisk) continue;

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
        if (existingRows?.length > 0) {
          skipped++;
          continue;
        }
        const row = {
          office_id: String(OFFICE_ID),
          company_id: companyId,
          type: "NFS",
          chave,
          periodo,
          status: "novo",
          file_path: fileRel,
        };
        const { data: insertedRow, error } = await supabase.from("fiscal_documents").insert(row).select("id").single();
        if (error) {
          if (error.code === "23505") skipped++;
          else errors.push({ file: fileRel, error: error.message });
        } else {
          inserted++;
          if (insertedRow?.id) insertedIds.push(insertedRow.id);
        }
      }
    }

    for (const sub of ["Recebidas", "Emitidas"]) {
      const segment = path.join(companyName, "FISCAL", "NFE-NFC", sub).replace(/\\/g, "/");
      const files = walkDir(segment, BASE_PATH);
      for (const fileRel of files) {
        pathsOnDisk.add(fileRel);
        const baseName = path.basename(fileRel, path.extname(fileRel));
        const nameLower = baseName.toLowerCase();
        const docType = nameLower.includes("nfc") || nameLower.includes("65") ? "NFC" : "NFE";
        const chave = baseName;
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
        if (existingRows?.length > 0) {
          skipped++;
          continue;
        }
        const row = {
          office_id: String(OFFICE_ID),
          company_id: companyId,
          type: docType,
          chave,
          periodo,
          status: "novo",
          file_path: fileRel,
        };
        const { data: insertedRow, error } = await supabase.from("fiscal_documents").insert(row).select("id").single();
        if (error) {
          if (error.code === "23505") skipped++;
          else errors.push({ file: fileRel, error: error.message });
        } else {
          inserted++;
          if (insertedRow?.id) insertedIds.push(insertedRow.id);
        }
      }
    }
  }

  console.log("Resultado: inseridos =", inserted, "| já existentes =", skipped, "| erros =", errors.length);
  if (errors.length) {
    errors.forEach((e) => console.error("  ", e.file, "→", e.error));
  }

  if (rollback && insertedIds.length > 0) {
    console.log("Rollback: apagando", insertedIds.length, "registros inseridos...");
    const { error: delErr } = await supabase.from("fiscal_documents").delete().in("id", insertedIds);
    if (delErr) console.error("Erro ao apagar:", delErr.message);
    else console.log("Apagados com sucesso.");
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
