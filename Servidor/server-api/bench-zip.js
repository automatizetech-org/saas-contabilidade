/**
 * Teste de velocidade: montar ZIP com 1500 arquivos (leitura em lotes).
 * Uso: node bench-zip.js
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import archiver from "archiver";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join("C:", "Users", "Victor", "Documents", "EMPRESAS", "Empresa Teste", "FISCAL", "NFS");
const BATCH = 200;
const OUT = path.join(__dirname, "bench-output.zip");

function collectFiles(dir, list, max) {
  if (list.length >= max) return;
  if (!fs.existsSync(dir)) return;
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const e of entries) {
    if (list.length >= max) break;
    const full = path.join(dir, e.name);
    if (e.isDirectory()) collectFiles(full, list, max);
    else if (e.isFile() && /\.xml$/i.test(e.name)) list.push(full);
  }
}

async function main() {
  console.log("Coletando 1500 arquivos em", ROOT, "...");
  const t0 = Date.now();
  const files = [];
  collectFiles(path.join(ROOT, "Recebidas"), files, 800);
  collectFiles(path.join(ROOT, "Emitidas"), files, 1500);
  const t1 = Date.now();
  console.log("Arquivos:", files.length, "| listagem:", t1 - t0, "ms");

  const toAdd = files.slice(0, 1500).map((fullPath) => ({
    fullPath,
    nameInZip: path.basename(fullPath),
  }));

  const outStream = fs.createWriteStream(OUT);
  const archive = archiver("zip", { zlib: { level: 0 } });
  archive.pipe(outStream);

  const t2 = Date.now();
  for (let i = 0; i < toAdd.length; i += BATCH) {
    const batch = toAdd.slice(i, i + BATCH);
    const buffers = await Promise.all(
      batch.map(({ fullPath }) => fs.promises.readFile(fullPath).catch(() => null))
    );
    for (let j = 0; j < batch.length; j++) {
      if (buffers[j]) archive.append(buffers[j], { name: batch[j].nameInZip });
    }
  }
  await new Promise((resolve, reject) => {
    outStream.on("finish", resolve);
    archive.on("error", reject);
    archive.finalize();
  });
  const t3 = Date.now();
  const zipMs = t3 - t2;
  const size = fs.statSync(OUT).size;
  console.log("ZIP gerado:", OUT);
  console.log("Tamanho:", (size / 1024 / 1024).toFixed(2), "MB");
  console.log("Tempo leitura + ZIP:", zipMs, "ms (~", (zipMs / 1000).toFixed(1), "s)");
  console.log("Arquivos/segundo:", (toAdd.length / (zipMs / 1000)).toFixed(0));
  try { fs.unlinkSync(OUT); } catch (_) {}
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
