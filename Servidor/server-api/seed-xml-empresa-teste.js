/**
 * Cria 1500 arquivos XML na pasta Empresa Teste para testes.
 * Estrutura: EMPRESAS/Empresa Teste/FISCAL/NFS/Recebidas e Emitidas (o fiscal-watcher sincroniza esses paths).
 * Uso: node seed-xml-empresa-teste.js
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// Pasta base: C:\Users\Victor\Documents\EMPRESAS\Empresa Teste
const ROOT = path.join("C:", "Users", "Victor", "Documents", "EMPRESAS", "Empresa Teste", "FISCAL", "NFS");

const TOTAL = 1500;
const RECEBIDAS = 800;
const EMITIDAS = 700;

/** XML mínimo válido tipo NFS (nota de serviço) para o sync reconhecer */
function makeNfsXml(num, subpasta) {
  const id = `NFS-${subpasta}-${String(num).padStart(5, "0")}`;
  const now = new Date();
  const ano = now.getFullYear();
  const mes = String(now.getMonth() + 1).padStart(2, "0");
  return `<?xml version="1.0" encoding="UTF-8"?>
<nfe xmlns="http://www.portalfiscal.inf.br/nfe">
  <infNFe Id="${id}">
    <ide>
      <cUF>31</cUF>
      <nNF>${num}</nNF>
      <serie>1</serie>
      <dhEmi>${now.toISOString()}</dhEmi>
    </ide>
    <emit>
      <xNome>Empresa Teste</xNome>
      <CNPJ>00000000000191</CNPJ>
    </emit>
    <dest>
      <xNome>Cliente Teste ${num}</xNome>
    </dest>
  </infNFe>
</nfe>`;
}

function ensureDir(dir) {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

function main() {
  const recebidasDir = path.join(ROOT, "Recebidas");
  const emitidasDir = path.join(ROOT, "Emitidas");
  ensureDir(recebidasDir);
  ensureDir(emitidasDir);

  console.log("Criando", TOTAL, "XMLs em", ROOT);
  let created = 0;
  for (let i = 1; i <= RECEBIDAS; i++) {
    const xml = makeNfsXml(i, "REC");
    const file = path.join(recebidasDir, `nfs-recebida-${String(i).padStart(5, "0")}.xml`);
    fs.writeFileSync(file, xml, "utf8");
    created++;
    if (created % 200 === 0) console.log("  ", created, "/", TOTAL);
  }
  for (let i = 1; i <= EMITIDAS; i++) {
    const xml = makeNfsXml(i, "EMI");
    const file = path.join(emitidasDir, `nfs-emitida-${String(i).padStart(5, "0")}.xml`);
    fs.writeFileSync(file, xml, "utf8");
    created++;
    if (created % 200 === 0) console.log("  ", created, "/", TOTAL);
  }
  console.log("Pronto:", created, "arquivos XML em Empresa Teste/FISCAL/NFS/Recebidas e Emitidas.");
}

main();
