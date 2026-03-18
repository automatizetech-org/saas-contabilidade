/**
 * Serviço para ler DOCX, identificar trechos em vermelho (campos dinâmicos) e gerar DOCX preenchido.
 * Preserva formatação; substitui apenas o texto dos runs em vermelho.
 */

import JSZip from "jszip";

const NS_W =
  "http://schemas.openxmlformats.org/wordprocessingml/2006/main";

const RED_VALS = new Set([
  "FF0000",
  "ff0000",
  "RED",
  "red",
  "F80000",
  "f80000",
]);

function parseXml(xmlText: string): Document {
  const parser = new DOMParser();
  return parser.parseFromString(xmlText, "text/xml");
}

function getRunText(run: Element): string {
  const ts = run.getElementsByTagNameNS(NS_W, "t");
  let s = "";
  for (let i = 0; i < ts.length; i++) {
    s += (ts[i].textContent ?? "");
  }
  return s;
}

function isRedRun(run: Element): boolean {
  const rPr = run.getElementsByTagNameNS(NS_W, "rPr")[0];
  if (!rPr) return false;
  const colors = rPr.getElementsByTagNameNS(NS_W, "color");
  for (let i = 0; i < colors.length; i++) {
    const el = colors[i];
    const val = (el.getAttribute("w:val") ?? el.getAttribute("val") ?? "").trim();
    if (val && RED_VALS.has(val)) return true;
    if (/^[0-9A-Fa-f]{6}$/.test(val) && val.toLowerCase().startsWith("f")) return true;
  }
  return false;
}

/**
 * Carrega um DOCX (ArrayBuffer) e retorna a lista de textos em vermelho (placeholders).
 */
export async function getRedPlaceholdersFromDocx(buffer: ArrayBuffer): Promise<string[]> {
  const zip = await JSZip.loadAsync(buffer);
  const docEntry = zip.file("word/document.xml");
  if (!docEntry) return [];
  const xmlText = await docEntry.async("string");
  const doc = parseXml(xmlText);
  const runs = doc.getElementsByTagNameNS(NS_W, "r");
  const placeholders: string[] = [];
  const seen = new Set<string>();
  for (let i = 0; i < runs.length; i++) {
    const run = runs[i];
    if (!isRedRun(run)) continue;
    const text = getRunText(run).trim();
    if (text && !seen.has(text)) {
      seen.add(text);
      placeholders.push(text);
    }
  }
  return placeholders;
}

/**
 * Substitui em cada run vermelho o texto pelo valor correspondente em replacements (chave = texto original).
 * Retorna o DOCX modificado como Blob.
 */
export async function fillDocxWithReplacements(
  buffer: ArrayBuffer,
  replacements: Record<string, string>
): Promise<Blob> {
  const zip = await JSZip.loadAsync(buffer);
  const docEntry = zip.file("word/document.xml");
  if (!docEntry) throw new Error("document.xml não encontrado no DOCX");
  const xmlText = await docEntry.async("string");
  const doc = parseXml(xmlText);
  const runs = doc.getElementsByTagNameNS(NS_W, "r");
  for (let i = 0; i < runs.length; i++) {
    const run = runs[i];
    if (!isRedRun(run)) continue;
    const oldText = getRunText(run).trim();
    const newText = replacements[oldText] ?? oldText;
    const ts = run.getElementsByTagNameNS(NS_W, "t");
    if (ts.length) {
      const first = ts[0];
      first.textContent = newText;
      for (let j = 1; j < ts.length; j++) ts[j].textContent = "";
    }
  }
  const serializer = new XMLSerializer();
  const newXml = serializer.serializeToString(doc);
  zip.file("word/document.xml", newXml);
  const blob = await zip.generateAsync({
    type: "blob",
    mimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  });
  return blob;
}
