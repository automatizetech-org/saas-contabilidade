/**
 * Extração de texto de PDFs para auto-preenchimento do formulário de Alteração Empresarial.
 * Usa pdfjs-dist (Mozilla PDF.js) — gratuito, sem IA.
 * Busca: Inscrição Municipal, CPF do Sócio (em ordem).
 */

import * as pdfjsLib from "pdfjs-dist";
import workerSrc from "pdfjs-dist/build/pdf.worker.min.mjs?url";

if (typeof window !== "undefined") {
  pdfjsLib.GlobalWorkerOptions.workerSrc = workerSrc;
}

/** CNPJ do escritório (37.197.978/0001-03) — nunca usar como CNPJ da empresa extraído do PDF */
const CNPJ_ESCRITORIO = "37197978000103";

export interface ExtractedPdfFields {
  /** CNPJ (14 dígitos) ou CPF (11 dígitos) da empresa — primeiro campo do formulário; usado para buscar na Receita quando for CNPJ */
  cnpjOuCpfEmpresa?: string;
  inscricaoMunicipal?: string;
  cpfsSocio: string[];
  /** Descrição da atividade econômica principal (após "CÓDIGO E DESCRIÇÃO DA ATIVIDADE ECONÔMICA PRINCIPAL") */
  tipoAtividade?: string;
  /** E-mail encontrado após "ENDEREÇO ELETRÔNICO" */
  emailContato?: string;
  /** Primeiro telefone válido no formato (XX) XXXXX-XXXX ou (XX) XXXX-XXXX — ignora (0000) 0000-0000 */
  telefoneContato?: string;
}

/** Extrai texto de todas as páginas do PDF. */
async function extractFullTextFromPdf(file: File): Promise<string> {
  const arrayBuffer = await file.arrayBuffer();
  const pdf = await pdfjsLib.getDocument({ data: arrayBuffer }).promise;
  const parts: string[] = [];
  for (let i = 1; i <= pdf.numPages; i++) {
    const page = await pdf.getPage(i);
    const textContent = await page.getTextContent();
    const pageText = textContent.items
      .map((item) => ("str" in item ? item.str : ""))
      .join(" ");
    parts.push(pageText);
  }
  return parts.join("\n");
}

/** Normaliza dígitos de CPF para 11 caracteres e formata 000.000.000-00. */
function normalizeCpf(raw: string): string {
  const digits = raw.replace(/\D/g, "").slice(0, 11);
  if (digits.length !== 11) return "";
  return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6, 9)}-${digits.slice(9)}`;
}

/**
 * Extrai Inscrição Municipal, CPFs do sócio, atividade principal, e-mail e telefone do texto do PDF.
 * Primeiro extrai CNPJ/CPF da empresa (NÚMERO DE INSCRIÇÃO ou C.N.P.J. / C.P.F.) para preencher o campo e buscar na Receita.
 */
function parseExtractedText(text: string): ExtractedPdfFields {
  const result: ExtractedPdfFields = { cpfsSocio: [] };

  // CNPJ/CPF da empresa (primeiro campo do formulário) — mesmo regex para os dois formatos
  // Formato 1: NÚMERO DE INSCRIÇÃO depois 65.252.839/0001-62 (ou CPF 000.000.000-00)
  // Formato 2: C.N.P.J. / C.P.F. 65.252.839/0001-62
  // Formato 3: CNPJ: 64.982.472/0001-70
  const cnpjCpfPattern =
    /(\d{2}\.?\d{3}\.?\d{3}\/?\d{4}-?\d{2}|\d{3}\.?\d{3}\.?\d{3}-?\d{2})/;
  const numInscMatch = text.match(/N[ÚU]MERO\s+DE\s+INSCRI[CÇ][AÃ]O\s*[\r\n]*\s*/i);
  const cnpjCpfLabelMatch = text.match(/C\.?N\.?P\.?J\.?\s*\/\s*C\.?P\.?F\.?\s*/i);
  const cnpjLabelMatch = text.match(/CNPJ\s*:\s*/i);
  if (numInscMatch) {
    const after = text.slice(text.indexOf(numInscMatch[0]) + numInscMatch[0].length);
    const numMatch = after.match(cnpjCpfPattern);
    if (numMatch) {
      const digits = numMatch[1].replace(/\D/g, "");
      if ((digits.length === 11 || digits.length === 14) && digits !== CNPJ_ESCRITORIO) {
        result.cnpjOuCpfEmpresa = digits;
      }
    }
  }
  if (!result.cnpjOuCpfEmpresa && cnpjCpfLabelMatch) {
    const after = text.slice(text.indexOf(cnpjCpfLabelMatch[0]) + cnpjCpfLabelMatch[0].length);
    const numMatch = after.match(cnpjCpfPattern);
    if (numMatch) {
      const digits = numMatch[1].replace(/\D/g, "");
      if ((digits.length === 11 || digits.length === 14) && digits !== CNPJ_ESCRITORIO) {
        result.cnpjOuCpfEmpresa = digits;
      }
    }
  }
  if (!result.cnpjOuCpfEmpresa && cnpjLabelMatch) {
    const after = text.slice(text.indexOf(cnpjLabelMatch[0]) + cnpjLabelMatch[0].length);
    const numMatch = after.match(cnpjCpfPattern);
    if (numMatch) {
      const digits = numMatch[1].replace(/\D/g, "");
      if ((digits.length === 11 || digits.length === 14) && digits !== CNPJ_ESCRITORIO) {
        result.cnpjOuCpfEmpresa = digits;
      }
    }
  }

  // Inscrição Municipal: Nº Inscrição Municipal 7661479 (número após "Inscrição Municipal")
  const inscMunicipalMatch = text.match(
    /N[º°]?\s*Inscri[cç][ãa]o\s*Municipal\s*(\d[\d.\s]*\d|\d+)/i
  );
  if (inscMunicipalMatch) {
    const num = inscMunicipalMatch[1].replace(/\s/g, "").replace(/\./g, "");
    if (/^\d+$/.test(num)) result.inscricaoMunicipal = num;
  }

  // CPF do sócio: CPF 037.360.951-55 ou CPF:037.360.951-55 ou CPF: 037.360.951-55
  const cpfRegex =
    /CPF\s*:?\s*(\d{3}[.\s]?\d{3}[.\s]?\d{3}[-.\s]?\d{2})/gi;
  const seen = new Set<string>();
  let m: RegExpExecArray | null;
  while ((m = cpfRegex.exec(text)) !== null) {
    const formatted = normalizeCpf(m[1]);
    if (formatted && !seen.has(formatted)) {
      seen.add(formatted);
      result.cpfsSocio.push(formatted);
    }
  }

  // Atividade econômica principal: só a primeira — para antes de "CÓDIGO E DESCRIÇÃO DAS ATIVIDADES ECONÔMICAS SECUNDÁRIAS"
  // No PDF o texto pode vir corrido: "68.21-8-01 - Corretagem... CÓDIGO E DESCRIÇÃO DAS ATIVIDADES SECUNDÁRIAS 66.30-4-00..."
  const secHeader =
    "C[OÓ]DIGO\\s+E\\s+DESCRI[CÇ][AÃ]O\\s+DAS\\s+ATIVIDADES\\s+ECON[OÔ]MICAS\\s+SECUND[AÁ]RIAS";
  const atividadeRegex = new RegExp(
    `C[OÓ]DIGO\\s+E\\s+DESCRI[CÇ][AÃ]O\\s+DA\\s+ATIVIDADE\\s+ECON[OÔ]MICA\\s+PRINCIPAL[\\s\\S]*?(\\d{2}\\.\\d{2}-\\d-\\d{2})\\s*-\\s*(.+?)(?=\\s*${secHeader}|\\n|$)`,
    "i"
  );
  const atividadeMatch = text.match(atividadeRegex);
  if (atividadeMatch) {
    const desc = atividadeMatch[2].trim();
    if (desc) result.tipoAtividade = desc;
  }

  // E-mail: após "ENDEREÇO ELETRÔNICO", captura o e-mail (pode estar na mesma linha ou na seguinte, com ou sem espaços)
  const emailMatch = text.match(
    /ENDERE[CÇ]O\s+ELETR[OÔ]NICO\s*[\r\n]*\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})/i
  );
  if (emailMatch) {
    const email = emailMatch[1].replace(/\s/g, "").toLowerCase();
    if (email) result.emailContato = email;
  }

  // Telefone: primeiro número (XX) XXXXX-XXXX ou (XX) XXXX-XXXX; ignora (00) 0000-0000 e (0000) 0000-0000
  const telefoneRegex = /\(\d{2}\)\s*\d{4,5}[-\s]?\d{4}/g;
  let telMatch: RegExpExecArray | null;
  while ((telMatch = telefoneRegex.exec(text)) !== null) {
    const raw = telMatch[0];
    const digits = raw.replace(/\D/g, "");
    if (digits.length >= 10 && !/^0+$/.test(digits) && digits !== "0000000000") {
      const ddd = raw.slice(1, 3);
      const num = digits.slice(2); // 8 ou 9 dígitos
      result.telefoneContato =
        num.length >= 8
          ? num.length === 9
            ? `(${ddd}) ${num.slice(0, 5)}-${num.slice(5)}`
            : `(${ddd}) ${num.slice(0, 4)}-${num.slice(4)}`
          : raw;
      break;
    }
  }

  return result;
}

/**
 * Lê o PDF e retorna campos extraídos para auto-preenchimento.
 * Só processa arquivos com tipo application/pdf ou nome terminando em .pdf.
 */
export async function extractPdfFormFields(
  file: File
): Promise<ExtractedPdfFields | null> {
  const isPdf =
    file.type === "application/pdf" ||
    file.name.toLowerCase().endsWith(".pdf");
  if (!isPdf) return null;
  try {
    const fullText = await extractFullTextFromPdf(file);
    return parseExtractedText(fullText);
  } catch {
    return null;
  }
}
