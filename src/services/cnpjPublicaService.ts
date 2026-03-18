/**
 * Consulta CNPJ em APIs públicas (Receita/dados abertos).
 * 1) publica.cnpj.ws — dados completos incluindo sócios com CPF.
 * 2) Fallback: BrasilAPI quando a primeira falhar ou retornar 404.
 * Usar apenas dígitos (sem máscara) na URL.
 */

const BASE_PUBLICA = "https://publica.cnpj.ws/cnpj";
const BASE_BRASIL_API = "https://brasilapi.com.br/api/cnpj/v1";

export interface CnpjPublicaEstabelecimento {
  cnpj: string;
  cnpj_raiz: string;
  nome_fantasia?: string;
  data_inicio_atividade?: string;
  data_situacao_cadastral?: string;
  email?: string;
  ddd1?: string;
  telefone1?: string;
  ddd2?: string;
  telefone2?: string;
  logradouro?: string;
  numero?: string;
  complemento?: string;
  bairro?: string;
  cep?: string;
  atividade_principal?: { id: string; descricao: string };
  atividades_secundarias?: Array<{ id: string; descricao: string }>;
  inscricoes_estaduais?: Array<{ inscricao_estadual: string; estado?: { sigla: string } }>;
  tipo_logradouro?: string;
  estado?: { sigla: string };
  cidade?: { nome: string };
}

export interface CnpjPublicaSocio {
  nome: string;
  cpf_cnpj_socio?: string;
  tipo?: string;
  data_entrada?: string;
}

export interface CnpjPublicaResponse {
  cnpj_raiz: string;
  razao_social: string;
  capital_social?: string;
  estabelecimento: CnpjPublicaEstabelecimento;
  socios?: CnpjPublicaSocio[];
  natureza_juridica?: { descricao: string } | null;
  porte?: { descricao: string } | null;
  simples?: {
    simples?: string;
    data_opcao_simples?: string | null;
    data_exclusao_simples?: string | null;
  };
}

/** Dados mapeados para o formulário de Alteração Empresarial */
export interface CnpjFormData {
  razao_social: string;
  cnpj: string;
  data_abertura: string;
  tipo_atividade: string;
  inscricao_estadual: string;
  state_code: string;
  city_name: string;
  email: string;
  telefone: string;
  /** Lista de sócios (nome + CPF); pode vir mais de um da API */
  socios: Array<{ nome: string; cpf_socio: string }>;
  nome_fantasia: string;
  capital_social: string;
  natureza_juridica: string;
  porte: string;
  situacao_cadastral: string;
  tributacao: string;
}

function onlyDigits(s: string): string {
  return String(s ?? "").replace(/\D/g, "");
}

function formatIE(ie: string): string {
  const d = onlyDigits(ie);
  if (d.length < 9) return ie;
  return `${d.slice(0, 2)}.${d.slice(2, 5)}.${d.slice(5, 8)}-${d.slice(8)}`;
}

/** Formata data YYYY-MM-DD para DD/MM/YYYY */
function formatData(s: string | undefined): string {
  if (!s) return "";
  const [y, m, d] = s.split("-");
  if (!y || !m || !d) return s;
  return `${d.padStart(2, "0")}/${m.padStart(2, "0")}/${y}`;
}

/** Formata valor monetário (capital_social vem como "105000.00") */
function formatCapital(s: string | undefined): string {
  if (!s) return "";
  const n = parseFloat(s);
  if (Number.isNaN(n)) return s;
  return n.toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export async function fetchCnpjPublica(cnpjApenasDigitos: string): Promise<CnpjFormData | null> {
  const digits = onlyDigits(cnpjApenasDigitos);
  if (digits.length !== 14) return null;

  // 1) Tentar publica.cnpj.ws (dados mais completos, incluindo CPF dos sócios)
  try {
    const res = await fetch(`${BASE_PUBLICA}/${digits}`, {
      method: "GET",
      headers: { Accept: "application/json" },
    });
    if (res.ok) {
      const data: CnpjPublicaResponse = await res.json();
      return mapPublicaToFormData(data, digits);
    }
    if (res.status === 404) {
      // CNPJ não encontrado na publica; tentar BrasilAPI
      const fallback = await fetchBrasilApi(digits);
      if (fallback) return fallback;
      throw new Error("CNPJ não encontrado.");
    }
    throw new Error("API indisponível ou sem retorno.");
  } catch (e) {
    if (e instanceof Error && e.message === "CNPJ não encontrado.") throw e;
    // Erro de rede ou outro: tentar BrasilAPI como fallback
    const fallback = await fetchBrasilApi(digits);
    if (fallback) return fallback;
    throw e instanceof Error ? e : new Error("API indisponível ou sem retorno.");
  }
}

/** Resposta da BrasilAPI (fallback) — estrutura diferente da publica.cnpj.ws */
interface BrasilApiResponse {
  cnpj?: string;
  razao_social?: string;
  nome_fantasia?: string;
  uf?: string;
  municipio?: string;
  data_inicio_atividade?: string;
  situacao_cadastral?: number;
  descricao_situacao_cadastral?: string;
  cnae_fiscal?: number;
  cnae_fiscal_descricao?: string;
  cnaes_secundarios?: Array<{ codigo: number; descricao: string }>;
  natureza_juridica?: string;
  porte?: string;
  capital_social?: number;
  email?: string | null;
  ddd_telefone_1?: string | null;
  ddd_telefone_2?: string | null;
  qsa?: Array<{ nome_socio: string; cnpj_cpf_do_socio?: string }>;
  regime_tributario?: Array<{ forma_de_tributacao?: string }>;
}

async function fetchBrasilApi(digits: string): Promise<CnpjFormData | null> {
  try {
    const res = await fetch(`${BASE_BRASIL_API}/${digits}`, {
      method: "GET",
      headers: { Accept: "application/json" },
    });
    if (!res.ok) return null;
    const data: BrasilApiResponse = await res.json();
    return mapBrasilApiToFormData(data, digits);
  } catch {
    return null;
  }
}

function mapBrasilApiToFormData(data: BrasilApiResponse, digits: string): CnpjFormData {
  const dataAbertura = data.data_inicio_atividade
    ? formatData(data.data_inicio_atividade)
    : "";
  let telefone = "";
  const dddTel = data.ddd_telefone_1 ?? "";
  if (dddTel.length >= 10) {
    const ddd = dddTel.slice(0, 2);
    const num = dddTel.slice(2).replace(/\D/g, "");
    telefone = num.length > 8 ? `(${ddd}) ${num.slice(0, 5)}-${num.slice(5)}` : `(${ddd}) ${num}`;
  }
  const tipoAtividade = data.cnae_fiscal_descricao?.trim() ?? "";
  let tributacao = "";
  const reg = data.regime_tributario;
  if (Array.isArray(reg) && reg.length > 0) {
    const forma = reg[0]?.forma_de_tributacao;
    if (forma) tributacao = forma;
  }
  const socios = (data.qsa ?? []).map((s) => ({
    nome: s.nome_socio ?? "",
    cpf_socio: s.cnpj_cpf_do_socio ?? "", // BrasilAPI às vezes mascara ***; deixamos como está
  }));
  const capital =
    data.capital_social != null
      ? Number(data.capital_social).toLocaleString("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 })
      : "";
  return {
    razao_social: data.razao_social ?? "",
    cnpj: digits,
    data_abertura: dataAbertura,
    tipo_atividade: tipoAtividade,
    inscricao_estadual: "",
    state_code: String(data.uf ?? "").trim().toUpperCase(),
    city_name: String(data.municipio ?? "").trim(),
    email: data.email ?? "",
    telefone,
    socios: socios.length > 0 ? socios : [{ nome: "", cpf_socio: "" }],
    nome_fantasia: data.nome_fantasia ?? "",
    capital_social: capital,
    natureza_juridica: data.natureza_juridica ?? "",
    porte: data.porte ?? "",
    situacao_cadastral: data.descricao_situacao_cadastral ?? "",
    tributacao,
  };
}

function mapPublicaToFormData(data: CnpjPublicaResponse, digits: string): CnpjFormData {
  const est = data.estabelecimento;
  const ie = est?.inscricoes_estaduais?.[0]?.inscricao_estadual;
  let telefone = "";
  if (est?.ddd1 && est?.telefone1) {
    const n = String(est.telefone1).replace(/\D/g, "");
    telefone = n.length > 8 ? `(${est.ddd1}) ${n.slice(0, 5)}-${n.slice(5)}` : `(${est.ddd1}) ${n}`;
  }
  if (!telefone && est?.ddd2 && est?.telefone2) {
    const n = String(est.telefone2).replace(/\D/g, "");
    telefone = n.length > 8 ? `(${est.ddd2}) ${n.slice(0, 5)}-${n.slice(5)}` : `(${est.ddd2}) ${n}`;
  }
  const simples = data.simples?.simples;
  let tributacao = "";
  if (simples === "Sim") tributacao = "Simples Nacional";
  const cnpjRaw = est?.cnpj ?? data.cnpj_raiz;
  const cnpjDigits = cnpjRaw ? onlyDigits(cnpjRaw) : digits;
  const socios = (data.socios ?? []).map((s) => ({
    nome: s.nome ?? "",
    cpf_socio: s.cpf_cnpj_socio ?? "",
  }));

  const tipoAtividade = est?.atividade_principal?.descricao?.trim() ?? "";

  const naturezaDesc =
    data.natureza_juridica && typeof data.natureza_juridica === "object" && "descricao" in data.natureza_juridica
      ? (data.natureza_juridica as { descricao?: string }).descricao
      : "";
  const porteDesc =
    data.porte && typeof data.porte === "object" && "descricao" in data.porte
      ? (data.porte as { descricao?: string }).descricao
      : "";

  return {
    razao_social: data.razao_social ?? "",
    cnpj: cnpjDigits.length === 14 ? cnpjDigits : digits,
    data_abertura: formatData(est?.data_inicio_atividade),
    tipo_atividade: tipoAtividade,
    inscricao_estadual: ie ? formatIE(ie) : "",
    state_code: String(est?.estado?.sigla ?? "").trim().toUpperCase(),
    city_name: String(est?.cidade?.nome ?? "").trim(),
    email: est?.email ?? "",
    telefone,
    socios: socios.length > 0 ? socios : [{ nome: "", cpf_socio: "" }],
    nome_fantasia: est?.nome_fantasia ?? "",
    capital_social: formatCapital(data.capital_social),
    natureza_juridica: naturezaDesc ?? "",
    porte: porteDesc ?? "",
    situacao_cadastral: est?.situacao_cadastral ?? "",
    tributacao,
  };
}
