/**
 * Consulta CNPJ em APIs públicas (Receita/dados abertos).
 * 1) publica.cnpj.ws — dados completos incluindo sócios com CPF.
 * 2) Fallback: BrasilAPI quando a primeira falhar ou retornar 404.
 * Usar apenas dígitos (sem máscara) na URL.
 *
 * Limite da API pública CNPJ.ws (mesmo IP): 3 requisições por minuto; 429 após exceder;
 * volume excessivo pode bloquear por 1 h. Ref.: https://docs.cnpj.ws/referencia-de-api/api-publica/limitacoes
 * BrasilAPI: outro host — fila de throttle própria (~65/min conservador); 429 tratado com pausa curta.
 * Na importação em lote usamos as duas filas em paralelo (vários workers): enquanto uns esperam a publica, outros consomem a fila da BrasilAPI.
 */

const BASE_PUBLICA = "https://publica.cnpj.ws/cnpj";
const BASE_BRASIL_API = "https://brasilapi.com.br/api/cnpj/v1";

/** ≥20 s entre inícios de chamada à publica ⇒ no máx. ~3/min (regra oficial). */
const PUBLICA_CNPJ_MIN_INTERVAL_MS = 21_000;
const PUBLICA_429_COOLDOWN_MS = 61_000;

/** BrasilAPI: intervalo próprio (limite não é o mesmo da publica.cnpj.ws). */
const BRASIL_API_MIN_INTERVAL_MS = 950;
const BRASIL_API_429_COOLDOWN_MS = 5_000;

let nextPublicaAllowedAt = 0;
let nextBrasilAllowedAt = 0;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function waitPublicaRateLimitSlot(): Promise<void> {
  const now = Date.now();
  if (now < nextPublicaAllowedAt) {
    await sleep(nextPublicaAllowedAt - now);
  }
  nextPublicaAllowedAt = Date.now() + PUBLICA_CNPJ_MIN_INTERVAL_MS;
}

async function fetchPublicaCnpjResponse(digits: string): Promise<Response> {
  await waitPublicaRateLimitSlot();
  let res = await fetch(`${BASE_PUBLICA}/${digits}`, {
    method: "GET",
    headers: { Accept: "application/json" },
  });
  if (res.status === 429) {
    await sleep(PUBLICA_429_COOLDOWN_MS);
    await waitPublicaRateLimitSlot();
    res = await fetch(`${BASE_PUBLICA}/${digits}`, {
      method: "GET",
      headers: { Accept: "application/json" },
    });
  }
  return res;
}

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

async function waitBrasilRateLimitSlot(): Promise<void> {
  const now = Date.now();
  if (now < nextBrasilAllowedAt) {
    await sleep(nextBrasilAllowedAt - now);
  }
  nextBrasilAllowedAt = Date.now() + BRASIL_API_MIN_INTERVAL_MS;
}

function hasUsableCnpjFormData(data: CnpjFormData | null | undefined): boolean {
  if (!data) return false;
  return Boolean(data.razao_social?.trim() || data.nome_fantasia?.trim());
}

/** GET BrasilAPI com fila própria (não conta no limite da publica.cnpj.ws). */
async function fetchBrasilApiThrottled(digits: string): Promise<CnpjFormData | null> {
  await waitBrasilRateLimitSlot();
  const doFetch = () =>
    fetch(`${BASE_BRASIL_API}/${digits}`, {
      method: "GET",
      headers: { Accept: "application/json" },
    });
  try {
    let res = await doFetch();
    if (res.status === 429) {
      await sleep(BRASIL_API_429_COOLDOWN_MS);
      await waitBrasilRateLimitSlot();
      res = await doFetch();
    }
    if (!res.ok) return null;
    const data: BrasilApiResponse = await res.json();
    return mapBrasilApiToFormData(data, digits);
  } catch {
    return null;
  }
}

/**
 * Importação em lote: 1) BrasilAPI (rápida); 2) só se faltar nome/dados, publica.cnpj.ws (lenta, mais completa).
 * Cada API tem throttle global — várias consultas em paralelo intercalam as filas.
 */
async function fetchCnpjMultiSourceForBulk(digits: string): Promise<CnpjFormData> {
  const fromBrasil = await fetchBrasilApiThrottled(digits);
  if (hasUsableCnpjFormData(fromBrasil)) return fromBrasil!;

  try {
    const res = await fetchPublicaCnpjResponse(digits);
    if (res.ok) {
      const json: CnpjPublicaResponse = await res.json();
      const mapped = mapPublicaToFormData(json, digits);
      if (hasUsableCnpjFormData(mapped)) return mapped;
      throw new Error("CNPJ não encontrado.");
    }
    if (res.status === 404) {
      throw new Error("CNPJ não encontrado.");
    }
    if (hasUsableCnpjFormData(fromBrasil)) return fromBrasil!;
    throw new Error("API indisponível ou sem retorno.");
  } catch (e) {
    if (e instanceof Error && e.message === "CNPJ não encontrado.") throw e;
    if (hasUsableCnpjFormData(fromBrasil)) return fromBrasil!;
    throw e instanceof Error ? e : new Error("API indisponível ou sem retorno.");
  }
}

export async function fetchCnpjPublica(cnpjApenasDigitos: string): Promise<CnpjFormData | null> {
  const digits = onlyDigits(cnpjApenasDigitos);
  if (digits.length !== 14) return null;

  // 1) Tentar publica.cnpj.ws (dados mais completos, incluindo CPF dos sócios)
  try {
    const res = await fetchPublicaCnpjResponse(digits);
    if (res.ok) {
      const data: CnpjPublicaResponse = await res.json();
      return mapPublicaToFormData(data, digits);
    }
    if (res.status === 404) {
      // CNPJ não encontrado na publica; tentar BrasilAPI
      const fallback = await fetchBrasilApiThrottled(digits);
      if (fallback) return fallback;
      throw new Error("CNPJ não encontrado.");
    }
    throw new Error("API indisponível ou sem retorno.");
  } catch (e) {
    if (e instanceof Error && e.message === "CNPJ não encontrado.") throw e;
    // Erro de rede ou outro: tentar BrasilAPI como fallback
    const fallback = await fetchBrasilApiThrottled(digits);
    if (fallback) return fallback;
    throw e instanceof Error ? e : new Error("API indisponível ou sem retorno.");
  }
}

export type CnpjPublicaRetryResult =
  | { ok: true; data: CnpjFormData }
  | { ok: false; kind: "not_found"; message: string }
  | { ok: false; kind: "transient"; message: string }

/**
 * Várias tentativas com espera crescente entre elas (rede / rate limit / API instável).
 * "CNPJ não encontrado." não repete — é falha definitiva após fallbacks internos de fetchCnpjPublica.
 */
export async function fetchCnpjPublicaWithRetries(
  cnpjApenasDigitos: string,
  opts?: {
    maxAttempts?: number
    delayMs?: number
    onRetry?: (info: { attempt: number; maxAttempts: number; lastMessage: string }) => void
    /**
     * true = BrasilAPI + publica.cnpj.ws em filas separadas (ideal para importação em lote em paralelo).
     * false/omitido = fluxo tela única (publica primeiro, depois BrasilAPI).
     */
    multiSourceBulk?: boolean
  },
): Promise<CnpjPublicaRetryResult> {
  const maxAttempts = Math.max(1, opts?.maxAttempts ?? 4)
  /** Entre tentativas; chamadas à publica já são espaçadas (~21s) pelo rate limit. */
  const delayMs = Math.max(200, opts?.delayMs ?? 800)
  const digits = onlyDigits(cnpjApenasDigitos)
  if (digits.length !== 14) {
    return { ok: false, kind: "not_found", message: "CNPJ inválido para consulta." }
  }

  const multi = opts?.multiSourceBulk === true

  let lastMsg = "API indisponível ou sem retorno."

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const data = multi ? await fetchCnpjMultiSourceForBulk(digits) : await fetchCnpjPublica(digits)
      if (data) return { ok: true, data }
      lastMsg = "Resposta vazia da consulta."
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e)
      lastMsg = msg
      if (msg === "CNPJ não encontrado.") {
        return { ok: false, kind: "not_found", message: msg }
      }
    }
    if (attempt < maxAttempts) {
      opts?.onRetry?.({ attempt, maxAttempts, lastMessage: lastMsg })
      await sleep(delayMs * attempt)
    }
  }

  return { ok: false, kind: "transient", message: lastMsg }
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
