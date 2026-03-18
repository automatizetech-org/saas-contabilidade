/**
 * Validações e máscaras para formulários (CNPJ, CPF, e-mail, datas).
 */

export function onlyDigits(s: string): string {
  return String(s ?? "").replace(/\D/g, "");
}

/** Valida CNPJ (14 dígitos + dígitos verificadores). */
export function validateCNPJ(value: string): boolean {
  const d = onlyDigits(value);
  if (d.length !== 14) return false;
  if (/^(\d)\1+$/.test(d)) return false;
  let sum = 0;
  let mul = 5;
  for (let i = 0; i < 12; i++) {
    sum += parseInt(d[i], 10) * mul;
    mul = mul === 2 ? 9 : mul - 1;
  }
  let rest = sum % 11;
  const dv1 = rest < 2 ? 0 : 11 - rest;
  if (dv1 !== parseInt(d[12], 10)) return false;
  sum = 0;
  mul = 6;
  for (let i = 0; i < 13; i++) {
    sum += parseInt(d[i], 10) * mul;
    mul = mul === 2 ? 9 : mul - 1;
  }
  rest = sum % 11;
  const dv2 = rest < 2 ? 0 : 11 - rest;
  return dv2 === parseInt(d[13], 10);
}

/** Valida CPF (11 dígitos + dígitos verificadores). */
export function validateCPF(value: string): boolean {
  const d = onlyDigits(value);
  if (d.length !== 11) return false;
  if (/^(\d)\1+$/.test(d)) return false;
  let sum = 0;
  for (let i = 0; i < 9; i++) sum += parseInt(d[i], 10) * (10 - i);
  let rest = (sum * 10) % 11;
  if (rest === 10) rest = 0;
  if (rest !== parseInt(d[9], 10)) return false;
  sum = 0;
  for (let i = 0; i < 10; i++) sum += parseInt(d[i], 10) * (11 - i);
  rest = (sum * 10) % 11;
  if (rest === 10) rest = 0;
  return rest === parseInt(d[10], 10);
}

/** Formata CNPJ para exibição: 00.000.000/0001-00 */
export function formatCNPJ(value: string): string {
  const d = onlyDigits(value).slice(0, 14);
  if (d.length <= 2) return d;
  if (d.length <= 5) return `${d.slice(0, 2)}.${d.slice(2)}`;
  if (d.length <= 8) return `${d.slice(0, 2)}.${d.slice(2, 5)}.${d.slice(5)}`;
  if (d.length <= 12) return `${d.slice(0, 2)}.${d.slice(2, 5)}.${d.slice(5, 8)}/${d.slice(8)}`;
  return `${d.slice(0, 2)}.${d.slice(2, 5)}.${d.slice(5, 8)}/${d.slice(8, 12)}-${d.slice(12)}`;
}

/** Formata CPF para exibição: 000.000.000-00 */
export function formatCPF(value: string): string {
  const d = onlyDigits(value).slice(0, 11);
  if (d.length <= 3) return d;
  if (d.length <= 6) return `${d.slice(0, 3)}.${d.slice(3)}`;
  if (d.length <= 9) return `${d.slice(0, 3)}.${d.slice(3, 6)}.${d.slice(6)}`;
  return `${d.slice(0, 3)}.${d.slice(3, 6)}.${d.slice(6, 9)}-${d.slice(9)}`;
}

/** Formata como CPF (até 11 dígitos) ou CNPJ (12–14 dígitos) conforme a quantidade de dígitos. */
export function formatCNPJOrCPF(value: string): string {
  const d = onlyDigits(value).slice(0, 14);
  if (d.length <= 11) return formatCPF(d);
  return formatCNPJ(d);
}

/** Validação simples de e-mail. */
export function validateEmail(value: string): boolean {
  const v = String(value ?? "").trim();
  if (!v) return false;
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(v);
}

/** Formata telefone (62) 98248-6434. */
export function formatTelefone(ddd: string, numero: string): string {
  const d = onlyDigits(ddd).slice(0, 2);
  const n = onlyDigits(numero).slice(0, 9);
  if (!d || !n) return d || n ? `${d} ${n}` : "";
  if (n.length <= 4) return `(${d}) ${n}`;
  return `(${d}) ${n.slice(0, 5)}-${n.slice(5)}`;
}

/** Formata telefone a partir de string livre: (00) 00000-0000 */
export function formatTelefoneInput(value: string): string {
  const d = onlyDigits(value).slice(0, 11);
  if (d.length <= 2) return d ? `(${d}` : "";
  if (d.length <= 6) return `(${d.slice(0, 2)}) ${d.slice(2)}`;
  return `(${d.slice(0, 2)}) ${d.slice(2, 7)}-${d.slice(7, 11)}`;
}

/** Formata Competência Inicial: MM/AAAA (apenas dígitos, max 6) */
export function formatCompetencia(value: string): string {
  const d = onlyDigits(value).slice(0, 6);
  if (d.length <= 2) return d;
  return `${d.slice(0, 2)}/${d.slice(2)}`;
}

/** Formata data para DD/MM/AAAA (digita livre, aplica máscara) */
export function formatDataDDMMAAAA(value: string): string {
  const d = onlyDigits(value).slice(0, 8);
  if (d.length <= 2) return d;
  if (d.length <= 4) return `${d.slice(0, 2)}/${d.slice(2)}`;
  return `${d.slice(0, 2)}/${d.slice(2, 4)}/${d.slice(4)}`;
}

/** Formata data curta DD/MM (Data do Primeiro Honorário) */
export function formatDataDDMM(value: string): string {
  const d = onlyDigits(value).slice(0, 4);
  if (d.length <= 2) return d;
  return `${d.slice(0, 2)}/${d.slice(2)}`;
}

/** Formata valor em reais para input: 1.234,56 (padrão BR) */
export function formatCurrencyBRL(value: string): string {
  const digits = onlyDigits(value).slice(0, 15);
  if (digits.length === 0) return "";
  const centavos = digits.slice(-2).padStart(2, "0");
  let inteiros = digits.slice(0, -2) || "0";
  inteiros = inteiros.replace(/^0+/, "") || "0";
  const withDots = inteiros.replace(/\B(?=(\d{3})+(?!\d))/g, ".");
  return `${withDots},${centavos}`;
}

/** Retorna apenas os dígitos do valor (para armazenar e formatar na exibição) */
export function currencyToDigits(value: string): string {
  return onlyDigits(value).slice(0, 15);
}

/** Remove formatação de moeda e retorna valor numérico (para armazenar) */
export function parseCurrencyBRL(value: string): number {
  const d = onlyDigits(value);
  if (d.length === 0) return 0;
  return parseInt(d, 10) / 100;
}
