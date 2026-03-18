/**
 * Salário mínimo — API BCB (série 1619).
 * Uso: qualificação do plano por % do honorário sobre o salário mínimo.
 */

const BCB_SALARIO_MINIMO_URL =
  "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1619/dados/ultimos/1?formato=json";

export type QualificacaoPlano = "BRONZE" | "PRATA" | "OURO" | "DIAMANTE";

/** Retorna o último valor do salário mínimo (número em reais) ou null se falhar. */
export async function fetchSalarioMinimoBCB(): Promise<number | null> {
  try {
    const res = await fetch(BCB_SALARIO_MINIMO_URL, {
      method: "GET",
      headers: { Accept: "application/json" },
    });
    if (!res.ok) return null;
    const data = await res.json();
    const item = Array.isArray(data) && data.length > 0 ? data[0] : null;
    if (!item || typeof item.valor !== "string") return null;
    const valor = parseFloat(item.valor.replace(",", "."));
    return Number.isFinite(valor) ? valor : null;
  } catch {
    return null;
  }
}

/**
 * Calcula a qualificação do plano com base no valor do honorário e no salário mínimo.
 * Até 30% = BRONZE | 30,01% a 50% = PRATA | 50,01% a 100% = OURO | Acima de 100% = DIAMANTE
 */
export function qualificacaoFromHonorario(
  valorHonorarioReais: number,
  salarioMinimo: number
): QualificacaoPlano {
  if (!Number.isFinite(valorHonorarioReais) || !Number.isFinite(salarioMinimo) || salarioMinimo <= 0) {
    return "BRONZE";
  }
  const percentual = (valorHonorarioReais / salarioMinimo) * 100;
  if (percentual <= 30) return "BRONZE";
  if (percentual <= 50) return "PRATA";
  if (percentual <= 100) return "OURO";
  return "DIAMANTE";
}

/** Configuração de exibição: emoji + classe de cor para cada qualificação */
export const QUALIFICACAO_DISPLAY: Record<
  QualificacaoPlano,
  { emoji: string; label: string; className: string }
> = {
  BRONZE: {
    emoji: "🥉",
    label: "BRONZE",
    className: "text-amber-700 dark:text-amber-400",
  },
  PRATA: {
    emoji: "🥈",
    label: "PRATA",
    className: "text-slate-500 dark:text-slate-300",
  },
  OURO: {
    emoji: "🥇",
    label: "OURO",
    className: "text-amber-500 dark:text-yellow-400",
  },
  DIAMANTE: {
    emoji: "💎",
    label: "DIAMANTE",
    className: "text-cyan-400 dark:text-sky-300",
  },
};
