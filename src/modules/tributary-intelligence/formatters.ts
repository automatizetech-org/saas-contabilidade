export function roundTo(value: number, decimals = 2) {
  const factor = 10 ** decimals
  return Math.round((value + Number.EPSILON) * factor) / factor
}

export function formatCurrencyBRL(value: number | null | undefined) {
  const safeValue = Number.isFinite(value) ? Number(value) : 0
  return safeValue.toLocaleString("pt-BR", {
    style: "currency",
    currency: "BRL",
    minimumFractionDigits: 2,
  })
}

export function formatPercentBRL(value: number, decimals = 2) {
  return `${value.toLocaleString("pt-BR", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  })}%`
}

export function parseCurrencyInput(raw: string) {
  const normalized = raw.replace(/[^\d,.-]/g, "").replace(/\./g, "").replace(",", ".")
  const parsed = Number(normalized)
  return Number.isFinite(parsed) ? parsed : 0
}

export function currencyInputValue(value: number) {
  return value === 0 ? "" : formatCurrencyBRL(value)
}

export function formatMonthLabel(referenceMonth: string) {
  const [year, month] = referenceMonth.split("-").map(Number)
  return new Date(year, month - 1, 1).toLocaleDateString("pt-BR", {
    month: "short",
    year: "2-digit",
  })
}

export function formatPeriodLabel(referenceMonth: string) {
  const [year, month] = referenceMonth.split("-")
  return `${month}/${year}`
}
