import type { Json } from "@/types/database";

export function getDefaultDeclarationCompetence(today = new Date()): string {
  const base = new Date(today.getFullYear(), today.getMonth() - 1, 1);
  return `${base.getFullYear()}-${String(base.getMonth() + 1).padStart(2, "0")}`;
}

export function formatCompetenceLabel(value: string | null | undefined): string {
  const raw = String(value ?? "").trim();
  if (!/^\d{4}-\d{2}$/.test(raw)) return "-";
  const year = raw.slice(0, 4);
  const month = raw.slice(5, 7);
  return `${month}/${year}`;
}

export function formatYearLabel(value: string | null | undefined): string {
  const raw = String(value ?? "").trim();
  return /^\d{4}$/.test(raw) ? raw : "-";
}

export function formatDateLabel(value: string | null | undefined): string {
  const raw = String(value ?? "").trim().slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return "-";
  const [year, month, day] = raw.split("-");
  return `${day}/${month}/${year}`;
}

export function formatCurrencyFromCents(value: number | null | undefined): string {
  if (!Number.isFinite(value)) return "-";
  return ((value ?? 0) / 100).toLocaleString("pt-BR", {
    style: "currency",
    currency: "BRL",
  });
}

export function sanitizeDeclarationError(error: unknown, fallback: string): string {
  const message =
    error instanceof Error
      ? error.message
      : typeof error === "string"
        ? error
        : fallback;
  const raw = String(message || "").trim();
  if (!raw) return fallback;
  if (
    raw.toLowerCase().includes("permission denied") ||
    raw.toLowerCase().includes("row-level security")
  ) {
    return "Voce nao tem permissao para executar esta rotina.";
  }
  return raw;
}

export function isValidCompetence(value: string): boolean {
  if (!/^\d{4}-\d{2}$/.test(value)) return false;
  const month = Number(value.slice(5, 7));
  return month >= 1 && month <= 12;
}

export function isValidYear(value: string): boolean {
  if (!/^\d{4}$/.test(value)) return false;
  const year = Number(value);
  return year >= 2000 && year <= 2100;
}

export function isValidIsoDate(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return false;
  const date = new Date(`${value}T12:00:00`);
  return !Number.isNaN(date.getTime()) && date.toISOString().slice(0, 10) === value;
}

export function toPeriodRange(competence: string): { periodStart: string; periodEnd: string } {
  const year = Number(competence.slice(0, 4));
  const month = Number(competence.slice(5, 7));
  const periodStart = `${competence}-01`;
  const periodEnd = new Date(year, month, 0).toISOString().slice(0, 10);
  return { periodStart, periodEnd };
}

export function getProgressMetrics(total: number, completed: number) {
  const safeTotal = Math.max(total, 0);
  const safeCompleted = Math.min(Math.max(completed, 0), safeTotal || 0);
  const percent = safeTotal === 0 ? 0 : Math.round((safeCompleted / safeTotal) * 100);
  return {
    completed: safeCompleted,
    total: safeTotal,
    percent,
  };
}

export function createRunId() {
  return `declaration-run-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export function asObject(value: Json | null | undefined): Record<string, Json> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, Json>;
}

export function asArray(value: Json | null | undefined): Json[] {
  return Array.isArray(value) ? value : [];
}
