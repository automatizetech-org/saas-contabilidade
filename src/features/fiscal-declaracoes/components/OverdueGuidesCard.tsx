import { AlertTriangle, FileClock, RefreshCcw } from "lucide-react";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { Button } from "@/components/ui/button";
import type { OverdueGuide } from "../types";
import { formatCompetenceLabel, formatCurrencyFromCents, formatDateLabel } from "../helpers";

type OverdueGuidesCardProps = {
  guides: OverdueGuide[];
  busy?: boolean;
  onRecalculate: (guide: OverdueGuide) => void;
};

export function OverdueGuidesCard({
  guides,
  busy = false,
  onRecalculate,
}: OverdueGuidesCardProps) {
  return (
    <GlassCard className="border border-border/70 p-6">
      <div className="space-y-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-destructive/10 text-destructive">
                <FileClock className="h-5 w-5" />
              </div>
              <div>
                <h3 className="text-lg font-semibold font-display tracking-tight">Guias vencidas</h3>
                <p className="text-sm text-muted-foreground">
                  Este bloco ignora a competência padrão e mostra tudo que já está vencido no contexto visível.
                </p>
              </div>
            </div>
          </div>
          <div className="rounded-2xl border border-border bg-background/60 px-4 py-3 text-right">
            <p className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Total</p>
            <p className="mt-1 text-2xl font-semibold font-display">{guides.length}</p>
          </div>
        </div>

        {guides.length === 0 ? (
          <div className="rounded-2xl border border-dashed border-border bg-muted/20 px-4 py-6 text-sm text-muted-foreground">
            Nenhuma guia vencida disponível para as empresas selecionadas no momento.
          </div>
        ) : (
          <div className="space-y-3">
            {guides.map((guide) => (
              <div
                key={guide.id}
                className="flex flex-col gap-4 rounded-2xl border border-border bg-background/70 p-4 lg:flex-row lg:items-center lg:justify-between"
              >
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2">
                    <p className="truncate text-sm font-semibold">{guide.companyName}</p>
                    <StatusBadge status={guide.status} />
                  </div>
                  <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
                    <span>Competência: {formatCompetenceLabel(guide.competence)}</span>
                    <span>Vencimento: {formatDateLabel(guide.dueDate)}</span>
                    <span>Valor: {formatCurrencyFromCents(guide.amountCents)}</span>
                    {guide.referenceLabel ? <span>Referência: {guide.referenceLabel}</span> : null}
                  </div>
                </div>

                <Button type="button" disabled={busy} onClick={() => onRecalculate(guide)}>
                  <RefreshCcw className="h-4 w-4" />
                  Recalcular
                </Button>
              </div>
            ))}
          </div>
        )}

        <div className="rounded-2xl border border-amber-500/20 bg-amber-500/5 p-4 text-sm text-amber-100">
          <p className="flex items-start gap-2 text-amber-700 dark:text-amber-300">
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
            O recálculo reutiliza o mesmo modal de emissão, já com a empresa selecionada e o campo de novo vencimento habilitado.
          </p>
        </div>
      </div>
    </GlassCard>
  );
}
