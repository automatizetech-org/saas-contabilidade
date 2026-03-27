import { AlertCircle, CheckCircle2, Clock3, Download, Eye, Loader2 } from "lucide-react";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { Progress } from "@/components/ui/progress";
import { Button } from "@/components/ui/button";
import { cn } from "@/utils";
import type { DeclarationRunItem, DeclarationRunState } from "../types";
import { getProgressMetrics } from "../helpers";

type DeclarationProcessingPanelProps = {
  run: DeclarationRunState | null;
  loading?: boolean;
  onOpenArtifact?: (run: DeclarationRunState, item: DeclarationRunItem) => void;
  onDownloadArtifact?: (run: DeclarationRunState, item: DeclarationRunItem) => void;
  onClearHistory?: (runId: string) => void;
};

export function DeclarationProcessingPanel({
  run,
  loading = false,
  onOpenArtifact,
  onDownloadArtifact,
  onClearHistory,
}: DeclarationProcessingPanelProps) {
  if (!run) {
    return (
      <GlassCard className="border border-border/70 p-6">
        <div className="space-y-2">
          <h3 className="text-lg font-semibold font-display tracking-tight">Acompanhamento do processamento</h3>
          <p className="text-sm text-muted-foreground">
            Quando uma emissao, recalculo ou solicitacao for iniciada, o andamento por empresa aparecera aqui.
          </p>
        </div>
      </GlassCard>
    );
  }

  const successCount = run.items.filter((item) => item.status === "sucesso").length;
  const errorCount = run.items.filter((item) => item.status === "erro").length;
  const processingCount = run.items.filter((item) => item.status === "processando").length;
  const completedCount = successCount + errorCount;
  const metrics = getProgressMetrics(run.items.length, completedCount);

  return (
    <GlassCard className="border border-border/70 p-6">
      <div className="space-y-5">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div className="space-y-2">
            <div className="flex flex-wrap items-center gap-2">
              <h3 className="text-lg font-semibold font-display tracking-tight">{run.title}</h3>
              <StatusBadge status={run.terminal ? (errorCount > 0 ? "divergente" : "sucesso") : "processando"} />
            </div>
            <p className="text-sm text-muted-foreground">
              Acompanhe o status individual de cada empresa e os artefatos gerados ao final.
            </p>
          </div>

          <div className="space-y-3">
            <div className="grid grid-cols-2 gap-2 rounded-2xl border border-border bg-background/60 p-3 text-xs sm:grid-cols-4">
              <div>
                <p className="text-muted-foreground">Concluidas</p>
                <p className="mt-1 text-lg font-semibold">{metrics.completed}/{metrics.total}</p>
              </div>
              <div>
                <p className="text-muted-foreground">Sucesso</p>
                <p className="mt-1 text-lg font-semibold text-success">{successCount}</p>
              </div>
              <div>
                <p className="text-muted-foreground">Erro</p>
                <p className="mt-1 text-lg font-semibold text-destructive">{errorCount}</p>
              </div>
              <div>
                <p className="text-muted-foreground">Em andamento</p>
                <p className="mt-1 text-lg font-semibold text-info">{processingCount}</p>
              </div>
            </div>
            {onClearHistory ? (
              <div className="flex justify-end">
                <Button type="button" variant="outline" size="sm" onClick={() => onClearHistory(run.runId)}>
                  {run.terminal ? "Limpar historico" : "Limpar acompanhamento"}
                </Button>
              </div>
            ) : null}
          </div>
        </div>

        <div className="space-y-2">
          <div className="flex items-center justify-between gap-3 text-sm">
            <span className="text-muted-foreground">Progresso da conclusao</span>
            <strong>
              {metrics.completed}/{metrics.total} • {metrics.percent}%
            </strong>
          </div>
          <Progress value={metrics.percent} className="h-2.5" />
          {loading && !run.terminal ? (
            <p className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
              <Loader2 className="h-3.5 w-3.5 animate-spin" />
              Atualizando status do processamento...
            </p>
          ) : null}
        </div>

        <div className="space-y-3">
          {run.items.map((item) => {
            const hasArtifact = Boolean(
              item.artifact?.filePath || item.artifact?.url || item.artifact?.artifactKey,
            );
            return (
              <div
                key={`${run.runId}-${item.companyId}`}
                className={cn(
                  "rounded-2xl border border-border bg-background/70 p-4",
                  item.status === "sucesso" && "border-success/30",
                  item.status === "erro" && "border-destructive/25",
                )}
              >
                <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                  <div className="min-w-0 space-y-2">
                    <div className="flex flex-wrap items-center gap-2">
                      <p className="truncate text-sm font-semibold">{item.companyName}</p>
                      <StatusBadge status={item.status} />
                    </div>
                    <p className="text-xs text-muted-foreground">{item.companyDocument || "CNPJ nao informado"}</p>
                    <p className="text-sm">{item.message}</p>
                  </div>

                  <div className="flex shrink-0 items-center gap-2">
                    {item.status === "sucesso" ? (
                      <CheckCircle2 className="h-4 w-4 text-success" />
                    ) : item.status === "erro" ? (
                      <AlertCircle className="h-4 w-4 text-destructive" />
                    ) : (
                      <Clock3 className="h-4 w-4 text-info" />
                    )}
                    {hasArtifact ? (
                      <>
                        <Button type="button" variant="outline" size="sm" onClick={() => onOpenArtifact?.(run, item)}>
                          <Eye className="h-4 w-4" />
                          Visualizar
                        </Button>
                        <Button type="button" size="sm" onClick={() => onDownloadArtifact?.(run, item)}>
                          <Download className="h-4 w-4" />
                          {item.artifact?.label || "Baixar"}
                        </Button>
                      </>
                    ) : null}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </GlassCard>
  );
}
