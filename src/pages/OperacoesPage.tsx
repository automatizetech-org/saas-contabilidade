import { StatsCard } from "@/components/dashboard/StatsCard";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { Activity, Clock, AlertTriangle, CheckCircle2, Server } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { cn } from "@/utils";
import { getRobots } from "@/services/robotsService";
import { getRecentExecutionRequests } from "@/services/executionRequestsService";
import { useMemo } from "react";
import { getOperationsOverview } from "@/services/operationsService";

function robotStatusLabel(status: "active" | "inactive" | "processing"): string {
  if (status === "processing") return "Executando";
  if (status === "active") return "Online";
  return "Inativo";
}

function eventStatusLabel(status: "pending" | "running" | "completed" | "failed"): "sucesso" | "erro" | "processando" {
  if (status === "completed") return "sucesso";
  if (status === "failed") return "erro";
  return "processando";
}

function formatEventTime(iso: string): string {
  const date = new Date(iso);
  const now = new Date();
  const today = now.toDateString();
  const yesterday = new Date(now);
  yesterday.setDate(yesterday.getDate() - 1);
  if (date.toDateString() === today) {
    return date.toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" });
  }
  if (date.toDateString() === yesterday.toDateString()) {
    return "Ontem " + date.toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" });
  }
  return date.toLocaleString("pt-BR", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
}

export default function OperacoesPage() {
  const { data: robots = [] } = useQuery({ queryKey: ["robots"], queryFn: getRobots });
  const { data: executions = [] } = useQuery({
    queryKey: ["execution-requests"],
    queryFn: () => getRecentExecutionRequests(50),
  });
  const { data: overview } = useQuery({
    queryKey: ["operations-overview"],
    queryFn: getOperationsOverview,
  });

  const robotMap = useMemo(() => new Map(robots.map((robot) => [robot.technical_id, robot.display_name])), [robots]);

  const eventLabel = (robotTechnicalIds: string[]) => {
    const first = robotTechnicalIds?.[0];
    if (!first) return "Execucao";
    const name = robotMap.get(first);
    return name ?? (first.toLowerCase().includes("nfs") ? "Coleta XML NFS" : first);
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold font-display tracking-tight">Operacoes e Sincronizacao</h1>
        <p className="text-sm text-muted-foreground mt-1">Status dos robos, eventos de sincronizacao e metricas.</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <StatsCard
          title="Eventos hoje"
          value={String(overview?.eventosHoje ?? 0)}
          change={(overview?.eventosOntem ?? 0) > 0 ? `Ontem: ${overview?.eventosOntem ?? 0}` : undefined}
          changeType="neutral"
          icon={Activity}
        />
        <StatsCard
          title="Taxa de sucesso"
          value={`${(overview?.taxaSucesso ?? 0).toFixed(1)}%`}
          changeType={(overview?.taxaSucesso ?? 0) >= 90 ? "positive" : (overview?.taxaSucesso ?? 0) >= 70 ? "neutral" : "negative"}
          icon={CheckCircle2}
        />
        <StatsCard title="Falhas" value={String(overview?.falhas ?? 0)} changeType="negative" icon={AlertTriangle} />
        <StatsCard title="Robos" value={String(overview?.robots ?? robots.length)} changeType="neutral" icon={Clock} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <GlassCard className="p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-sm font-semibold font-display">Status dos Servicos</h3>
            <Server className="h-4 w-4 text-muted-foreground" />
          </div>
          <div className="space-y-3">
            {robots.length === 0 ? (
              <p className="text-xs text-muted-foreground">Nenhum robo configurado.</p>
            ) : (
              robots.map((robot) => (
                <div key={robot.id} className="flex items-center justify-between py-2 border-b border-border last:border-0">
                  <div className="flex items-center gap-3">
                    <div
                      className={cn(
                        "h-2 w-2 rounded-full",
                        robot.status === "processing" && "bg-amber-500 animate-pulse",
                        robot.status === "active" && "bg-success animate-pulse",
                        robot.status === "inactive" && "bg-muted"
                      )}
                    />
                    <span className="text-xs font-medium">{robot.display_name}</span>
                  </div>
                  <span className="text-[10px] font-medium uppercase text-muted-foreground">
                    {robotStatusLabel(robot.status)}
                  </span>
                </div>
              ))
            )}
          </div>
        </GlassCard>

        <GlassCard className="p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-sm font-semibold font-display">Log de Eventos da Sincronizacao</h3>
          </div>
          <p className="text-xs text-muted-foreground mb-3">Ultimas execucoes dos robos.</p>
          <div className="space-y-2 max-h-[280px] overflow-y-auto">
            {executions.length === 0 ? (
              <p className="text-xs text-muted-foreground">Nenhum evento recente.</p>
            ) : (
              executions.map((execution) => (
                <div
                  key={execution.id}
                  className="flex items-start justify-between gap-2 py-2 border-b border-border last:border-0 text-xs"
                >
                  <div className="min-w-0">
                    <p className="font-medium truncate">{eventLabel(execution.robot_technical_ids)}</p>
                    <p className="text-[10px] text-muted-foreground">
                      {formatEventTime(execution.completed_at ?? execution.created_at)}
                      {execution.status === "failed" && execution.error_message && (
                        <> · {execution.error_message.length > 60 ? `${execution.error_message.slice(0, 60)}...` : execution.error_message}</>
                      )}
                    </p>
                  </div>
                  <StatusBadge status={eventStatusLabel(execution.status)} className="shrink-0" />
                </div>
              ))
            )}
          </div>
        </GlassCard>
      </div>

      <GlassCard className="overflow-hidden">
        <div className="p-4 border-b border-border">
          <h3 className="text-sm font-semibold font-display">Historico de Execucoes</h3>
          <p className="text-xs text-muted-foreground">Ultimas 50 execucoes com status e mensagem de erro quando houver.</p>
        </div>
        <div className="divide-y divide-border overflow-x-auto">
          {executions.length === 0 ? (
            <div className="p-6 text-center text-sm text-muted-foreground">Nenhuma execucao encontrada.</div>
          ) : (
            executions.map((execution) => (
              <div
                key={execution.id}
                className="px-4 py-3 flex flex-wrap items-center justify-between gap-2 hover:bg-muted/30 transition-colors"
              >
                <div className="flex items-center gap-3 min-w-0">
                  <StatusBadge status={eventStatusLabel(execution.status)} />
                  <div className="min-w-0">
                    <p className="text-xs font-medium truncate">{eventLabel(execution.robot_technical_ids)}</p>
                    <p className="text-[10px] text-muted-foreground">
                      {new Date(execution.completed_at ?? execution.created_at).toLocaleString("pt-BR")}
                      {execution.status === "failed" && execution.error_message && (
                        <span className="block mt-0.5 text-destructive">{execution.error_message}</span>
                      )}
                    </p>
                  </div>
                </div>
              </div>
            ))
          )}
        </div>
      </GlassCard>
    </div>
  );
}
