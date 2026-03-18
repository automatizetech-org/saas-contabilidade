import { StatsCard } from "@/components/dashboard/StatsCard";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { Activity, Clock, AlertTriangle, CheckCircle2, Server } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { cn } from "@/utils";
import { getRobots } from "@/services/robotsService";
import { getRecentExecutionRequests } from "@/services/executionRequestsService";
import { useMemo } from "react";

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
  const d = new Date(iso);
  const now = new Date();
  const today = now.toDateString();
  const yesterday = new Date(now);
  yesterday.setDate(yesterday.getDate() - 1);
  if (d.toDateString() === today) {
    return d.toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" });
  }
  if (d.toDateString() === yesterday.toDateString()) {
    return "Ontem " + d.toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" });
  }
  return d.toLocaleString("pt-BR", { day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
}

export default function OperacoesPage() {
  const { data: robots = [] } = useQuery({ queryKey: ["robots"], queryFn: getRobots });
  const { data: executions = [] } = useQuery({
    queryKey: ["execution-requests"],
    queryFn: () => getRecentExecutionRequests(50),
  });

  const robotMap = useMemo(() => new Map(robots.map((r) => [r.technical_id, r.display_name])), [robots]);

  const metrics = useMemo(() => {
    const now = new Date();
    const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const yesterdayStart = new Date(todayStart);
    yesterdayStart.setDate(yesterdayStart.getDate() - 1);
    let todayCount = 0;
    let yesterdayCount = 0;
    let successCount = 0;
    let failCount = 0;
    const completed = executions.filter((e) => e.status === "completed" || e.status === "failed");
    for (const e of completed) {
      const t = new Date(e.completed_at ?? e.created_at).getTime();
      if (t >= todayStart.getTime()) todayCount++;
      else if (t >= yesterdayStart.getTime() && t < todayStart.getTime()) yesterdayCount++;
      if (e.status === "completed") successCount++;
      else if (e.status === "failed") failCount++;
    }
    const total = completed.length;
    const successRate = total ? ((successCount / total) * 100).toFixed(1) : "0";
    return {
      eventosHoje: todayCount,
      eventosOntem: yesterdayCount,
      taxaSucesso: successRate,
      falhas: failCount,
    };
  }, [executions]);

  const eventLabel = (robotTechnicalIds: string[]) => {
    const first = robotTechnicalIds?.[0];
    if (!first) return "Execução";
    const name = robotMap.get(first);
    return name ?? (first.toLowerCase().includes("nfs") ? "Coleta XML NFS" : first);
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold font-display tracking-tight">Operações e Sincronização</h1>
        <p className="text-sm text-muted-foreground mt-1">Status dos robôs, eventos de sincronização e métricas</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <StatsCard
          title="Eventos hoje"
          value={metrics.eventosHoje.toString()}
          change={metrics.eventosOntem > 0 ? `Ontem: ${metrics.eventosOntem}` : undefined}
          changeType="neutral"
          icon={Activity}
        />
        <StatsCard
          title="Taxa de sucesso"
          value={`${metrics.taxaSucesso}%`}
          changeType={Number(metrics.taxaSucesso) >= 90 ? "positive" : Number(metrics.taxaSucesso) >= 70 ? "neutral" : "negative"}
          icon={CheckCircle2}
        />
        <StatsCard title="Falhas" value={metrics.falhas.toString()} changeType="negative" icon={AlertTriangle} />
        <StatsCard title="Robôs" value={robots.length.toString()} changeType="neutral" icon={Clock} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <GlassCard className="p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-sm font-semibold font-display">Status dos Serviços</h3>
            <Server className="h-4 w-4 text-muted-foreground" />
          </div>
          <div className="space-y-3">
            {robots.length === 0 ? (
              <p className="text-xs text-muted-foreground">Nenhum robô configurado.</p>
            ) : (
              robots.map((r) => (
                <div key={r.id} className="flex items-center justify-between py-2 border-b border-border last:border-0">
                  <div className="flex items-center gap-3">
                    <div
                      className={cn(
                        "h-2 w-2 rounded-full",
                        r.status === "processing" && "bg-amber-500 animate-pulse",
                        r.status === "active" && "bg-success animate-pulse",
                        r.status === "inactive" && "bg-muted"
                      )}
                    />
                    <span className="text-xs font-medium">{r.display_name}</span>
                  </div>
                  <span className="text-[10px] font-medium uppercase text-muted-foreground">
                    {robotStatusLabel(r.status)}
                  </span>
                </div>
              ))
            )}
          </div>
        </GlassCard>

        <GlassCard className="p-6">
          <div className="flex items-center justify-between mb-4">
            <h3 className="text-sm font-semibold font-display">Log de Eventos da Sincronização</h3>
          </div>
          <p className="text-xs text-muted-foreground mb-3">Últimas execuções dos robôs</p>
          <div className="space-y-2 max-h-[280px] overflow-y-auto">
            {executions.length === 0 ? (
              <p className="text-xs text-muted-foreground">Nenhum evento recente.</p>
            ) : (
              executions.map((ev) => (
                <div
                  key={ev.id}
                  className="flex items-start justify-between gap-2 py-2 border-b border-border last:border-0 text-xs"
                >
                  <div className="min-w-0">
                    <p className="font-medium truncate">{eventLabel(ev.robot_technical_ids)}</p>
                    <p className="text-[10px] text-muted-foreground">
                      {formatEventTime(ev.completed_at ?? ev.created_at)}
                      {ev.status === "failed" && ev.error_message && (
                        <> · {ev.error_message.length > 60 ? ev.error_message.slice(0, 60) + "…" : ev.error_message}</>
                      )}
                    </p>
                  </div>
                  <StatusBadge status={eventStatusLabel(ev.status)} className="shrink-0" />
                </div>
              ))
            )}
          </div>
        </GlassCard>
      </div>

      <GlassCard className="overflow-hidden">
        <div className="p-4 border-b border-border">
          <h3 className="text-sm font-semibold font-display">Histórico de Execuções</h3>
          <p className="text-xs text-muted-foreground">Últimas 50 execuções com status e mensagem de erro quando houver</p>
        </div>
        <div className="divide-y divide-border overflow-x-auto">
          {executions.length === 0 ? (
            <div className="p-6 text-center text-sm text-muted-foreground">Nenhuma execução encontrada.</div>
          ) : (
            executions.map((ev) => (
              <div
                key={ev.id}
                className="px-4 py-3 flex flex-wrap items-center justify-between gap-2 hover:bg-muted/30 transition-colors"
              >
                <div className="flex items-center gap-3 min-w-0">
                  <StatusBadge status={eventStatusLabel(ev.status)} />
                  <div className="min-w-0">
                    <p className="text-xs font-medium truncate">{eventLabel(ev.robot_technical_ids)}</p>
                    <p className="text-[10px] text-muted-foreground">
                      {new Date(ev.completed_at ?? ev.created_at).toLocaleString("pt-BR")}
                      {ev.status === "failed" && ev.error_message && (
                        <span className="block mt-0.5 text-destructive">{ev.error_message}</span>
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
