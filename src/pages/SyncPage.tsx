import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { MiniChart } from "@/components/dashboard/Charts";
import { StatsCard } from "@/components/dashboard/StatsCard";
import { RefreshCw, CheckCircle2, AlertTriangle, Clock } from "lucide-react";

const syncData = [
  { name: "00h", value: 5 },
  { name: "04h", value: 3 },
  { name: "08h", value: 22 },
  { name: "12h", value: 18 },
  { name: "16h", value: 30 },
  { name: "20h", value: 12 },
];

const events = [
  { id: "evt-001", tipo: "webhook", payload: "Lote NFS #4521", status: "sucesso" as const, idempotencyKey: "idem-abc-001", timestamp: "2025-07-15 14:32:00", retries: 0 },
  { id: "evt-002", tipo: "webhook", payload: "Lote NFE #4520", status: "sucesso" as const, idempotencyKey: "idem-abc-002", timestamp: "2025-07-15 14:28:00", retries: 0 },
  { id: "evt-003", tipo: "pull", payload: "Validação NFS #4519", status: "erro" as const, idempotencyKey: "idem-abc-003", timestamp: "2025-07-15 14:15:00", retries: 3 },
  { id: "evt-004", tipo: "webhook", payload: "Cálculo Preço #4518", status: "sucesso" as const, idempotencyKey: "idem-abc-004", timestamp: "2025-07-15 14:10:00", retries: 0 },
  { id: "evt-005", tipo: "webhook", payload: "Coleta XMLs #4517", status: "sucesso" as const, idempotencyKey: "idem-abc-005", timestamp: "2025-07-15 13:58:00", retries: 0 },
];

export default function SyncPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold font-display tracking-tight">Sincronização</h1>
        <p className="text-sm text-muted-foreground mt-1">Auditoria de webhooks, filas e eventos</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <StatsCard title="Eventos Hoje" value="347" icon={RefreshCw} change="+12% vs ontem" changeType="positive" />
        <StatsCard title="Sucesso" value="344" icon={CheckCircle2} change="99.1%" changeType="positive" />
        <StatsCard title="Falhas" value="3" icon={AlertTriangle} changeType="negative" />
        <StatsCard title="Latência Média" value="1.8s" icon={Clock} change="-0.3s" changeType="positive" />
      </div>

      <GlassCard className="p-6">
        <h3 className="text-sm font-semibold font-display mb-4">Volume de Eventos (24h)</h3>
        <MiniChart data={syncData} type="bar" color="hsl(210, 92%, 55%)" height={200} />
      </GlassCard>

      <GlassCard className="overflow-hidden">
        <div className="p-4 border-b border-border">
          <h3 className="text-sm font-semibold font-display">Log de Eventos</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border bg-muted/50">
                <th className="text-left px-4 py-3 font-medium text-muted-foreground">ID</th>
                <th className="text-left px-4 py-3 font-medium text-muted-foreground">Tipo</th>
                <th className="text-left px-4 py-3 font-medium text-muted-foreground">Payload</th>
                <th className="text-left px-4 py-3 font-medium text-muted-foreground">Status</th>
                <th className="text-left px-4 py-3 font-medium text-muted-foreground">Idempotency Key</th>
                <th className="text-left px-4 py-3 font-medium text-muted-foreground">Retries</th>
                <th className="text-left px-4 py-3 font-medium text-muted-foreground">Timestamp</th>
              </tr>
            </thead>
            <tbody>
              {events.map((evt) => (
                <tr key={evt.id} className="border-b border-border hover:bg-muted/30 transition-colors">
                  <td className="px-4 py-3 font-mono">{evt.id}</td>
                  <td className="px-4 py-3">
                    <span className="rounded bg-primary/10 px-2 py-0.5 text-[10px] font-medium text-primary-icon uppercase">{evt.tipo}</span>
                  </td>
                  <td className="px-4 py-3">{evt.payload}</td>
                  <td className="px-4 py-3"><StatusBadge status={evt.status} /></td>
                  <td className="px-4 py-3 font-mono text-muted-foreground text-[10px]">{evt.idempotencyKey}</td>
                  <td className="px-4 py-3">{evt.retries}</td>
                  <td className="px-4 py-3 text-muted-foreground">{evt.timestamp}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </GlassCard>
    </div>
  );
}
