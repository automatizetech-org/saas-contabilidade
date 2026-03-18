import { StatsCard } from "@/components/dashboard/StatsCard";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { MiniChart } from "@/components/dashboard/Charts";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { DollarSign, TrendingUp, AlertTriangle, CheckCircle2 } from "lucide-react";

const revenueData = [
  { name: "Jan", value: 45000 },
  { name: "Fev", value: 52000 },
  { name: "Mar", value: 48000 },
  { name: "Abr", value: 61000 },
  { name: "Mai", value: 55000 },
  { name: "Jun", value: 67000 },
  { name: "Jul", value: 72000 },
];

const empresas = [
  { nome: "Tech Solutions Ltda", preco: "R$ 8.500,00", status: "validado" as const, periodo: "07/2025", pendencias: 0 },
  { nome: "Comércio ABC", preco: "R$ 4.200,00", status: "validado" as const, periodo: "07/2025", pendencias: 0 },
  { nome: "Indústria XYZ", preco: "R$ 12.800,00", status: "divergente" as const, periodo: "07/2025", pendencias: 2 },
  { nome: "Serviços Delta", preco: "R$ 3.900,00", status: "pendente" as const, periodo: "07/2025", pendencias: 3 },
  { nome: "Logística Beta", preco: "R$ 6.700,00", status: "validado" as const, periodo: "07/2025", pendencias: 0 },
  { nome: "Alfa Comercial", preco: "R$ 5.100,00", status: "validado" as const, periodo: "06/2025", pendencias: 0 },
];

export default function FinanceiroPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold font-display tracking-tight">Financeiro</h1>
        <p className="text-sm text-muted-foreground mt-1">Valores consolidados e pendências financeiras</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <StatsCard title="Receita Total" value="R$ 72.4k" change="+7.2% vs mês anterior" changeType="positive" icon={DollarSign} />
        <StatsCard title="Recebidos" value="R$ 61.2k" change="84.5% do total" changeType="positive" icon={TrendingUp} />
        <StatsCard title="Pendentes" value="R$ 11.2k" change="5 empresas" changeType="negative" icon={AlertTriangle} />
        <StatsCard title="Taxa Recebimento" value="84.5%" change="Meta: 90%" changeType="neutral" icon={CheckCircle2} />
      </div>

      <GlassCard className="p-6">
        <h3 className="text-sm font-semibold font-display mb-4">Evolução Receita</h3>
        <MiniChart data={revenueData} type="area" height={240} />
      </GlassCard>

      <GlassCard className="overflow-hidden">
        <div className="p-4 border-b border-border">
          <h3 className="text-sm font-semibold font-display">Preço por Empresa</h3>
          <p className="text-xs text-muted-foreground">Valor calculado pelo servidor de automações</p>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border bg-muted/50">
                <th className="text-left px-4 py-3 font-medium text-muted-foreground">Empresa</th>
                <th className="text-left px-4 py-3 font-medium text-muted-foreground">Período</th>
                <th className="text-left px-4 py-3 font-medium text-muted-foreground">Valor</th>
                <th className="text-left px-4 py-3 font-medium text-muted-foreground">Status</th>
                <th className="text-left px-4 py-3 font-medium text-muted-foreground">Pendências</th>
              </tr>
            </thead>
            <tbody>
              {empresas.map((e, i) => (
                <tr key={i} className="border-b border-border hover:bg-muted/30 transition-colors">
                  <td className="px-4 py-3 font-medium">{e.nome}</td>
                  <td className="px-4 py-3">{e.periodo}</td>
                  <td className="px-4 py-3 font-semibold font-display">{e.preco}</td>
                  <td className="px-4 py-3"><StatusBadge status={e.status} /></td>
                  <td className="px-4 py-3">
                    {e.pendencias > 0 ? (
                      <span className="text-destructive font-medium">{e.pendencias} doc(s) faltante(s)</span>
                    ) : (
                      <span className="text-success">Completo</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </GlassCard>
    </div>
  );
}
