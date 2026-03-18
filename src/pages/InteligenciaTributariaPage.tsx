import { useQuery } from "@tanstack/react-query"
import { Link } from "react-router-dom"
import { StatsCard } from "@/components/dashboard/StatsCard"
import { GlassCard } from "@/components/dashboard/GlassCard"
import { DonutChart, MiniChart } from "@/components/dashboard/Charts"
import { Building2, Calculator, Landmark, TrendingUp } from "lucide-react"
import { useSelectedCompanyIds } from "@/hooks/useSelectedCompanies"
import { getTaxIntelligenceOverview } from "@/services/tributaryIntelligenceService"
import { formatCurrencyBRL, formatPercentBRL } from "@/modules/tributary-intelligence/formatters"

const topics = [
  { label: "Visão Geral", path: "/inteligencia-tributaria" },
  { label: "Simples Nacional", path: "/inteligencia-tributaria/simples-nacional" },
  { label: "Lucro Real", path: "/inteligencia-tributaria/lucro-real" },
  { label: "Lucro Presumido", path: "/inteligencia-tributaria/lucro-presumido" },
]

export default function InteligenciaTributariaPage() {
  const { selectedCompanyIds } = useSelectedCompanyIds()
  const companyFilter = selectedCompanyIds.length > 0 ? selectedCompanyIds : null
  const { data, isLoading } = useQuery({
    queryKey: ["tax-intelligence-overview", companyFilter],
    queryFn: () => getTaxIntelligenceOverview(companyFilter),
  })

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold font-display tracking-tight">Inteligência Tributária</h1>
        <p className="text-sm text-muted-foreground mt-1">Simulações auditáveis, leitura executiva do regime e base pronta para evolução normativa.</p>
      </div>

      <div className="flex flex-wrap gap-2">
        {topics.map((item) => (
          <Link key={item.path} to={item.path} className="rounded-lg border border-border bg-card px-4 py-2 text-xs font-medium hover:bg-muted transition-colors">
            {item.label}
          </Link>
        ))}
      </div>

      {isLoading && !data ? (
        <div className="text-sm text-muted-foreground">Carregando...</div>
      ) : (
        <>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            <StatsCard title="Cálculos salvos" value={data?.cards.calculosSalvos ?? 0} icon={Calculator} />
            <StatsCard title="DAS médio" value={formatCurrencyBRL(data?.cards.mediaDas ?? 0)} icon={Landmark} />
            <StatsCard title="Alíquota efetiva média" value={formatPercentBRL(data?.cards.mediaAliquotaEfetiva ?? 0, 2)} icon={TrendingUp} />
            <StatsCard title="Empresas analisadas" value={data?.cards.empresasAtivas ?? 0} icon={Building2} />
          </div>

          <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
            <GlassCard className="p-6 xl:col-span-2">
              <h3 className="text-sm font-semibold font-display mb-2">Evolução das simulações</h3>
              <p className="text-xs text-muted-foreground mb-4">Volume recente de apurações para apoiar comparativos mensais.</p>
              <MiniChart data={data?.byMonth ?? []} type="bar" height={280} valueLabel="Cálculos" />
            </GlassCard>

            <GlassCard className="p-6">
              <h3 className="text-sm font-semibold font-display mb-2">Distribuição por anexo</h3>
              <p className="text-xs text-muted-foreground mb-4">Anexos efetivamente aplicados nas últimas memórias salvas.</p>
              <DonutChart data={data?.annexDistribution ?? []} height={280} />
            </GlassCard>
          </div>

          <div className="grid grid-cols-1 xl:grid-cols-3 gap-4">
            <GlassCard className="p-6">
              <h3 className="text-sm font-semibold font-display mb-4">Submódulos</h3>
              <div className="space-y-3">
                {(data?.byTopic ?? []).map((item) => (
                  <div key={item.name} className="rounded-xl border border-border bg-background/70 px-4 py-4">
                    <p className="text-xs text-muted-foreground">{item.name}</p>
                    <p className="text-2xl font-bold font-display mt-1">{item.value}</p>
                  </div>
                ))}
              </div>
            </GlassCard>

            <GlassCard className="p-6 xl:col-span-2">
              <h3 className="text-sm font-semibold font-display mb-4">Leitura executiva</h3>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
                <Link to="/inteligencia-tributaria/simples-nacional" className="rounded-xl border border-border bg-background/70 p-5 hover:bg-muted/30 transition-colors">
                  <p className="text-sm font-semibold font-display">Simples Nacional</p>
                  <p className="text-xs text-muted-foreground mt-2">Motor real com RBT12, FS12, fator R, memória de cálculo e decomposição do DAS.</p>
                </Link>
                <Link to="/inteligencia-tributaria/lucro-real" className="rounded-xl border border-border bg-background/70 p-5 hover:bg-muted/30 transition-colors">
                  <p className="text-sm font-semibold font-display">Lucro Real</p>
                  <p className="text-xs text-muted-foreground mt-2">Estrutura pronta para cenários de IRPJ, CSLL, adições, exclusões e LALUR.</p>
                </Link>
                <Link to="/inteligencia-tributaria/lucro-presumido" className="rounded-xl border border-border bg-background/70 p-5 hover:bg-muted/30 transition-colors">
                  <p className="text-sm font-semibold font-display">Lucro Presumido</p>
                  <p className="text-xs text-muted-foreground mt-2">Base visual inicial para margens presumidas, PIS/Cofins e calendário fiscal.</p>
                </Link>
              </div>
            </GlassCard>
          </div>

          <GlassCard className="overflow-hidden">
            <div className="p-4 border-b border-border">
              <h3 className="text-sm font-semibold font-display">Últimas memórias registradas</h3>
              <p className="text-xs text-muted-foreground mt-1">Histórico recente para retomada rápida dos estudos tributários.</p>
            </div>
            <div className="divide-y divide-border">
              {(data?.recentCalculations ?? []).length === 0 ? (
                <div className="px-4 py-6 text-sm text-muted-foreground">Nenhum cálculo salvo até o momento.</div>
              ) : (
                data!.recentCalculations.map((item) => (
                  <div key={item.id} className="px-4 py-3 flex flex-wrap items-center justify-between gap-3 hover:bg-muted/30 transition-colors">
                    <div>
                      <p className="text-xs font-medium">{item.companyName}</p>
                      <p className="text-[11px] text-muted-foreground">{item.apurationPeriod} • Anexo {item.appliedAnnex}</p>
                    </div>
                    <p className="text-sm font-semibold font-display">{formatCurrencyBRL(item.estimatedDas)}</p>
                  </div>
                ))
              )}
            </div>
          </GlassCard>
        </>
      )}
    </div>
  )
}
