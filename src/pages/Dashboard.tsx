import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { formatDistanceToNow } from "date-fns";
import { ptBR } from "date-fns/locale";
import {
  AlertTriangle,
  BarChart3,
  Clock,
  FileText,
  FolderArchive,
  Landmark,
  RefreshCw,
  Sparkles,
  Users,
} from "lucide-react";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { MiniChart, DonutChart } from "@/components/dashboard/Charts";
import { StatsCard } from "@/components/dashboard/StatsCard";
import { AnimatedNumber } from "@/components/dashboard/AnimatedNumber";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { useSelectedCompanyIds } from "@/hooks/useSelectedCompanies";
import { useBranding, getAnalyticsTitle } from "@/contexts/BrandingContext";
import { getDashboardOverview, getRecentFiscalDocuments } from "@/services/dashboardService";

export default function Dashboard() {
  const { selectedCompanyIds } = useSelectedCompanyIds();
  const { branding } = useBranding();
  const analyticsTitle = getAnalyticsTitle(branding?.client_name);
  const companyFilter = selectedCompanyIds.length > 0 ? selectedCompanyIds : null;

  const { data: overview, isLoading: overviewLoading } = useQuery({
    queryKey: ["dashboard-overview", companyFilter],
    queryFn: () => getDashboardOverview(companyFilter),
    placeholderData: keepPreviousData,
  });

  const { data: recentEvents = [], isLoading: recentLoading } = useQuery({
    queryKey: ["dashboard-recent", companyFilter],
    queryFn: () => getRecentFiscalDocuments(companyFilter, 6),
  });

  const docsByType = overview?.docsByType ?? [
    { name: "NFS-e", value: 0 },
    { name: "NF-e", value: 0 },
    { name: "NFC-e", value: 0 },
  ];

  return (
    <div className="space-y-6">
      <div className="relative overflow-hidden rounded-2xl card-3d-elevated p-4 sm:p-6 md:p-8">
        <div className="absolute top-4 right-4 sm:right-8 h-24 w-24 rounded-full bg-primary/5 blur-2xl animate-float sm:h-32 sm:w-32" />
        <div className="absolute bottom-4 left-4 sm:left-12 h-16 w-16 rounded-full bg-accent/5 blur-2xl animate-float sm:h-24 sm:w-24" style={{ animationDelay: "2s" }} />
        <div className="relative flex min-w-0 flex-col gap-4 sm:flex-row sm:items-center sm:gap-6">
          <div className="relative shrink-0">
            <div className="absolute inset-0 scale-150 rounded-2xl bg-primary/20 blur-xl animate-pulse-slow" />
            <div className="relative flex h-14 w-14 items-center justify-center rounded-2xl bg-gradient-to-br from-primary to-accent shadow-lg animate-logo-float sm:h-16 sm:w-16">
              <BarChart3 className="h-7 w-7 text-primary-foreground sm:h-8 sm:w-8" />
            </div>
          </div>
          <div className="min-w-0">
            <div className="mb-1 flex flex-wrap items-center gap-2 sm:gap-3">
              <h1 className="min-w-0 truncate text-xl font-bold font-display text-gradient-animated sm:text-2xl md:text-3xl">
                {analyticsTitle}
              </h1>
              <span className="flex shrink-0 items-center gap-1 rounded-full bg-primary/10 px-2.5 py-1 text-[10px] font-semibold text-primary-icon">
                <Sparkles className="h-3 w-3 shrink-0" /> Visão executiva consolidada
              </span>
            </div>
            <p className="text-xs text-muted-foreground sm:text-sm">
              Indicadores consolidados de Fiscal, Departamento Pessoal e Contábil.
            </p>
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-4">
        <StatsCard
          title="Fiscal hoje"
          value={overviewLoading ? "—" : (overview?.executiveSummary.fiscal.processadosHoje ?? 0).toLocaleString()}
          icon={FolderArchive}
          description="Documentos/arquivos processados hoje (data de entrada)."
        />
        <StatsCard
          title="Total de documentos"
          value={overviewLoading ? "—" : (overview?.totalArquivosFisicos ?? overview?.totalDocuments ?? 0).toLocaleString()}
          icon={FileText}
          description="Total de arquivos físicos (PDF, XML e outros) armazenados."
        />
        <StatsCard
          title="Guias de DP"
          value={overviewLoading ? "—" : (overview?.executiveSummary.dp.guiasGeradas ?? 0).toLocaleString()}
          icon={Users}
          description="Guias do Departamento Pessoal registradas."
        />
        <StatsCard
          title="Contábil"
          value={overviewLoading ? "—" : (overview?.executiveSummary.contabil.balancosGerados ?? 0).toLocaleString()}
          icon={Landmark}
          description="Registros contábeis no sistema."
        />
      </div>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
        <GlassCard className="p-6 xl:col-span-2">
          <div className="mb-4 flex items-center justify-between">
            <div>
              <h3 className="text-sm font-semibold font-display">Quantidade de notas fiscais</h3>
              <p className="text-xs text-muted-foreground">Volume mensal de documentos por mês</p>
            </div>
            <BarChart3 className="h-4 w-4 text-primary-icon" />
          </div>
          <MiniChart data={overview?.documentsPerMonth ?? []} type="bar" height={240} valueLabel="Notas" />
        </GlassCard>

        <GlassCard className="p-6">
          <div className="mb-4">
            <h3 className="text-sm font-semibold font-display">Documentos por tipo</h3>
            <p className="text-xs text-muted-foreground">NFS-e, NF-e e NFC-e</p>
          </div>
          <DonutChart data={docsByType} height={180} />
          <div className="mt-4 space-y-2">
            {docsByType.map((item) => (
              <div key={item.name} className="flex items-center justify-between text-xs">
                <span className="text-muted-foreground">{item.name}</span>
                <span className="font-medium"><AnimatedNumber value={item.value} /></span>
              </div>
            ))}
          </div>
        </GlassCard>
      </div>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
        <GlassCard className="p-6">
          <div className="mb-4">
            <h3 className="text-sm font-semibold font-display">Guias de DP por tipo</h3>
            <p className="text-xs text-muted-foreground">FGTS, INSS, DARF, eSocial e outros tipos</p>
          </div>
          <DonutChart data={overview?.dpSummary.guiasPorTipo ?? []} height={220} />
        </GlassCard>

        <GlassCard className="p-6">
          <div className="mb-4">
            <h3 className="text-sm font-semibold font-display">Lançamentos contábeis por mês</h3>
            <p className="text-xs text-muted-foreground">Evolução dos registros do módulo contábil</p>
          </div>
          <MiniChart data={overview?.contabilSummary.lancamentosPorMes ?? []} type="bar" height={220} valueLabel="Lançamentos" />
        </GlassCard>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
        <GlassCard className="p-5">
          <div className="mb-3 flex items-center gap-3">
            <div className="rounded-full bg-primary/10 p-3">
              <FileText className="h-5 w-5 text-primary-icon" />
            </div>
            <div>
              <p className="text-sm font-semibold">Fiscal</p>
              <p className="text-xs text-muted-foreground">Visão executiva do fiscal</p>
            </div>
          </div>
          <div className="space-y-2 text-xs">
            <div className="flex items-center justify-between rounded-lg bg-muted/20 px-3 py-2">
              <span className="text-muted-foreground">Total de notas</span>
              <span className="font-semibold"><AnimatedNumber value={overview?.executiveSummary.fiscal.totalNotasFiscais ?? overview?.executiveSummary.fiscal.totalDocumentos ?? 0} /></span>
            </div>
            <div className="flex items-center justify-between rounded-lg bg-muted/20 px-3 py-2">
              <span className="text-muted-foreground">Total de arquivos</span>
              <span className="font-semibold"><AnimatedNumber value={overview?.executiveSummary.fiscal.totalArquivosFisicos ?? 0} /></span>
            </div>
            <div className="flex items-center justify-between rounded-lg bg-muted/20 px-3 py-2">
              <span className="text-muted-foreground">Processados hoje</span>
              <span className="font-semibold"><AnimatedNumber value={overview?.executiveSummary.fiscal.processadosHoje ?? 0} /></span>
            </div>
            <div className="flex items-center justify-between rounded-lg bg-muted/20 px-3 py-2">
              <span className="text-muted-foreground">Empresas ativas</span>
              <span className="font-semibold"><AnimatedNumber value={overview?.executiveSummary.fiscal.empresasAtivas ?? 0} /></span>
            </div>
          </div>
        </GlassCard>

        <GlassCard className="p-5">
          <div className="mb-3 flex items-center gap-3">
            <div className="rounded-full bg-warning/15 p-3">
              <Users className="h-5 w-5 text-warning" />
            </div>
            <div>
              <p className="text-sm font-semibold">Departamento Pessoal</p>
              <p className="text-xs text-muted-foreground">Guias e fechamento</p>
            </div>
          </div>
          <div className="space-y-2 text-xs">
            <div className="flex items-center justify-between rounded-lg bg-muted/20 px-3 py-2">
              <span className="text-muted-foreground">Guias geradas</span>
              <span className="font-semibold"><AnimatedNumber value={overview?.executiveSummary.dp.guiasGeradas ?? 0} /></span>
            </div>
            <div className="flex items-center justify-between rounded-lg bg-muted/20 px-3 py-2">
              <span className="text-muted-foreground">Guias pendentes</span>
              <span className="font-semibold"><AnimatedNumber value={overview?.executiveSummary.dp.guiasPendentes ?? 0} /></span>
            </div>
            <div className="flex items-center justify-between rounded-lg bg-muted/20 px-3 py-2">
              <span className="text-muted-foreground">Folha no mês</span>
              <span className="font-semibold"><AnimatedNumber value={overview?.executiveSummary.dp.folhaProcessadaMes ?? 0} /></span>
            </div>
          </div>
        </GlassCard>

        <GlassCard className="p-5">
          <div className="mb-3 flex items-center gap-3">
            <div className="rounded-full bg-info/15 p-3">
              <Landmark className="h-5 w-5 text-info" />
            </div>
            <div>
              <p className="text-sm font-semibold">Contábil</p>
              <p className="text-xs text-muted-foreground">Atualização e pendências</p>
            </div>
          </div>
          <div className="space-y-2 text-xs">
            <div className="flex items-center justify-between rounded-lg bg-muted/20 px-3 py-2">
              <span className="text-muted-foreground">Balanços gerados</span>
              <span className="font-semibold"><AnimatedNumber value={overview?.executiveSummary.contabil.balancosGerados ?? 0} /></span>
            </div>
            <div className="flex items-center justify-between rounded-lg bg-muted/20 px-3 py-2">
              <span className="text-muted-foreground">Empresas atualizadas</span>
              <span className="font-semibold"><AnimatedNumber value={overview?.executiveSummary.contabil.empresasAtualizadas ?? 0} /></span>
            </div>
            <div className="flex items-center justify-between rounded-lg bg-muted/20 px-3 py-2">
              <span className="text-muted-foreground">Pendentes</span>
              <span className="font-semibold"><AnimatedNumber value={overview?.executiveSummary.contabil.pendentes ?? 0} /></span>
            </div>
          </div>
        </GlassCard>
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <GlassCard className="p-6">
          <div className="mb-4 flex items-center justify-between">
            <div>
              <h3 className="text-sm font-semibold font-display">Pendências operacionais</h3>
              <p className="text-xs text-muted-foreground">Documentos e etapas que exigem atenção</p>
            </div>
          </div>
          <div className="space-y-3">
            {(overview?.processingStatus ?? []).filter((item) => ["pendente", "processando", "divergente"].includes(String(item.name).toLowerCase())).length === 0 ? (
              <p className="text-xs text-muted-foreground">Nenhuma pendência operacional.</p>
            ) : (
              (overview?.processingStatus ?? [])
                .filter((item) => ["pendente", "processando", "divergente"].includes(String(item.name).toLowerCase()))
                .map((item) => (
                  <div key={item.name} className="flex items-center justify-between rounded-xl border border-border bg-background/70 px-3 py-2 text-xs">
                    <span className="capitalize text-muted-foreground">{item.name}</span>
                    <span className="font-semibold"><AnimatedNumber value={item.value} /></span>
                  </div>
                ))
            )}
          </div>
        </GlassCard>

        <GlassCard className="p-6">
          <div className="mb-4 flex items-center justify-between">
            <div>
              <h3 className="text-sm font-semibold font-display">Atividade recente</h3>
              <p className="text-xs text-muted-foreground">Últimos documentos lançados no Supabase</p>
            </div>
          </div>
          <div className="space-y-3">
            {recentLoading ? (
              <p className="text-xs text-muted-foreground">Carregando…</p>
            ) : recentEvents.length === 0 ? (
              <p className="text-xs text-muted-foreground">Nenhum documento recente.</p>
            ) : (
              recentEvents.map((event) => (
                <div key={event.id} className="flex min-w-0 flex-col gap-2 border-b border-border py-3 last:border-0 sm:flex-row sm:items-center sm:justify-between">
                  <div className="min-w-0">
                    <p className="truncate text-xs font-medium">{String(event.companyName || "—")}</p>
                    <p className="text-[10px] text-muted-foreground">{event.type}</p>
                  </div>
                  <div className="flex flex-wrap items-center gap-2">
                    <StatusBadge status={event.status as "validado" | "novo" | "divergente" | "processando" | "pendente"} />
                    <span className="text-[10px] text-muted-foreground">
                      {event.created_at ? formatDistanceToNow(new Date(event.created_at), { addSuffix: true, locale: ptBR }) : "—"}
                    </span>
                  </div>
                </div>
              ))
            )}
          </div>
        </GlassCard>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
        <GlassCard className="p-5">
          <div className="mb-3 flex items-center gap-3">
            <div className="rounded-full bg-primary/10 p-3">
              <Users className="h-5 w-5 text-primary-icon" />
            </div>
            <div>
              <p className="text-sm font-semibold">Dados por empresa (Supabase)</p>
              <p className="text-xs text-muted-foreground">Empresas com mais documentos</p>
            </div>
          </div>
          <div className="space-y-2">
            {(overview?.topCompanies ?? []).length === 0 ? (
              <p className="text-xs text-muted-foreground">Nenhum dado por empresa.</p>
            ) : (
              overview?.topCompanies.map((company) => (
                <div key={company.companyId} className="flex items-center justify-between rounded-lg bg-muted/20 px-3 py-2 text-xs">
                  <span className="truncate text-muted-foreground">{company.companyName}</span>
                  <span className="font-semibold"><AnimatedNumber value={company.total} /></span>
                </div>
              ))
            )}
          </div>
        </GlassCard>

        <GlassCard className="p-5">
          <div className="mb-3 flex items-center gap-3">
            <div className="rounded-full bg-success/15 p-3">
              <RefreshCw className="h-5 w-5 text-success" />
            </div>
            <div>
              <p className="text-sm font-semibold">Sync</p>
              <p className="text-xs text-muted-foreground">Resumo dos eventos de sincronização</p>
            </div>
          </div>
          <div className="space-y-2 text-xs">
            <div className="flex items-center justify-between rounded-lg bg-muted/20 px-3 py-2">
              <span className="text-muted-foreground">Eventos recentes</span>
              <span className="font-semibold"><AnimatedNumber value={overview?.syncSummary.totalEventos ?? 0} /></span>
            </div>
            <div className="flex items-center justify-between rounded-lg bg-muted/20 px-3 py-2">
              <span className="text-muted-foreground">Sucessos</span>
              <span className="font-semibold"><AnimatedNumber value={overview?.syncSummary.sucessos ?? 0} /></span>
            </div>
            <div className="flex items-center justify-between rounded-lg bg-muted/20 px-3 py-2">
              <span className="text-muted-foreground">Falhas</span>
              <span className="font-semibold"><AnimatedNumber value={overview?.syncSummary.falhas ?? 0} /></span>
            </div>
          </div>
        </GlassCard>

        <GlassCard className="p-5">
          <div className="mb-3 flex items-center gap-3">
            <div className="rounded-full bg-info/15 p-3">
              <Clock className="h-5 w-5 text-info" />
            </div>
            <div>
              <p className="text-sm font-semibold">Eventos de sincronização</p>
              <p className="text-xs text-muted-foreground">Últimos registros do sistema</p>
            </div>
          </div>
          <div className="space-y-2">
            {(overview?.syncEvents ?? []).length === 0 ? (
              <p className="text-xs text-muted-foreground">Nenhum evento recente.</p>
            ) : (
              overview?.syncEvents.map((event) => (
                <div key={event.id} className="rounded-lg bg-muted/20 px-3 py-2 text-xs">
                  <div className="flex items-center justify-between gap-2">
                    <span className="truncate font-medium">{event.companyName}</span>
                    <span className="uppercase text-[10px] text-muted-foreground">{event.status}</span>
                  </div>
                  <p className="mt-1 text-[10px] text-muted-foreground">{event.tipo}</p>
                </div>
              ))
            )}
          </div>
        </GlassCard>
      </div>
    </div>
  );
}
