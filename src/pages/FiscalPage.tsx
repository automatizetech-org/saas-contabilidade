import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import { StatsCard } from "@/components/dashboard/StatsCard";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { DonutChart, MiniChart } from "@/components/dashboard/Charts";
import { Building2, CalendarDays, Receipt } from "lucide-react";
import { useSelectedCompanyIds } from "@/hooks/useSelectedCompanies";
import { getFiscalOverviewAnalytics, getRecentFiscalDocuments } from "@/services/dashboardService";

const topicos = [
  { label: "NFS", path: "/fiscal/nfs" },
  { label: "NFE/NFC", path: "/fiscal/nfe-nfc" },
  { label: "DIFAL", path: "/fiscal/difal" },
  { label: "IRRF/CSLL", path: "/fiscal/irrf-csll" },
  { label: "Certidões", path: "/fiscal/certidoes" },
];

function getDefaultPeriod() {
  const now = new Date();
  const dateTo = now.toISOString().slice(0, 10);
  const dateFrom = new Date(now.getFullYear(), now.getMonth(), 1).toISOString().slice(0, 10);
  return { dateFrom, dateTo };
}

export default function FiscalPage() {
  const { selectedCompanyIds } = useSelectedCompanyIds();
  const companyFilter = selectedCompanyIds.length > 0 ? selectedCompanyIds : null;
  const defaults = useMemo(() => getDefaultPeriod(), []);
  const [dateFrom, setDateFrom] = useState(defaults.dateFrom);
  const [dateTo, setDateTo] = useState(defaults.dateTo);

  const { data: analytics, isLoading: loadingAnalytics } = useQuery({
    queryKey: ["fiscal-overview-analytics", companyFilter, dateFrom, dateTo],
    queryFn: () => getFiscalOverviewAnalytics(companyFilter, dateFrom, dateTo),
  });
  const { data: recentDocs = [], isLoading: loadingRecent } = useQuery({
    queryKey: ["fiscal-recent", companyFilter],
    queryFn: () => getRecentFiscalDocuments(companyFilter, 10),
  });

  const cards = analytics?.cards ?? {
    totalDocumentos: 0,
    documentosHoje: 0,
    empresasComEmissao: 0,
  };
  const byType = analytics?.byType ?? [];
  const byMonth = analytics?.byMonth ?? [];
  const byCompany = analytics?.byCompany ?? [];
  const byStatus = analytics?.byStatus ?? [];
  const byTypeSummary = analytics?.byTypeSummary ?? { NFS: 0, NFE: 0, NFC: 0, outros: 0 };

  const cardsPorTipo = [
    { tipo: "NFS-e", valor: byTypeSummary.NFS, descricao: "Notas de serviço no período", path: "/fiscal/nfs" },
    { tipo: "NF-e", valor: byTypeSummary.NFE, descricao: "Notas fiscais eletrônicas", path: "/fiscal/nfe-nfc" },
    { tipo: "NFC-e", valor: byTypeSummary.NFC, descricao: "Notas ao consumidor", path: "/fiscal/nfe-nfc" },
    { tipo: "Outros", valor: byTypeSummary.outros, descricao: "Demais tipos localizados", path: "/documentos" },
    { tipo: "Empresas ativas", valor: cards.empresasComEmissao, descricao: "Empresas com emissão no período", path: "/empresas" },
  ];

  const tooltipStyle = {
    background: "var(--ap-tooltip-bg)",
    color: "var(--ap-tooltip-text)",
    border: "1px solid var(--ap-tooltip-border)",
    borderRadius: "10px",
    fontSize: "12px",
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h1 className="text-2xl font-bold font-display tracking-tight">Fiscal</h1>
          <p className="text-sm text-muted-foreground mt-1">Visão analítica e conferência rápida das rotinas fiscais.</p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <div className="flex items-center gap-2 rounded-lg border border-border bg-background px-3 py-2">
            <span className="text-xs text-muted-foreground">Data inicial</span>
            <input type="date" value={dateFrom} onChange={(event) => setDateFrom(event.target.value)} className="bg-transparent text-xs outline-none" />
          </div>
          <div className="flex items-center gap-2 rounded-lg border border-border bg-background px-3 py-2">
            <span className="text-xs text-muted-foreground">Data final</span>
            <input type="date" value={dateTo} onChange={(event) => setDateTo(event.target.value)} className="bg-transparent text-xs outline-none" />
          </div>
          {topicos.map((item) => (
            <Link key={item.path} to={item.path} className="rounded-lg border border-border bg-card px-4 py-2 text-xs font-medium hover:bg-muted transition-colors">
              {item.label}
            </Link>
          ))}
        </div>
      </div>

      {(loadingAnalytics || loadingRecent) && !analytics ? (
        <div className="text-sm text-muted-foreground">Carregando…</div>
      ) : (
        <>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <StatsCard title="Total no período" value={cards.totalDocumentos.toLocaleString()} icon={Receipt} description="Notas fiscais no período (documentos reais)" />
            <StatsCard title="Emitidos hoje" value={cards.documentosHoje.toLocaleString()} icon={CalendarDays} description="Documentos com data de referência hoje" />
            <StatsCard title="Empresas com emissão" value={cards.empresasComEmissao.toLocaleString()} icon={Building2} description="Empresas com pelo menos uma nota no período" />
          </div>

          <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
            <GlassCard className="p-6">
              <h3 className="text-sm font-semibold font-display mb-2">Distribuição por tipo</h3>
              <p className="text-xs text-muted-foreground mb-4">NFS-e, NF-e, NFC-e e demais tipos localizados</p>
              <DonutChart data={byType} height={260} />
            </GlassCard>

            <GlassCard className="p-6">
              <h3 className="text-sm font-semibold font-display mb-2">Status dos documentos</h3>
              <p className="text-xs text-muted-foreground mb-4">Autorizados, pendentes, rejeitados, cancelados e outros status</p>
              <DonutChart data={byStatus} height={260} />
            </GlassCard>
          </div>

          <div className="grid grid-cols-1 xl:grid-cols-2 gap-4">
            <GlassCard className="p-6">
              <h3 className="text-sm font-semibold font-display mb-2">Documentos fiscais por mês</h3>
              <p className="text-xs text-muted-foreground mb-4">Evolução mensal da quantidade de documentos</p>
              <MiniChart data={byMonth} type="bar" height={260} valueLabel="Documentos" />
            </GlassCard>

            <GlassCard className="p-6">
              <h3 className="text-sm font-semibold font-display mb-2">Volume por empresa</h3>
              <p className="text-xs text-muted-foreground mb-4">Empresas com maior volume no período</p>
              <div className="h-[260px]">
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={byCompany} layout="vertical" margin={{ left: 12, right: 12 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" opacity={0.35} />
                    <XAxis type="number" tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
                    <YAxis type="category" dataKey="name" width={140} tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
                    <Tooltip contentStyle={tooltipStyle} formatter={(value: number) => [value, "Documentos"]} />
                    <Bar dataKey="value" fill="hsl(var(--primary))" radius={[0, 6, 6, 0]} />
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </GlassCard>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            <GlassCard className="p-6">
              <h3 className="text-sm font-semibold font-display mb-4">Últimos documentos</h3>
              {recentDocs.length === 0 ? (
                <p className="text-xs text-muted-foreground">Nenhum documento recente encontrado.</p>
              ) : (
                <div className="space-y-3">
                  {recentDocs.map((d) => (
                    <Link key={d.id} to={d.type === "NFE" || d.type === "NFC" ? "/fiscal/nfe-nfc" : `/fiscal/${(d.type || "nfs").toLowerCase()}`} className="flex items-center justify-between py-2 border-b border-border last:border-0 hover:bg-muted/30 rounded px-1 -mx-1 transition-colors">
                      <div className="min-w-0">
                        <p className="text-xs font-medium truncate">{d.companyName || "—"}</p>
                        <p className="text-[10px] text-muted-foreground">{d.type}</p>
                      </div>
                      <StatusBadge status={d.status as "validado" | "novo" | "processando" | "pendente" | "divergente"} />
                    </Link>
                  ))}
                </div>
              )}
            </GlassCard>

            <GlassCard className="p-6">
              <h3 className="text-sm font-semibold font-display mb-4">Métricas rápidas</h3>
              <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
                {cardsPorTipo.map((item) => (
                  <Link key={item.tipo} to={item.path} className="rounded-xl border border-border bg-background/70 px-4 py-4 hover:bg-muted/30 transition-colors">
                    <p className="text-xs text-muted-foreground">{item.tipo}</p>
                    <p className="text-2xl font-bold font-display mt-1">{item.valor.toLocaleString()}</p>
                    <p className="text-[11px] text-muted-foreground mt-2">{item.descricao}</p>
                  </Link>
                ))}
              </div>
            </GlassCard>
          </div>
        </>
      )}
    </div>
  );
}
