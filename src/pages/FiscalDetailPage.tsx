import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { MiniChart } from "@/components/dashboard/Charts";
import { StatsCard } from "@/components/dashboard/StatsCard";
import { CursorPagination } from "@/components/common/CursorPagination";
import { useParams } from "react-router-dom";
import { FileText, FileDown, CalendarDays, Download, AlertCircle, FileArchive, DollarSign, Calendar, Medal } from "lucide-react";
import { useState, useMemo, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSelectedCompanyIds } from "@/hooks/useSelectedCompanies";
import { getNfsStatsByDateRange } from "@/services/dashboardService";
import { getCertidoesOverviewSummary, getFiscalDetailDocumentPathsForZip, getFiscalDetailDocumentsPage, getFiscalDetailSummary, getUnifiedDocumentsZipPaths, type CursorPageToken, type FiscalDetailKind } from "@/services/documentsService";
import { downloadFiscalDocument, downloadListedFilesZipWithCategory, downloadServerFileByPath, hasServerApi, markFiscalDocumentDownloaded } from "@/services/serverFileService";
import { toast } from "sonner";
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from "recharts";
import { Button } from "@/components/ui/button";
import { Progress } from "@/components/ui/progress";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { getVisibilityAwareRefetchInterval } from "@/lib/queryPolling";

type ServiceCodeRow = { code: string; description: string; total_value: number };
type FiscalDocumentRow = Awaited<ReturnType<typeof getFiscalDetailDocumentsPage>>["items"][number];

const TYPE_LABELS: Record<string, string> = {
  nfs: "NFS - Notas Fiscais de Servico",
  nfe: "NFE - Notas Fiscais Eletronicas",
  nfc: "NFC - Notas Fiscais ao Consumidor",
  "nfe-nfc": "NFE / NFC - Notas Fiscais Eletronicas e ao Consumidor",
  certidoes: "Certidoes",
  "simples-nacional": "Simples Nacional",
  difal: "DIFAL",
  "irrf-csll": "IRRF/CSLL",
};

const OBRIGACOES_FISCAIS = ["simples-nacional", "difal", "irrf-csll", "certidoes"];
const MESES = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"];

function formatCurrencyBRL(value: number) {
  return value.toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
}

function formatDateLabel(value: string | null | undefined) {
  if (!value) return "—";
  const date = String(value).slice(0, 10);
  const [year, month, day] = date.split("-");
  if (!year || !month || !day) return date;
  return `${day}/${month}/${year}`;
}

function getTodayPeriodDefaults() {
  const now = new Date();
  const first = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-01`;
  const last = new Date(now.getFullYear(), now.getMonth() + 1, 0).toISOString().slice(0, 10);
  return { first, last };
}

function formatMonthKey(key: string) {
  if (!/^\d{4}-\d{2}$/.test(key)) return key;
  const year = Number(key.slice(0, 4));
  const month = Number(key.slice(5, 7));
  return `${MESES[month - 1]}/${String(year).slice(2)}`;
}

function getDownloadLabel(filePath: string | null) {
  if (!filePath) return "Baixar";
  const lower = filePath.toLowerCase();
  if (lower.endsWith(".pdf")) return "Baixar PDF";
  if (lower.endsWith(".xml")) return "Baixar XML";
  return "Baixar";
}

function getSuggestedName(filePath: string | null, fallback: string) {
  return filePath?.split(/[\\/]/).pop() || fallback;
}

function ServiceCodesRankingTable({
  title,
  subtitle,
  rows,
  loading,
  emptyMessage,
}: {
  title: string;
  subtitle: string;
  rows: ServiceCodeRow[];
  loading: boolean;
  emptyMessage: string;
}) {
  return (
    <GlassCard className="p-6">
      <h3 className="text-sm font-semibold font-display mb-1">{title}</h3>
      <p className="text-xs text-muted-foreground mb-4">{subtitle}</p>
      {loading ? (
        <p className="text-xs text-muted-foreground">Carregando...</p>
      ) : rows.length === 0 ? (
        <p className="text-xs text-muted-foreground">{emptyMessage}</p>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-border">
          <table className="w-full text-xs min-w-[320px]">
            <thead>
              <tr className="border-b border-border bg-muted/50">
                <th className="text-left px-3 py-2.5 font-medium text-muted-foreground w-14">Pos.</th>
                <th className="text-left px-3 py-2.5 font-medium text-muted-foreground w-20">Codigo</th>
                <th className="text-left px-3 py-2.5 font-medium text-muted-foreground">Descricao</th>
                <th className="text-right px-3 py-2.5 font-medium text-muted-foreground w-28">Valor</th>
              </tr>
            </thead>
            <tbody>
              {rows.slice(0, 30).map((row, index) => (
                <tr key={`${row.code}-${index}`} className="border-b border-border last:border-0 hover:bg-muted/30">
                  <td className="px-3 py-2.5">
                    <span className="inline-flex items-center gap-1 font-medium">
                      {index < 3 ? <Medal className="h-3.5 w-3.5 shrink-0" /> : null}
                      {index + 1}º
                    </span>
                  </td>
                  <td className="px-3 py-2.5 font-mono">{row.code || "—"}</td>
                  <td className="px-3 py-2.5 text-muted-foreground">{row.description || "—"}</td>
                  <td className="px-3 py-2.5 text-right font-medium whitespace-nowrap">{formatCurrencyBRL(row.total_value)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </GlassCard>
  );
}

function CertidoesContent({ companyFilter }: { companyFilter: string[] | null }) {
  const [search, setSearch] = useState("");
  const [tipoFiltro, setTipoFiltro] = useState("all");
  const [pageSize, setPageSize] = useState(10);
  const [page, setPage] = useState(1);
  const [cursorHistory, setCursorHistory] = useState<Array<CursorPageToken | null>>([null]);
  const [downloadingZip, setDownloadingZip] = useState(false);
  const [downloadProgress, setDownloadProgress] = useState(0);

  useEffect(() => {
    setPage(1);
    setCursorHistory([null]);
  }, [search, tipoFiltro, companyFilter, pageSize]);

  const currentCursor = cursorHistory[page - 1] ?? null;

  const { data: overview, isLoading: overviewLoading } = useQuery({
    queryKey: ["certidoes-overview", companyFilter],
    queryFn: () => getCertidoesOverviewSummary(companyFilter),
    refetchInterval: () => getVisibilityAwareRefetchInterval(),
    refetchIntervalInBackground: true,
  });

  const { data: certidoesPage, isLoading: tableLoading } = useQuery({
    queryKey: ["certidoes-page", companyFilter, search, tipoFiltro, pageSize, page, currentCursor?.id ?? null, currentCursor?.createdAt ?? null, currentCursor?.sortDate ?? null],
    queryFn: () =>
      getFiscalDetailDocumentsPage({
        kind: "certidoes",
        companyIds: companyFilter,
        search,
        certidaoTipo: tipoFiltro,
        cursor: currentCursor,
        limit: pageSize,
      }),
    refetchInterval: () => getVisibilityAwareRefetchInterval(),
    refetchIntervalInBackground: true,
  });

  const chartData = (overview?.chartData ?? [
    { name: "Negativas", value: 0 },
    { name: "Irregulares", value: 0 },
  ]).map((item, index) => ({
    ...item,
    color: index === 0 ? "hsl(214, 84%, 56%)" : "hsl(0, 72%, 51%)",
  }));
  const items = certidoesPage?.items ?? [];
  const hasMore = certidoesPage?.hasMore ?? false;

  const handleBaixarPdf = async (row: FiscalDocumentRow) => {
    if (!row.file_path) {
      toast.error("PDF nao disponivel para esta certidao.");
      return;
    }
    if (!hasServerApi()) {
      toast.error("Nenhum servidor do escritorio esta disponivel para baixar o PDF.");
      return;
    }
    try {
      await downloadServerFileByPath(row.file_path, getSuggestedName(row.file_path, "certidao.pdf"));
      toast.success("PDF baixado com sucesso.");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Nao foi possivel baixar o PDF.");
    }
  };

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        <GlassCard className="lg:col-span-5 p-6">
          <h3 className="text-sm font-semibold font-display mb-2">Situacao das certidoes</h3>
          <p className="text-xs text-muted-foreground mb-4">Resumo seguro via RPC no banco.</p>
          <div className="w-full max-w-[260px] h-[240px] mx-auto">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={chartData} cx="50%" cy="50%" innerRadius={52} outerRadius={72} paddingAngle={2} dataKey="value" stroke="transparent" label={({ value }) => value} labelLine={false}>
                  {chartData.map((entry) => <Cell key={entry.name} fill={entry.color} />)}
                </Pie>
                <Tooltip formatter={(value: number) => [value, "certidoes"]} />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </GlassCard>
        <div className="lg:col-span-7 grid grid-cols-1 sm:grid-cols-2 gap-4">
          <GlassCard className="p-5 border-l-4 border-l-sky-500 bg-sky-500/5 dark:bg-sky-500/10">
            <div className="flex items-center gap-2 text-sky-600 dark:text-sky-500"><FileText className="h-5 w-5 shrink-0" /><span className="text-sm font-medium">Negativas</span></div>
            <p className="text-2xl font-bold mt-2">{overview?.cards.negativas ?? 0}</p>
          </GlassCard>
          <GlassCard className="p-5 border-l-4 border-l-red-500 bg-red-500/5 dark:bg-red-500/10">
            <div className="flex items-center gap-2 text-red-600 dark:text-red-500"><AlertCircle className="h-5 w-5 shrink-0" /><span className="text-sm font-medium">Irregulares</span></div>
            <p className="text-2xl font-bold mt-2">{overview?.cards.irregulares ?? 0}</p>
          </GlassCard>
        </div>
      </div>

      {downloadingZip && (
        <div className="space-y-2 rounded-xl border border-border bg-card p-4">
          <p className="text-sm font-medium text-foreground">Baixando ZIP...</p>
          <Progress value={downloadProgress} className="h-2" />
          <p className="text-xs text-muted-foreground">{downloadProgress}%</p>
        </div>
      )}

      <GlassCard className="overflow-hidden">
        <div className="p-3 sm:p-4 border-b border-border flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
          <h3 className="text-sm font-semibold font-display">Certidoes</h3>
          <div className="flex w-full flex-col gap-2 sm:w-auto sm:flex-row sm:items-center">
            <Select value={tipoFiltro} onValueChange={setTipoFiltro}>
              <SelectTrigger className="sm:w-[180px]">
                <SelectValue placeholder="Filtrar tipo" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Todas</SelectItem>
                <SelectItem value="federal">Federal</SelectItem>
                <SelectItem value="fgts">FGTS</SelectItem>
                <SelectItem value="estadual">Estadual</SelectItem>
              </SelectContent>
            </Select>
            <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Buscar por empresa, CNPJ ou tipo..." className="rounded-lg border border-border bg-background px-3 py-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring w-full min-w-0 sm:w-64 sm:max-w-[18rem]" />
            {hasServerApi() && (
              <Button
                variant="default"
                size="sm"
                className="gap-1.5 text-xs shrink-0"
                disabled={downloadingZip}
                onClick={async () => {
                  setDownloadingZip(true);
                  setDownloadProgress(0);
                  try {
                    const zipPaths = await getFiscalDetailDocumentPathsForZip({
                      kind: "certidoes",
                      companyIds: companyFilter,
                      search: search || undefined,
                      certidaoTipo: tipoFiltro,
                    });

                    if (zipPaths.length === 0) {
                      toast.error("Nenhuma certidao com PDF disponivel para os filtros atuais.");
                      return;
                    }

                    const itemsToZip = zipPaths.map((r) => ({
                      companyName: r.empresa || "EMPRESA",
                      category: "certidoes",
                      filePath: r.file_path,
                    }));

                    await downloadListedFilesZipWithCategory(itemsToZip, "certidoes", (p) => setDownloadProgress(p));
                    toast.success(`Download iniciado: ${itemsToZip.length} certidao(oes) da lista.`);
                  } catch (error) {
                    toast.error(error instanceof Error ? error.message : "Erro ao baixar ZIP.");
                  } finally {
                    setDownloadingZip(false);
                    setDownloadProgress(0);
                  }
                }}
              >
                <FileArchive className="h-3.5 w-3.5" />
                {downloadingZip ? "Gerando..." : "Baixar ZIP da lista"}
              </Button>
            )}
          </div>
        </div>
        <div className="overflow-x-auto">
          {overviewLoading || tableLoading ? (
            <div className="p-8 text-center text-sm text-muted-foreground">Carregando certidoes...</div>
          ) : items.length === 0 ? (
            <div className="p-8 text-center text-sm text-muted-foreground">Nenhuma certidao encontrada.</div>
          ) : (
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border bg-muted/50">
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Empresa</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Tipo</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Competencia</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Atualizacao</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Situacao</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Acoes</th>
                </tr>
              </thead>
              <tbody>
                {items.map((row) => (
                  <tr key={row.id} className="border-b border-border hover:bg-muted/30 transition-colors">
                    <td className="px-4 py-3">
                      <div className="font-medium">{row.empresa}</div>
                      <div className="text-muted-foreground">{row.cnpj || "—"}</div>
                    </td>
                    <td className="px-4 py-3 text-muted-foreground">{row.tipo_certidao || row.type}</td>
                    <td className="px-4 py-3 text-muted-foreground">{row.periodo || "—"}</td>
                    <td className="px-4 py-3 text-muted-foreground">{formatDateLabel(row.document_date)}</td>
                    <td className="px-4 py-3"><StatusBadge status={row.status as never} /></td>
                    <td className="px-4 py-3">
                      <Button variant="outline" size="sm" className="h-7 text-xs gap-1" onClick={() => handleBaixarPdf(row)} disabled={!row.file_path}>
                        <Download className="h-3.5 w-3.5" />
                        PDF
                      </Button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
        {items.length > 0 && (
          <CursorPagination
            currentPage={page}
            pageSize={pageSize}
            shownItems={items.length}
            hasMore={hasMore}
            onPrevious={() => setPage((current) => Math.max(1, current - 1))}
            onNext={() => {
              if (!certidoesPage?.nextCursor || !hasMore) return;
              setCursorHistory((history) => {
                const next = history.slice(0, page);
                next[page] = certidoesPage.nextCursor;
                return next;
              });
              setPage((current) => current + 1);
            }}
            onPageSizeChange={(next) => {
              setPageSize(next);
              setPage(1);
              setCursorHistory([null]);
            }}
          />
        )}
      </GlassCard>
    </div>
  );
}

export default function FiscalDetailPage() {
  const { type } = useParams<{ type: string }>();
  const [search, setSearch] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [fileKind, setFileKind] = useState<"all" | "xml" | "pdf">("all");
  const [origem, setOrigem] = useState<"all" | "recebidas" | "emitidas">("all");
  const [modelo, setModelo] = useState<"all" | "55" | "65">("all");
  const [pageSize, setPageSize] = useState(10);
  const [currentPage, setCurrentPage] = useState(1);
  const [cursorHistory, setCursorHistory] = useState<Array<CursorPageToken | null>>([null]);
  const [downloadingZip, setDownloadingZip] = useState(false);
  const [downloadProgress, setDownloadProgress] = useState(0);
  const { selectedCompanyIds } = useSelectedCompanyIds();
  const companyFilter = selectedCompanyIds.length > 0 ? selectedCompanyIds : null;
  const kind = (type ?? "nfs") as FiscalDetailKind;
  const label = TYPE_LABELS[type ?? "nfs"] || "Documentos Fiscais";
  const isObrigacao = Boolean(type && OBRIGACOES_FISCAIS.includes(type));
  const isNfeNfc = type === "nfe-nfc";
  const isNfs = type === "nfs";
  const periodDefaults = useMemo(getTodayPeriodDefaults, []);
  const resolvedDateFrom = isNfs || isNfeNfc ? (dateFrom || periodDefaults.first) : dateFrom;
  const resolvedDateTo = isNfs || isNfeNfc ? (dateTo || periodDefaults.last) : dateTo;
  const canDownload = hasServerApi();

  useEffect(() => {
    setCurrentPage(1);
    setCursorHistory([null]);
  }, [search, dateFrom, dateTo, fileKind, origem, modelo, companyFilter, type, pageSize]);

  const currentCursor = cursorHistory[currentPage - 1] ?? null;

  const { data: detailSummary, isLoading: summaryLoading } = useQuery({
    queryKey: ["fiscal-detail-summary", kind, companyFilter, resolvedDateFrom, resolvedDateTo],
    queryFn: () =>
      getFiscalDetailSummary({
        kind,
        companyIds: companyFilter,
        dateFrom: resolvedDateFrom || undefined,
        dateTo: resolvedDateTo || undefined,
      }),
    enabled: !isObrigacao,
    refetchInterval: () => getVisibilityAwareRefetchInterval(),
    refetchIntervalInBackground: true,
  });

  const { data: documentsPage, isLoading: documentsLoading } = useQuery({
    queryKey: ["fiscal-detail-page", kind, companyFilter, search, resolvedDateFrom, resolvedDateTo, fileKind, origem, modelo, pageSize, currentPage, currentCursor?.id ?? null, currentCursor?.createdAt ?? null, currentCursor?.sortDate ?? null],
    queryFn: () =>
      getFiscalDetailDocumentsPage({
        kind,
        companyIds: companyFilter,
        search,
        dateFrom: resolvedDateFrom || undefined,
        dateTo: resolvedDateTo || undefined,
        fileKind,
        origem,
        modelo,
        cursor: currentCursor,
        limit: pageSize,
      }),
    enabled: !isObrigacao,
    refetchInterval: () => getVisibilityAwareRefetchInterval(),
    refetchIntervalInBackground: true,
  });

  const nfsStatsQuery = useQuery({
    queryKey: ["nfs-stats", companyFilter, resolvedDateFrom, resolvedDateTo],
    queryFn: () => getNfsStatsByDateRange(companyFilter, resolvedDateFrom, resolvedDateTo),
    enabled: isNfs && !!resolvedDateFrom && !!resolvedDateTo,
    refetchInterval: () => getVisibilityAwareRefetchInterval(),
    refetchIntervalInBackground: true,
  });

  const prevPeriod = useMemo(() => {
    if (!isNfs || !resolvedDateFrom) return null;
    const year = Number(resolvedDateFrom.slice(0, 4));
    const month = Number(resolvedDateFrom.slice(5, 7));
    const prev = new Date(year, month - 2, 1);
    return {
      first: `${prev.getFullYear()}-${String(prev.getMonth() + 1).padStart(2, "0")}-01`,
      last: new Date(prev.getFullYear(), prev.getMonth() + 1, 0).toISOString().slice(0, 10),
    };
  }, [isNfs, resolvedDateFrom]);

  const nfsPrevStatsQuery = useQuery({
    queryKey: ["nfs-stats-prev", companyFilter, prevPeriod?.first, prevPeriod?.last],
    queryFn: () => getNfsStatsByDateRange(companyFilter, prevPeriod!.first, prevPeriod!.last),
    enabled: Boolean(prevPeriod?.first && prevPeriod?.last),
    refetchInterval: () => getVisibilityAwareRefetchInterval(),
    refetchIntervalInBackground: true,
  });

  const pageItems = documentsPage?.items ?? [];
  const hasMore = documentsPage?.hasMore ?? false;
  const summaryCards = detailSummary?.cards ?? {
    totalDocuments: 0,
    availableDocuments: 0,
    thisMonth: 0,
    nfeCount: 0,
    nfcCount: 0,
  };
  const volumeMensalData = (detailSummary?.byMonth ?? []).map((item) => ({
    name: formatMonthKey(item.key),
    value: item.value,
  }));

  const nfsVariation = useMemo(() => {
    const current = nfsStatsQuery.data?.valorEmitidas ?? 0;
    const previous = nfsPrevStatsQuery.data?.valorEmitidas ?? 0;
    if (!previous) return current > 0 ? "+100% vs mes anterior" : "igual ao mes anterior";
    const pct = ((current - previous) / previous) * 100;
    if (pct === 0) return "igual ao mes anterior";
    return `${pct > 0 ? "+" : ""}${pct.toFixed(1).replace(".", ",")}% vs mes anterior`;
  }, [nfsStatsQuery.data, nfsPrevStatsQuery.data]);

  const handleDownload = async (row: FiscalDocumentRow) => {
    try {
      if (kind === "certidoes") {
        if (!row.file_path) throw new Error("Arquivo indisponivel.");
        await downloadServerFileByPath(row.file_path, getSuggestedName(row.file_path, "certidao.pdf"));
      } else {
        await downloadFiscalDocument(row.id, getSuggestedName(row.file_path, row.chave || "documento"));
        await markFiscalDocumentDownloaded(row.id);
      }
      toast.success("Download iniciado.");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Erro ao baixar o arquivo.");
    }
  };

  if (type === "certidoes") {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold font-display tracking-tight">{label}</h1>
          <p className="text-sm text-muted-foreground mt-1">Certidoes fiscais com resumo por RPC e lista paginada.</p>
        </div>
        <CertidoesContent companyFilter={companyFilter} />
      </div>
    );
  }

  if (isObrigacao) {
    return (
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold font-display tracking-tight">{label}</h1>
          <p className="text-sm text-muted-foreground mt-1">Conteudo especifico desta obrigacao sera exibido aqui.</p>
        </div>
        <GlassCard className="p-8">
          <p className="text-sm text-muted-foreground">A camada de analytics e paginação desta obrigacao ainda depende do conteudo operacional da rotina.</p>
        </GlassCard>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold font-display tracking-tight">{label}</h1>
        <p className="text-sm text-muted-foreground mt-1">Resumo por RPC e documentos com paginacao server-side real.</p>
      </div>

      {downloadingZip && (
        <div className="space-y-2 rounded-xl border border-border bg-card p-4">
          <p className="text-sm font-medium text-foreground">Baixando ZIP...</p>
          <Progress value={downloadProgress} className="h-2" />
          <p className="text-xs text-muted-foreground">{downloadProgress}%</p>
        </div>
      )}

      {(isNfs || isNfeNfc) && (
        <GlassCard className="p-4">
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex items-center gap-2 text-muted-foreground">
              <Calendar className="h-4 w-4" />
              <span className="text-sm font-medium">Periodo</span>
            </div>
            <input type="date" value={resolvedDateFrom} onChange={(event) => setDateFrom(event.target.value)} className="rounded-lg border border-border bg-background px-3 py-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring" />
            <input type="date" value={resolvedDateTo} onChange={(event) => setDateTo(event.target.value)} className="rounded-lg border border-border bg-background px-3 py-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring" />
          </div>
        </GlassCard>
      )}

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {isNfs ? (
          <>
            <StatsCard title="Total no periodo" value={String(summaryCards.totalDocuments)} icon={FileText} />
            <StatsCard title="Valor prestadas" value={formatCurrencyBRL(nfsStatsQuery.data?.valorEmitidas ?? 0)} icon={DollarSign} change={nfsVariation} changeType="positive" />
            <StatsCard title="Valor tomadas" value={formatCurrencyBRL(nfsStatsQuery.data?.valorRecebidas ?? 0)} icon={DollarSign} />
            <StatsCard title="Este mes" value={String(summaryCards.thisMonth)} icon={CalendarDays} changeType="neutral" />
          </>
        ) : (
          <>
            <StatsCard title="Total" value={String(summaryCards.totalDocuments)} icon={FileText} />
            <StatsCard title="Disponiveis" value={String(summaryCards.availableDocuments)} icon={FileDown} changeType="positive" />
            <StatsCard title="Este mes" value={String(summaryCards.thisMonth)} icon={CalendarDays} changeType="neutral" />
            {isNfeNfc ? <StatsCard title="NFE / NFC" value={`${summaryCards.nfeCount}/${summaryCards.nfcCount}`} icon={FileText} /> : null}
          </>
        )}
      </div>

      {isNfs && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <ServiceCodesRankingTable
            title="Ranking de codigos de servico (prestadas)"
            subtitle="Codigos das notas emitidas pela empresa."
            rows={nfsStatsQuery.data?.serviceCodesRankingPrestadas ?? []}
            loading={nfsStatsQuery.isLoading}
            emptyMessage="Nenhum dado de codigo de servico neste periodo."
          />
          <ServiceCodesRankingTable
            title="Ranking de codigos de servico (tomadas)"
            subtitle="Codigos das notas recebidas pela empresa."
            rows={nfsStatsQuery.data?.serviceCodesRankingTomadas ?? []}
            loading={nfsStatsQuery.isLoading}
            emptyMessage="Nenhum dado de codigo de servico neste periodo."
          />
        </div>
      )}

      <GlassCard className="p-6">
        <h3 className="text-sm font-semibold font-display mb-4">Volume Mensal</h3>
        <MiniChart data={volumeMensalData} type="area" height={200} valueLabel="Documentos" />
      </GlassCard>

      <GlassCard className="overflow-hidden">
        <div className="p-3 sm:p-4 border-b border-border flex flex-col gap-3">
          <h3 className="text-sm font-semibold font-display">Documentos</h3>
          <div className="flex flex-wrap items-center gap-2">
            <input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Buscar por empresa, CNPJ ou chave..." className="rounded-lg border border-border bg-background px-3 py-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring w-full min-w-0 sm:w-52" />
            {!isNfs && !isNfeNfc && (
              <>
                <input type="date" value={dateFrom} onChange={(event) => setDateFrom(event.target.value)} className="rounded-lg border border-border bg-background px-3 py-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring" />
                <input type="date" value={dateTo} onChange={(event) => setDateTo(event.target.value)} className="rounded-lg border border-border bg-background px-3 py-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring" />
              </>
            )}
            <select value={fileKind} onChange={(event) => setFileKind(event.target.value as "all" | "xml" | "pdf")} className="rounded-lg border border-border bg-background px-3 py-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring">
              <option value="all">XML e PDF</option>
              <option value="xml">So XML</option>
              <option value="pdf">So PDF</option>
            </select>
            {isNfs && (
              <select value={origem} onChange={(event) => setOrigem(event.target.value as "all" | "recebidas" | "emitidas")} className="rounded-lg border border-border bg-background px-3 py-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring">
                <option value="all">Tomadas e prestadas</option>
                <option value="recebidas">Tomadas</option>
                <option value="emitidas">Prestadas</option>
              </select>
            )}
            {isNfeNfc && (
              <select value={modelo} onChange={(event) => setModelo(event.target.value as "all" | "55" | "65")} className="rounded-lg border border-border bg-background px-3 py-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring">
                <option value="all">Modelos 55 e 65</option>
                <option value="55">Somente 55</option>
                <option value="65">Somente 65</option>
              </select>
            )}
            {canDownload && (
              <Button
                variant="default"
                size="sm"
                className="gap-1.5 text-xs"
                disabled={downloadingZip}
                onClick={async () => {
                  setDownloadingZip(true);
                  setDownloadProgress(0);
                  try {
                    // Mesma lógica de /documentos: uma RPC + download-zip-by-paths (rápido).
                    const fileKindFilter: "Todos" | "XML" | "PDF" =
                      fileKind === "all" ? "Todos" : fileKind === "xml" ? "XML" : "PDF";
                    const zipPaths = await getUnifiedDocumentsZipPaths(
                      {
                        companyIds: companyFilter ?? undefined,
                        category: kind === "nfs" ? "nfs" : "nfe_nfc",
                        fileKind: fileKindFilter,
                        search: search || undefined,
                        dateFrom: resolvedDateFrom || undefined,
                        dateTo: resolvedDateTo || undefined,
                      },
                      50_000
                    );

                    if (zipPaths.length === 0) {
                      toast.error("Nenhum documento com arquivo disponivel para os filtros atuais.");
                      return;
                    }

                    const uniqueByPath = new Map<string, (typeof zipPaths)[number]>();
                    for (const row of zipPaths) {
                      if (!row.file_path || uniqueByPath.has(row.file_path)) continue;
                      uniqueByPath.set(row.file_path, row);
                    }

                    const categoryToFolder = (key: string) =>
                      key === "nfs" ? "nfs" : key === "nfe_nfc" ? "nfe-nfc" : "outros";

                    const items = Array.from(uniqueByPath.values()).map((row) => ({
                      companyName: row.empresa || "EMPRESA",
                      category: categoryToFolder(row.category_key),
                      filePath: row.file_path,
                    }));

                    const zipSuffix = kind === "nfs" ? "nfs" : "nfe-nfc";
                    await downloadListedFilesZipWithCategory(items, zipSuffix, (p) => setDownloadProgress(p));
                    toast.success(`Download iniciado: ${items.length} documento(s) da lista.`);
                  } catch (error) {
                    toast.error(error instanceof Error ? error.message : "Erro ao baixar ZIP.");
                  } finally {
                    setDownloadingZip(false);
                    setDownloadProgress(0);
                  }
                }}
              >
                <FileArchive className="h-3.5 w-3.5" />
                {downloadingZip ? "Gerando..." : "Baixar ZIP da lista"}
              </Button>
            )}
          </div>
        </div>
        <div className="overflow-x-auto">
          {summaryLoading || documentsLoading ? (
            <div className="p-8 text-center text-sm text-muted-foreground">Carregando...</div>
          ) : pageItems.length === 0 ? (
            <div className="p-8 text-center text-sm text-muted-foreground">Nenhum documento encontrado.</div>
          ) : (
            <table className="w-full text-xs min-w-[920px]">
              <thead>
                <tr className="border-b border-border bg-muted/50">
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Empresa</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">CNPJ</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Periodo</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Origem</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Chave</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Status</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Data</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Acoes</th>
                </tr>
              </thead>
              <tbody>
                {pageItems.map((row) => (
                  <tr key={row.id} className="border-b border-border hover:bg-muted/30 transition-colors">
                    <td className="px-4 py-3 font-medium">{row.empresa}</td>
                    <td className="px-4 py-3 text-muted-foreground">{row.cnpj ?? "—"}</td>
                    <td className="px-4 py-3">{row.periodo ?? "—"}</td>
                    <td className="px-4 py-3 text-muted-foreground">{row.origem ?? row.modelo ?? "—"}</td>
                    <td className="px-4 py-3 text-muted-foreground font-mono text-[10px]">{row.chave ? `${row.chave.slice(0, 20)}...` : "—"}</td>
                    <td className="px-4 py-3"><StatusBadge status={row.status as never} /></td>
                    <td className="px-4 py-3 text-muted-foreground">{formatDateLabel(row.document_date ?? row.created_at)}</td>
                    <td className="px-4 py-3">
                      {row.file_path ? (
                        <button
                          type="button"
                          onClick={() => handleDownload(row)}
                          className="inline-flex items-center gap-1 rounded-md px-2 py-1 text-xs font-medium bg-primary/10 text-primary-icon hover:bg-primary/20 transition-colors"
                        >
                          <Download className="h-3.5 w-3.5" /> {getDownloadLabel(row.file_path)}
                        </button>
                      ) : (
                        <span className="text-muted-foreground text-[10px]">Sem arquivo</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
        {pageItems.length > 0 && (
          <CursorPagination
            currentPage={currentPage}
            pageSize={pageSize}
            shownItems={pageItems.length}
            hasMore={hasMore}
            onPrevious={() => setCurrentPage((page) => Math.max(1, page - 1))}
            onNext={() => {
              if (!documentsPage?.nextCursor || !hasMore) return;
              setCursorHistory((history) => {
                const next = history.slice(0, currentPage);
                next[currentPage] = documentsPage.nextCursor;
                return next;
              });
              setCurrentPage((page) => page + 1);
            }}
            onPageSizeChange={(next) => {
              setPageSize(next);
              setCurrentPage(1);
              setCursorHistory([null]);
            }}
          />
        )}
      </GlassCard>
    </div>
  );
}
