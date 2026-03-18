import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { MiniChart } from "@/components/dashboard/Charts";
import { StatsCard } from "@/components/dashboard/StatsCard";
import { DataPagination } from "@/components/common/DataPagination";
import { useParams } from "react-router-dom";
import { FileText, FileDown, CalendarDays, Download, AlertCircle, ThumbsUp, FileArchive, DollarSign, ListOrdered, Calendar, Medal } from "lucide-react";
import { useState, useMemo, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSelectedCompanyIds } from "@/hooks/useSelectedCompanies";
import { getCertidoesDocuments, getFiscalDocumentsByType, getFiscalDocumentsNfeNfc, getNfsStatsByDateRange } from "@/services/dashboardService";
import { downloadFiscalCompaniesZip, downloadFiscalDocument, downloadServerFileByPath, hasServerApi, markFiscalDocumentDownloaded } from "@/services/serverFileService";
import { toast } from "sonner";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend, PieChart, Pie, Cell } from "recharts";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";

type ServiceCodeRow = { code: string; description: string; total_value: number };

function ServiceCodesRankingTable({
  title,
  subtitle,
  rows,
  loading,
  emptyMessage,
  maxRows = 30,
}: {
  title: string;
  subtitle: string;
  rows: ServiceCodeRow[];
  loading: boolean;
  emptyMessage: string;
  maxRows?: number;
}) {
  const slice = rows.slice(0, maxRows);
  const hasMore = rows.length > maxRows;
  return (
    <GlassCard className="p-6">
      <h3 className="text-sm font-semibold font-display mb-1">{title}</h3>
      <p className="text-xs text-muted-foreground mb-4">{subtitle}</p>
      {loading ? (
        <p className="text-xs text-muted-foreground">Carregando…</p>
      ) : slice.length === 0 ? (
        <p className="text-xs text-muted-foreground">{emptyMessage}</p>
      ) : (
        <>
          <div className="overflow-x-auto -webkit-overflow-scrolling-touch rounded-lg border border-border">
            <table className="w-full text-xs min-w-[320px]">
              <thead>
                <tr className="border-b border-border bg-muted/50">
                  <th className="text-left px-3 py-2.5 font-medium text-muted-foreground w-14">Posição</th>
                  <th className="text-left px-3 py-2.5 font-medium text-muted-foreground w-20">Código</th>
                  <th className="text-left px-3 py-2.5 font-medium text-muted-foreground">Descrição</th>
                  <th className="text-right px-3 py-2.5 font-medium text-muted-foreground w-28">Valor (R$)</th>
                </tr>
              </thead>
              <tbody>
                {slice.map((row, i) => {
                  const pos = i + 1;
                  const isGold = pos === 1;
                  const isSilver = pos === 2;
                  const isBronze = pos === 3;
                  return (
                    <tr key={i} className="border-b border-border last:border-0 hover:bg-muted/30">
                      <td className="px-3 py-2.5 align-middle w-14">
                        {isGold && (
                          <span className="inline-flex items-center gap-1 font-semibold text-amber-500 dark:text-amber-400">
                            <Medal className="h-3.5 w-3.5 shrink-0" />
                            1º
                          </span>
                        )}
                        {isSilver && (
                          <span className="inline-flex items-center gap-1 font-semibold text-muted-foreground">
                            <Medal className="h-3.5 w-3.5 shrink-0" />
                            2º
                          </span>
                        )}
                        {isBronze && (
                          <span className="inline-flex items-center gap-1 font-semibold text-amber-700 dark:text-amber-600">
                            <Medal className="h-3.5 w-3.5 shrink-0" />
                            3º
                          </span>
                        )}
                        {!isGold && !isSilver && !isBronze && (
                          <span className="text-muted-foreground font-medium">{pos}º</span>
                        )}
                      </td>
                      <td className="px-3 py-2.5 font-mono align-middle w-20">{row.code || "—"}</td>
                      <td className="px-3 py-2.5 text-muted-foreground align-middle">{row.description || "—"}</td>
                      <td className="px-3 py-2.5 text-right font-medium align-middle w-28 tabular-nums whitespace-nowrap">
                        {row.total_value.toLocaleString("pt-BR", { style: "currency", currency: "BRL" })}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          {hasMore && (
            <p className="text-[10px] text-muted-foreground mt-2">Exibindo os {maxRows} primeiros por valor.</p>
          )}
        </>
      )}
    </GlassCard>
  );
}

const typeLabels: Record<string, string> = {
  nfs: "NFS - Notas Fiscais de Serviço",
  nfe: "NFE - Notas Fiscais Eletrônicas",
  nfc: "NFC - Notas Fiscais ao Consumidor",
  "nfe-nfc": "NFE / NFC - Notas Fiscais Eletrônicas e ao Consumidor",
  "simples-nacional": "Simples Nacional",
  difal: "DIFAL",
  "irrf-csll": "IRRF/CSLL",
  certidoes: "Certidões",
};

const OBRIGACOES_FISCAIS = ["simples-nacional", "difal", "irrf-csll", "certidoes"];

const typeToDb = (t: string): "NFS" | "NFE" | "NFC" | "NFE_NFC" => {
  const u = t?.toLowerCase();
  if (u === "nfe-nfc") return "NFE_NFC";
  if (u === "nfs" || u === "nfe" || u === "nfc") return t?.toUpperCase() as "NFS" | "NFE" | "NFC";
  return "NFS";
};

const MESES = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"];

/** Extrai data do file_path quando a pasta segue .../Recebidas ou .../Emitidas/YYYY/MM/DD/... */
function getDocumentDateFromPath(filePath: string | null): string | null {
  if (!filePath || typeof filePath !== "string") return null;
  const match = filePath.match(/\/(Recebidas|Emitidas)\/(\d{4})\/(\d{2})\/(\d{1,2})(?:\/|$)/i);
  if (!match) return null;
  const [, , y, m, d] = match;
  const day = d.padStart(2, "0");
  if (parseInt(m, 10) < 1 || parseInt(m, 10) > 12) return null;
  if (parseInt(day, 10) < 1 || parseInt(day, 10) > 31) return null;
  return `${y}-${m}-${day}`;
}

/** Retorna data para exibição: document_date ou extraída do path (NFS Recebidas/Emitidas/YYYY/MM/DD). */
function getDocumentDisplayDate(doc: { document_date?: string | null; file_path?: string | null }): string | null {
  if (doc.document_date) return doc.document_date;
  return getDocumentDateFromPath(doc.file_path ?? null);
}

/** Para NFS (e NFE/NFC se o path tiver a mesma estrutura): retorna "recebidas" | "emitidas" a partir do path. */
function getDocumentOrigem(filePath: string | null, _docType: string): "recebidas" | "emitidas" | null {
  if (!filePath) return null;
  if (/\/Recebidas\//i.test(filePath)) return "recebidas";
  if (/\/Emitidas\//i.test(filePath)) return "emitidas";
  return null;
}

function getDocumentUniqueKey(doc: { chave?: string | null; id: string }) {
  const chave = (doc.chave || "").trim();
  return chave || doc.id;
}

function dedupeDocumentsByKey<T extends { chave?: string | null; id: string }>(documents: T[]): T[] {
  const seen = new Set<string>();
  const result: T[] = [];
  for (const doc of documents) {
    const key = getDocumentUniqueKey(doc);
    if (seen.has(key)) continue;
    seen.add(key);
    result.push(doc);
  }
  return result;
}

function getDocumentModel(docType: string): "55" | "65" | null {
  if (docType === "NFE") return "55";
  if (docType === "NFC") return "65";
  return null;
}

/** Gera dados do gráfico Volume Mensal a partir dos documentos (campo periodo = YYYY-MM). Últimos 12 meses. */
function buildVolumeMensalData(documents: { periodo?: string | null }[]): { name: string; value: number }[] {
  const byPeriodo = new Map<string, number>();
  for (const d of documents) {
    const p = (d.periodo || "").trim();
    if (!p || !/^\d{4}-\d{2}$/.test(p)) continue;
    byPeriodo.set(p, (byPeriodo.get(p) ?? 0) + 1);
  }
  const now = new Date();
  const result: { name: string; value: number }[] = [];
  for (let i = 11; i >= 0; i--) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
    const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
    const label = `${MESES[d.getMonth()]}/${String(d.getFullYear()).slice(2)}`;
    result.push({ name: label, value: byPeriodo.get(key) ?? 0 });
  }
  return result;
}

function buildVolumeMensalDataUnique(documents: { periodo?: string | null; chave?: string | null; id: string }[]): { name: string; value: number }[] {
  return buildVolumeMensalData(
    dedupeDocumentsByKey(documents).map((doc) => ({
      periodo: doc.periodo,
    }))
  );
}

/** Volume mensal para NFS: respeita o filtro De/Até, só meses do período, conta notas (chave única) por mês. */
function buildVolumeMensalDataNfs(
  filteredDocuments: { periodo?: string | null; chave?: string | null; id: string }[],
  dateFrom: string,
  dateTo: string
): { name: string; value: number }[] {
  const from = dateFrom.slice(0, 7);
  const to = dateTo.slice(0, 7);
  if (from > to) return [];
  const months: string[] = [];
  const yFrom = parseInt(from.slice(0, 4), 10);
  const mFrom = parseInt(from.slice(5, 7), 10);
  const yTo = parseInt(to.slice(0, 4), 10);
  const mTo = parseInt(to.slice(5, 7), 10);
  for (let y = yFrom; y <= yTo; y++) {
    const mStart = y === yFrom ? mFrom : 1;
    const mEnd = y === yTo ? mTo : 12;
    for (let m = mStart; m <= mEnd; m++) {
      months.push(`${y}-${String(m).padStart(2, "0")}`);
    }
  }
  const byPeriodo = new Map<string, Set<string>>();
  for (const d of filteredDocuments) {
    const p = (d.periodo || "").trim();
    if (!p || !/^\d{4}-\d{2}$/.test(p)) continue;
    const chave = (d.chave || "").trim() || d.id;
    if (!byPeriodo.has(p)) byPeriodo.set(p, new Set());
    byPeriodo.get(p)!.add(chave);
  }
  return months.map((key) => {
    const [y, m] = [parseInt(key.slice(0, 4), 10), parseInt(key.slice(5, 7), 10)];
    const label = `${MESES[m - 1]}/${String(y).slice(2)}`;
    const count = byPeriodo.get(key)?.size ?? 0;
    return { name: label, value: count };
  });
}

function formatarDataCertidao(iso: string) {
  const d = new Date(iso + "T12:00:00");
  return d.toLocaleDateString("pt-BR", { day: "2-digit", month: "2-digit", year: "numeric" });
}

function CertidoesContent({ companyFilter }: { companyFilter: string[] | null }) {
  const [search, setSearch] = useState("");
  const [tipoFiltro, setTipoFiltro] = useState("all");
  const { data: certidoes = [], isLoading } = useQuery({
    queryKey: ["certidoes-documents", companyFilter],
    queryFn: () => getCertidoesDocuments(companyFilter),
  });

  const filteredList = useMemo(() => {
    const q = search.trim().toLowerCase();
    return certidoes.filter((c) => {
      const matchesSearch =
        !q ||
        String(c.empresa || "").toLowerCase().includes(q) ||
        String(c.tipo_certidao || "").toLowerCase().includes(q) ||
        String(c.cnpj || "").toLowerCase().includes(q);
      const tipoNormalizado = String(c.tipo_certidao || "").toLowerCase();
      const matchesTipo =
        tipoFiltro === "all" ||
        (tipoFiltro === "federal" && tipoNormalizado === "federal") ||
        (tipoFiltro === "fgts" && tipoNormalizado === "fgts") ||
        (tipoFiltro === "estadual_go" && tipoNormalizado.includes("estadual"));
      return matchesSearch && matchesTipo;
    });
  }, [certidoes, search, tipoFiltro]);

  const chartData = useMemo(() => {
    const rows = [
      { name: "Negativas", value: 0, color: "hsl(214, 84%, 56%)" },
      { name: "Irregulares", value: 0, color: "hsl(0, 72%, 51%)" },
    ];
    for (const cert of certidoes) {
      const status = String(cert.status || "").toLowerCase();
      if (status === "regular" || status === "negativa") rows[0].value += 1;
      else rows[1].value += 1;
    }
    return rows;
  }, [certidoes]);

  const handleBaixarPdf = async (filePath: string | null, tipoCertidao: string) => {
    if (!filePath) {
      toast.error("PDF não disponível para esta certidão.");
      return;
    }
    if (!hasServerApi()) {
      toast.error("SERVER_API_URL não configurada para baixar o PDF.");
      return;
    }
    try {
      await downloadServerFileByPath(filePath, `${String(tipoCertidao || "certidao").toLowerCase()}.pdf`);
      toast.success("PDF baixado com sucesso.");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Não foi possível baixar o PDF.");
    }
  };

  const statusBadge = (statusRaw: string) => {
    const status = String(statusRaw || "").toLowerCase();
    if (status === "regular") return <span className="inline-flex items-center gap-1 rounded-full bg-sky-500/15 px-2 py-0.5 text-xs font-medium text-sky-700 dark:text-sky-400">Negativa</span>;
    if (status === "negativa") return <span className="inline-flex items-center gap-1 rounded-full bg-sky-500/15 px-2 py-0.5 text-xs font-medium text-sky-700 dark:text-sky-400">Negativa</span>;
    if (status === "positiva") return <span className="inline-flex items-center gap-1 rounded-full bg-red-500/15 px-2 py-0.5 text-xs font-medium text-red-700 dark:text-red-400">Positiva</span>;
    return <span className="inline-flex items-center gap-1 rounded-full bg-amber-500/15 px-2 py-0.5 text-xs font-medium text-amber-700 dark:text-amber-400">Irregular</span>;
  };

  return (
    <div className="space-y-6">
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        <GlassCard className="lg:col-span-5 p-6 flex flex-col items-center justify-center">
          <h3 className="text-sm font-semibold font-display mb-2 w-full text-left">Situação das certidões</h3>
          <p className="text-xs text-muted-foreground mb-4 w-full text-left">Dados reais vindos do robô e do Supabase</p>
          <div className="w-full max-w-[260px] h-[240px] flex items-center justify-center">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart margin={{ top: 36, right: 36, left: 36, bottom: 36 }}>
                <Pie data={chartData} cx="50%" cy="50%" innerRadius={52} outerRadius={72} paddingAngle={2} dataKey="value" stroke="transparent" label={({ value }) => value} labelLine={false}>
                  {chartData.map((entry, index) => <Cell key={`cell-${index}`} fill={entry.color} />)}
                </Pie>
                <Tooltip formatter={(value: number) => [value, "certidões"]} contentStyle={{ background: "#ffffff", color: "#111827", borderRadius: "10px", border: "1px solid rgba(15, 23, 42, 0.12)", fontSize: "12px" }} labelFormatter={(label) => label} />
              </PieChart>
            </ResponsiveContainer>
          </div>
          <div className="flex flex-wrap justify-center gap-x-4 gap-y-1 mt-2 text-xs">
            {chartData.map((entry) => (
              <span key={entry.name} className="flex items-center gap-1.5">
                <span className="w-2.5 h-2.5 rounded-full shrink-0" style={{ backgroundColor: entry.color }} />
                {entry.name}
              </span>
            ))}
          </div>
        </GlassCard>
        <div className="lg:col-span-7 grid grid-cols-1 sm:grid-cols-2 gap-4">

          <GlassCard className="p-5 border-l-4 border-l-sky-500 bg-sky-500/5 dark:bg-sky-500/10">
            <div className="flex items-center gap-2 text-sky-600 dark:text-sky-500"><FileText className="h-5 w-5 shrink-0" /><span className="text-sm font-medium">Negativas</span></div>
            <p className="text-2xl font-bold mt-2">{chartData[0]?.value ?? 0}</p>
            <p className="text-xs text-muted-foreground mt-1">Certidões negativas e regulares</p>
          </GlassCard>
          <GlassCard className="p-5 border-l-4 border-l-red-500 bg-red-500/5 dark:bg-red-500/10">
            <div className="flex items-center gap-2 text-red-600 dark:text-red-500"><AlertCircle className="h-5 w-5 shrink-0" /><span className="text-sm font-medium">Irregulares</span></div>
            <p className="text-2xl font-bold mt-2">{chartData[1]?.value ?? 0}</p>
            <p className="text-xs text-muted-foreground mt-1">Positivas ou com irregularidade</p>
          </GlassCard>
        </div>
      </div>

      <GlassCard className="overflow-hidden">
        <div className="p-3 sm:p-4 border-b border-border flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2">
          <h3 className="text-sm font-semibold font-display">Certidões</h3>
          <div className="flex w-full flex-col gap-2 sm:w-auto sm:flex-row sm:items-center">
            <Select value={tipoFiltro} onValueChange={setTipoFiltro}>
              <SelectTrigger className="h-auto w-full rounded-lg border-border bg-background px-3 py-2 text-xs focus:ring-1 focus:ring-ring focus:ring-offset-0 sm:w-[180px] sm:py-1.5">
                <SelectValue placeholder="Filtrar tipo" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Todas</SelectItem>
                <SelectItem value="federal">Federal</SelectItem>
                <SelectItem value="fgts">FGTS</SelectItem>
                <SelectItem value="estadual_go">Estadual (GO)</SelectItem>
              </SelectContent>
            </Select>
            <input value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Buscar por empresa, CNPJ ou tipo..." className="rounded-lg border border-border bg-background px-3 py-2 sm:py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-ring w-full min-w-0 sm:w-64 sm:max-w-[18rem]" />
          </div>
        </div>
        <div className="overflow-x-auto -webkit-overflow-scrolling-touch">
          {isLoading ? (
            <div className="p-8 text-center text-sm text-muted-foreground">Carregando certidões...</div>
          ) : filteredList.length === 0 ? (
            <div className="p-8 text-center text-sm text-muted-foreground">Nenhuma certidão encontrada.</div>
          ) : (
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border bg-muted/50">
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Empresa</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Tipo</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Competência</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Atualização</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Situação</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Ações</th>
                </tr>
              </thead>
              <tbody>
                {filteredList.map((cert) => (
                  <tr key={cert.id} className="border-b border-border hover:bg-muted/30 transition-colors">
                    <td className="px-4 py-3">
                      <div className="font-medium">{cert.empresa}</div>
                      <div className="text-muted-foreground">{cert.cnpj || "—"}</div>
                    </td>
                    <td className="px-4 py-3 text-muted-foreground">{cert.tipo_certidao}</td>
                    <td className="px-4 py-3 text-muted-foreground">{cert.periodo || "—"}</td>
                    <td className="px-4 py-3 text-muted-foreground">{cert.document_date ? formatarDataCertidao(cert.document_date) : "—"}</td>
                    <td className="px-4 py-3">{statusBadge(cert.status)}</td>
                    <td className="px-4 py-3">
                      <Button variant="outline" size="sm" className="h-7 text-xs gap-1" onClick={() => handleBaixarPdf(cert.file_path, cert.tipo_certidao)} disabled={!cert.file_path}>
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
      </GlassCard>
    </div>
  );
}

export default function FiscalDetailPage() {
  const { type } = useParams<{ type: string }>();
  const [search, setSearch] = useState("");
  const { selectedCompanyIds } = useSelectedCompanyIds();
  const companyFilter = selectedCompanyIds.length > 0 ? selectedCompanyIds : null;
  const dbType = typeToDb(type ?? "");
  const label = typeLabels[type ?? "nfs"] || "Documentos Fiscais";
  const isObrigacao = type && OBRIGACOES_FISCAIS.includes(type);
  const isNfeNfc = type === "nfe-nfc";

  const { data: documentsByType = [], isLoading: loadingByType } = useQuery({
    queryKey: ["fiscal-documents", dbType, companyFilter],
    queryFn: () => getFiscalDocumentsByType(dbType as "NFS" | "NFE" | "NFC", companyFilter),
    enabled: !isObrigacao && !isNfeNfc,
  });

  const { data: documentsNfeNfc = [], isLoading: loadingNfeNfc } = useQuery({
    queryKey: ["fiscal-documents-nfe-nfc", companyFilter],
    queryFn: () => getFiscalDocumentsNfeNfc(companyFilter),
    enabled: !isObrigacao && isNfeNfc,
  });

  const documents = isNfeNfc ? documentsNfeNfc : documentsByType;
  const isLoading = isNfeNfc ? loadingNfeNfc : loadingByType;

  const mesAtual = useMemo(() => {
    const now = new Date();
    return `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
  }, []);
  const nfsPeriodDefault = useMemo(() => {
    const now = new Date();
    const first = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-01`;
    const last = new Date(now.getFullYear(), now.getMonth() + 1, 0).toISOString().slice(0, 10);
    return { first, last };
  }, []);

  const [filterDateFrom, setFilterDateFrom] = useState("");
  const [filterDateTo, setFilterDateTo] = useState("");
  const [nfsDateFrom, setNfsDateFrom] = useState("");
  const [nfsDateTo, setNfsDateTo] = useState("");
  const [nfeNfcDateFrom, setNfeNfcDateFrom] = useState("");
  const [nfeNfcDateTo, setNfeNfcDateTo] = useState("");
  const [filterFileType, setFilterFileType] = useState<"all" | "xml" | "pdf">("all");
  const [filterOrigem, setFilterOrigem] = useState<"all" | "recebidas" | "emitidas">("all");
  const [filterModelo, setFilterModelo] = useState<"all" | "55" | "65">("all");
  const [downloadingZip, setDownloadingZip] = useState(false);
  const [pageSize, setPageSize] = useState(10);
  const [currentPage, setCurrentPage] = useState(1);

  const nfsDateFromResolved = type === "nfs" ? (nfsDateFrom || nfsPeriodDefault.first) : "";
  const nfsDateToResolved = type === "nfs" ? (nfsDateTo || nfsPeriodDefault.last) : "";
  const nfeNfcDateFromResolved = isNfeNfc ? (nfeNfcDateFrom || nfsPeriodDefault.first) : "";
  const nfeNfcDateToResolved = isNfeNfc ? (nfeNfcDateTo || nfsPeriodDefault.last) : "";
  const baseDocuments = useMemo(
    () => (isNfeNfc ? dedupeDocumentsByKey(documents) : documents),
    [documents, isNfeNfc]
  );

  const filteredDocuments = useMemo(() => {
    let list = baseDocuments;
    if (search.trim()) {
      const q = search.toLowerCase();
      list = list.filter(
        (d) =>
          (d.empresa && d.empresa.toLowerCase().includes(q)) ||
          (d.cnpj && d.cnpj.replace(/\D/g, "").includes(q.replace(/\D/g, ""))) ||
          (d.chave && d.chave.includes(q))
      );
    }
    const dateFrom = type === "nfs" ? nfsDateFromResolved : isNfeNfc ? nfeNfcDateFromResolved : filterDateFrom;
    const dateTo = type === "nfs" ? nfsDateToResolved : isNfeNfc ? nfeNfcDateToResolved : filterDateTo;
    if (dateFrom || dateTo) {
      list = list.filter((d) => {
        const date = getDocumentDisplayDate(d) ?? d.periodo ?? "";
        if (!date) return true;
        const docDate = date.slice(0, 10);
        if (docDate.length === 7) {
          const [y, m] = [parseInt(docDate.slice(0, 4), 10), parseInt(docDate.slice(5, 7), 10)];
          const firstDay = `${y}-${String(m).padStart(2, "0")}-01`;
          const lastDay = new Date(y, m, 0).toISOString().slice(0, 10);
          if (dateFrom && lastDay < dateFrom) return false;
          if (dateTo && firstDay > dateTo) return false;
          return true;
        }
        if (dateFrom && docDate < dateFrom) return false;
        if (dateTo && docDate > dateTo) return false;
        return true;
      });
    }
    if (!isNfeNfc && filterFileType !== "all") {
      list = list.filter((d) => {
        const fp = (d.file_path || "").toLowerCase();
        if (filterFileType === "xml") return fp.endsWith(".xml");
        if (filterFileType === "pdf") return fp.endsWith(".pdf");
        return true;
      });
    }
    if (!isNfeNfc && filterOrigem !== "all") {
      list = list.filter((d) => getDocumentOrigem(d.file_path ?? null, d.type) === filterOrigem);
    }
    if (isNfeNfc && filterModelo !== "all") {
      list = list.filter((d) => getDocumentModel(d.type) === filterModelo);
    }
    return list;
  }, [
    baseDocuments,
    search,
    filterDateFrom,
    filterDateTo,
    nfsDateFromResolved,
    nfsDateToResolved,
    nfeNfcDateFromResolved,
    nfeNfcDateToResolved,
    filterFileType,
    filterOrigem,
    filterModelo,
    type,
    isNfeNfc,
  ]);

  const canDownload = hasServerApi();
  const nfsStatsQuery = useQuery({
    queryKey: ["nfs-stats", companyFilter, nfsDateFromResolved, nfsDateToResolved],
    queryFn: () => getNfsStatsByDateRange(companyFilter, nfsDateFromResolved, nfsDateToResolved),
    enabled: type === "nfs" && !!nfsDateFromResolved && !!nfsDateToResolved,
  });
  const nfsStats = nfsStatsQuery.data;
  const loadingNfsStats = nfsStatsQuery.isLoading;

  const nfsPrevPeriod = useMemo(() => {
    if (!nfsDateFromResolved || nfsDateFromResolved.length < 7) return null;
    const y = parseInt(nfsDateFromResolved.slice(0, 4), 10);
    const m = parseInt(nfsDateFromResolved.slice(5, 7), 10);
    const prev = new Date(y, m - 2, 1);
    const first = `${prev.getFullYear()}-${String(prev.getMonth() + 1).padStart(2, "0")}-01`;
    const last = new Date(prev.getFullYear(), prev.getMonth() + 1, 0).toISOString().slice(0, 10);
    return { first, last };
  }, [nfsDateFromResolved]);

  const nfsStatsPrevQuery = useQuery({
    queryKey: ["nfs-stats-prev", companyFilter, nfsPrevPeriod?.first, nfsPrevPeriod?.last],
    queryFn: () => getNfsStatsByDateRange(companyFilter, nfsPrevPeriod!.first, nfsPrevPeriod!.last),
    enabled: type === "nfs" && !!nfsPrevPeriod?.first && !!nfsPrevPeriod?.last,
  });
  const nfsStatsPrev = nfsStatsPrevQuery.data;

  const nfsVariationEmitidas = useMemo(() => {
    if (!nfsStats || !nfsStatsPrev) return null;
    const curr = nfsStats.valorEmitidas;
    const prev = nfsStatsPrev.valorEmitidas;
    if (prev === 0) {
      if (curr === 0) return "igual ao mês anterior";
      return "+100% vs mês anterior";
    }
    const pct = ((curr - prev) / prev) * 100;
    if (pct === 0) return "igual ao mês anterior";
    const sign = pct > 0 ? "+" : "";
    return `${sign}${pct.toFixed(1).replace(".", ",")}% vs mês anterior`;
  }, [nfsStats, nfsStatsPrev]);

  const nfsVariationRecebidas = useMemo(() => {
    if (!nfsStats || !nfsStatsPrev) return null;
    const curr = nfsStats.valorRecebidas;
    const prev = nfsStatsPrev.valorRecebidas;
    if (prev === 0) {
      if (curr === 0) return "igual ao mês anterior";
      return "+100% vs mês anterior";
    }
    const pct = ((curr - prev) / prev) * 100;
    if (pct === 0) return "igual ao mês anterior";
    const sign = pct > 0 ? "+" : "";
    return `${sign}${pct.toFixed(1).replace(".", ",")}% vs mês anterior`;
  }, [nfsStats, nfsStatsPrev]);

  const nfsVariationTypeEmitidas = useMemo(() => {
    if (!nfsStats || !nfsStatsPrev) return "neutral";
    const prev = nfsStatsPrev.valorEmitidas;
    const curr = nfsStats.valorEmitidas;
    if (prev === 0) return curr > 0 ? "positive" : "neutral";
    const pct = ((curr - prev) / prev) * 100;
    return pct > 0 ? "positive" : pct < 0 ? "negative" : "neutral";
  }, [nfsStats, nfsStatsPrev]);

  const nfsVariationTypeRecebidas = useMemo(() => {
    if (!nfsStats || !nfsStatsPrev) return "neutral";
    const prev = nfsStatsPrev.valorRecebidas;
    const curr = nfsStats.valorRecebidas;
    if (prev === 0) return curr > 0 ? "positive" : "neutral";
    const pct = ((curr - prev) / prev) * 100;
    return pct > 0 ? "positive" : pct < 0 ? "negative" : "neutral";
  }, [nfsStats, nfsStatsPrev]);
  const comArquivo = (type === "nfs" ? filteredDocuments : baseDocuments).filter((d) => d.file_path && String(d.file_path).trim()).length;
  const disponiveisCount = comArquivo;
  const docCountForDisplay = useMemo(() => {
    if (type === "nfs" || isNfeNfc) {
      const chaves = new Set<string>();
      for (const d of (type === "nfs" ? filteredDocuments : baseDocuments)) {
        chaves.add(getDocumentUniqueKey(d));
      }
      return chaves.size;
    }
    return baseDocuments.length;
  }, [type, filteredDocuments, baseDocuments, isNfeNfc]);
  const esteMes = useMemo(() => {
    if (type === "nfs" || isNfeNfc) {
      const chaves = new Set<string>();
      for (const d of baseDocuments) {
        const p = (d.periodo || "").trim();
        if (!/^\d{4}-\d{2}$/.test(p) || p !== mesAtual) continue;
        chaves.add(getDocumentUniqueKey(d));
      }
      return chaves.size;
    }
    return baseDocuments.filter((d) => {
      const p = (d.periodo || "").trim();
      return /^\d{4}-\d{2}$/.test(p) && p === mesAtual;
    }).length;
  }, [type, baseDocuments, mesAtual, isNfeNfc]);
  const nfeCount = isNfeNfc ? baseDocuments.filter((d) => d.type === "NFE").length : 0;
  const nfcCount = isNfeNfc ? baseDocuments.filter((d) => d.type === "NFC").length : 0;

  const volumeMensalData = useMemo(() => {
    if (type === "nfs" && nfsDateFromResolved && nfsDateToResolved) {
      return buildVolumeMensalDataNfs(filteredDocuments, nfsDateFromResolved, nfsDateToResolved);
    }
    if (isNfeNfc) {
      return buildVolumeMensalDataUnique(baseDocuments);
    }
    return buildVolumeMensalData(baseDocuments);
  }, [type, baseDocuments, filteredDocuments, nfsDateFromResolved, nfsDateToResolved, isNfeNfc]);

  const documentsPagination = useMemo(() => {
    const total = filteredDocuments.length;
    const safePageSize = Math.max(1, pageSize);
    const totalPages = Math.max(1, Math.ceil(total / safePageSize));
    const page = Math.min(currentPage, totalPages);
    const from = (page - 1) * safePageSize;
    const to = Math.min(from + safePageSize, total);
    return {
      list: filteredDocuments.slice(from, to),
      totalPages,
      currentPage: page,
      from: total ? from + 1 : 0,
      to,
      total,
    };
  }, [filteredDocuments, pageSize, currentPage]);
  const documentsToShow = documentsPagination.list;

  useEffect(() => {
    if (documentsPagination.totalPages > 0 && currentPage > documentsPagination.totalPages) {
      setCurrentPage(1);
    }
  }, [currentPage, documentsPagination.totalPages]);

  const handleDownload = async (id: string, chave: string | null, filePath: string | null) => {
    try {
      const suggestedName = filePath ? filePath.split("/").pop() || (chave ? `documento-${chave}` : undefined) : undefined;
      await downloadFiscalDocument(id, suggestedName);
      await markFiscalDocumentDownloaded(id);
      toast.success("Download iniciado.");
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Erro ao baixar o arquivo.");
    }
  };

  const getDownloadLabel = (filePath: string | null) => {
    if (!filePath) return "Baixar";
    const lower = filePath.toLowerCase();
    if (lower.endsWith(".pdf")) return "Baixar PDF";
    if (lower.endsWith(".xml")) return "Baixar XML";
    return "Baixar";
  };

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold font-display tracking-tight">{label}</h1>
        <p className="text-sm text-muted-foreground mt-1">
          {isObrigacao ? (type === "certidoes" ? "Emitir e consultar certidões fiscais e negativas de débito." : "Obrigações e apurações deste tópico") : "Detalhamento de XMLs e status. Baixe o XML pelo servidor quando disponível."}
        </p>
      </div>

      {isObrigacao ? (
        type === "certidoes" ? (
          <CertidoesContent companyFilter={companyFilter} />
        ) : (
          <GlassCard className="p-8">
            <p className="text-sm text-muted-foreground">Conteúdo específico desta obrigação será exibido aqui.</p>
          </GlassCard>
        )
      ) : (
        <>
      {(type === "nfs" || isNfeNfc) && (
        <GlassCard className="p-4">
          <div className="flex flex-wrap items-center gap-3">
            <div className="flex items-center gap-2 text-muted-foreground">
              <Calendar className="h-4 w-4" />
              <span className="text-sm font-medium">Período</span>
            </div>
            <label className="flex items-center gap-2">
              <span className="text-xs text-muted-foreground">De</span>
              <input
                type="date"
                value={type === "nfs" ? nfsDateFromResolved : nfeNfcDateFromResolved}
                onChange={(e) => (type === "nfs" ? setNfsDateFrom(e.target.value) : setNfeNfcDateFrom(e.target.value))}
                className="rounded-lg border border-border bg-background px-3 py-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring"
              />
            </label>
            <label className="flex items-center gap-2">
              <span className="text-xs text-muted-foreground">Até</span>
              <input
                type="date"
                value={type === "nfs" ? nfsDateToResolved : nfeNfcDateToResolved}
                onChange={(e) => (type === "nfs" ? setNfsDateTo(e.target.value) : setNfeNfcDateTo(e.target.value))}
                className="rounded-lg border border-border bg-background px-3 py-2 text-xs focus:outline-none focus:ring-1 focus:ring-ring"
              />
            </label>
          </div>
          <p className="text-xs text-muted-foreground mt-2">Todos os dados da página (documentos, totais e ranking) seguem este período.</p>
        </GlassCard>
      )}

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {type === "nfs" ? (
          <>
            <StatsCard title="Total no período" value={docCountForDisplay.toString()} icon={FileText} />
            <StatsCard
              title="Valor prestadas (R$)"
              value={nfsStats ? nfsStats.valorEmitidas.toLocaleString("pt-BR", { style: "currency", currency: "BRL" }) : "—"}
              icon={DollarSign}
              change={nfsVariationEmitidas ?? undefined}
              changeType={nfsVariationTypeEmitidas}
            />
            <StatsCard
              title="Valor tomadas (R$)"
              value={nfsStats ? nfsStats.valorRecebidas.toLocaleString("pt-BR", { style: "currency", currency: "BRL" }) : "—"}
              icon={DollarSign}
              change={nfsVariationRecebidas ?? undefined}
              changeType={nfsVariationTypeRecebidas}
            />
            <StatsCard title="Este mês" value={esteMes.toString()} icon={CalendarDays} changeType="neutral" />
          </>
        ) : (
          <>
            <StatsCard title="Total" value={docCountForDisplay.toString()} icon={FileText} />
            <StatsCard title="Disponíveis" value={disponiveisCount.toString()} icon={FileDown} change={docCountForDisplay ? `${((disponiveisCount / docCountForDisplay) * 100).toFixed(1)}%` : "0%"} changeType="positive" />
            <StatsCard title="Este mês" value={esteMes.toString()} icon={CalendarDays} changeType="neutral" />
            {isNfeNfc && (
              <>
                <StatsCard title="NFE" value={nfeCount.toString()} icon={FileText} changeType="neutral" />
                <StatsCard title="NFC" value={nfcCount.toString()} icon={FileText} changeType="neutral" />
              </>
            )}
          </>
        )}
      </div>

      {type === "nfs" && (
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <ServiceCodesRankingTable
            title="Ranking de códigos de serviço (prestadas)"
            subtitle="Códigos das notas emitidas pela empresa. Dados enviados pelo robô ao concluir a execução."
            rows={nfsStats?.serviceCodesRankingPrestadas ?? []}
            loading={loadingNfsStats}
            emptyMessage="Nenhum dado de códigos de serviço (prestadas) neste período. Execute o robô NFS para popular."
          />
          <ServiceCodesRankingTable
            title="Ranking de códigos de serviço (tomadas)"
            subtitle="Códigos das notas recebidas pela empresa. Dados enviados pelo robô ao concluir a execução."
            rows={nfsStats?.serviceCodesRankingTomadas ?? []}
            loading={loadingNfsStats}
            emptyMessage="Nenhum dado de códigos de serviço (tomadas) neste período. Execute o robô NFS para popular."
          />
        </div>
      )}

      <GlassCard className="p-6">
        <h3 className="text-sm font-semibold font-display mb-4">Volume Mensal</h3>
        <MiniChart
          data={volumeMensalData}
          type="area"
          height={200}
          valueLabel={type === "nfs" ? "Notas" : undefined}
        />
      </GlassCard>

      <GlassCard className="overflow-hidden">
        <div className="p-3 sm:p-4 border-b border-border flex flex-col gap-3">
          <h3 className="text-sm font-semibold font-display">Documentos</h3>
          <div className="flex flex-wrap items-center gap-2">
            <input
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Buscar por empresa ou chave..."
              className="rounded-lg border border-border bg-background px-3 py-2 sm:py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-ring w-full min-w-0 sm:w-40 max-w-[12rem]"
            />
            {!isNfeNfc && type !== "nfs" && (
              <>
                <input
                  type="date"
                  value={filterDateFrom}
                  onChange={(e) => setFilterDateFrom(e.target.value)}
                  className="rounded-lg border border-border bg-background px-3 py-2 sm:py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-ring"
                  title="Data a partir de"
                />
                <input
                  type="date"
                  value={filterDateTo}
                  onChange={(e) => setFilterDateTo(e.target.value)}
                  className="rounded-lg border border-border bg-background px-3 py-2 sm:py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-ring"
                  title="Data até"
                />
              </>
            )}
            {!isNfeNfc && (
            <select
              value={filterFileType}
              onChange={(e) => setFilterFileType(e.target.value as "all" | "xml" | "pdf")}
              className="rounded-lg border border-border bg-background px-3 py-2 sm:py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-ring"
              title="Tipo de arquivo"
            >
              <option value="all">Todos (XML e PDF)</option>
              <option value="xml">Só XML</option>
              <option value="pdf">Só PDF</option>
            </select>
            )}
            {type === "nfs" && (
              <select
                value={filterOrigem}
                onChange={(e) => setFilterOrigem(e.target.value as "all" | "recebidas" | "emitidas")}
                className="rounded-lg border border-border bg-background px-3 py-2 sm:py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-ring"
                title="Origem"
              >
                <option value="all">NFS Tomada e Prestada</option>
                <option value="recebidas">NFS Tomada (Recebidas)</option>
                <option value="emitidas">NFS Prestada (Emitidas)</option>
              </select>
            )}
            {isNfeNfc && (
              <select
                value={filterModelo}
                onChange={(e) => setFilterModelo(e.target.value as "all" | "55" | "65")}
                className="rounded-lg border border-border bg-background px-3 py-2 sm:py-1.5 text-xs focus:outline-none focus:ring-1 focus:ring-ring"
                title="Modelo"
              >
                <option value="all">Modelos 55 e 65</option>
                <option value="55">Somente 55 (NFE)</option>
                <option value="65">Somente 65 (NFC)</option>
              </select>
            )}
            {canDownload && (
                <Button
                  variant="default"
                  size="sm"
                  className="gap-1.5 text-xs"
                  disabled={downloadingZip || filteredDocuments.filter((d) => d.file_path).length === 0}
                  onClick={async () => {
                    const companyIds = [...new Set(filteredDocuments.filter((d) => d.file_path && String(d.file_path).trim()).map((d) => d.company_id))];
                    if (companyIds.length === 0) {
                      toast.error("Nenhum documento com arquivo disponível na lista.");
                      return;
                    }
                    setDownloadingZip(true);
                    try {
                      const types = type ? [typeToDb(type)] : [];
                      await downloadFiscalCompaniesZip(companyIds, type ?? undefined, types);
                      toast.success(`Download iniciado: ${companyIds.length} empresa(s) (documentos listados).`);
                    } catch (e) {
                      toast.error(e instanceof Error ? e.message : "Erro ao baixar ZIP.");
                    } finally {
                    setDownloadingZip(false);
                  }
                }}
              >
                <FileArchive className="h-3.5 w-3.5" />
                {downloadingZip ? "Gerando…" : "Baixar ZIP dos documentos listados"}
              </Button>
            )}
          </div>
        </div>
        <div className="overflow-x-auto -webkit-overflow-scrolling-touch">
          {isLoading ? (
            <div className="p-8 text-center text-sm text-muted-foreground">Carregando...</div>
          ) : filteredDocuments.length === 0 ? (
            <div className="p-8 text-center text-sm text-muted-foreground">Nenhum documento encontrado. Os robôs podem popular os dados ao processar XMLs.</div>
          ) : (
            <>
            <table className="w-full text-xs">
              <thead>
                <tr className="border-b border-border bg-muted/50">
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Empresa</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">CNPJ</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Período</th>
                  {type === "nfs" && (
                    <th className="text-left px-4 py-3 font-medium text-muted-foreground">Origem</th>
                  )}
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Chave</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Status</th>
                  <th className="text-left px-4 py-3 font-medium text-muted-foreground">Data</th>
                  {canDownload && <th className="text-left px-4 py-3 font-medium text-muted-foreground">Ações</th>}
                </tr>
              </thead>
              <tbody>
                {documentsToShow.map((doc) => {
                  const displayDate = getDocumentDisplayDate(doc);
                  const origem = getDocumentOrigem(doc.file_path ?? null, doc.type);
                  const modelo = getDocumentModel(doc.type);
                  return (
                  <tr key={doc.id} className="border-b border-border hover:bg-muted/30 transition-colors">
                    <td className="px-4 py-3 font-medium">{doc.empresa}</td>
                    <td className="px-4 py-3 text-muted-foreground">{doc.cnpj ?? "—"}</td>
                    <td className="px-4 py-3">{doc.periodo}</td>
                    {type === "nfs" && (
                      <td className="px-4 py-3 text-muted-foreground">
                        {origem === "recebidas" ? "Tomada (Recebidas)" : origem === "emitidas" ? "Prestada (Emitidas)" : "—"}
                      </td>
                    )}
                    <td className="px-4 py-3 text-muted-foreground font-mono text-[10px]">{doc.chave ? `${doc.chave.slice(0, 20)}...` : "—"}</td>
                    <td className="px-4 py-3"><StatusBadge status={doc.status as "validado" | "novo" | "divergente" | "processando" | "pendente"} /></td>
                    <td className="px-4 py-3 text-muted-foreground">{displayDate ?? "—"}</td>
                    {canDownload && (
                      <td className="px-4 py-3">
                        {doc.file_path ? (
                          <button
                            type="button"
                            onClick={() => handleDownload(doc.id, doc.chave, doc.file_path)}
                            className="inline-flex items-center gap-1 rounded-md px-2 py-1 text-xs font-medium bg-primary/10 text-primary-icon hover:bg-primary/20 transition-colors"
                          >
                            <Download className="h-3.5 w-3.5" /> {getDownloadLabel(doc.file_path)}
                          </button>
                        ) : (
                          <span className="text-muted-foreground text-[10px]">Sem arquivo</span>
                        )}
                      </td>
                    )}
                  </tr>
                );})}
              </tbody>
            </table>
            {filteredDocuments.length > 0 && (
              <DataPagination
                currentPage={documentsPagination.currentPage}
                totalPages={documentsPagination.totalPages}
                totalItems={documentsPagination.total}
                from={documentsPagination.from}
                to={documentsPagination.to}
                pageSize={pageSize}
                onPageChange={setCurrentPage}
                onPageSizeChange={(next) => {
                  setPageSize(next);
                  setCurrentPage(1);
                }}
              />
            )}
            </>
          )}
        </div>
      </GlassCard>
        </>
      )}
    </div>
  );
}
