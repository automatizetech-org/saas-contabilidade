import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { FileText, Download, Filter, Search, FileArchive } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useSelectedCompanyIds } from "@/hooks/useSelectedCompanies";
import { getAllHubDocuments } from "@/services/dashboardService";
import { downloadListedFilesZipWithCategory, downloadServerFileByPath, hasServerApi } from "@/services/serverFileService";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { DataPagination } from "@/components/common/DataPagination";

function exportToCsv(
  data: Array<{ empresa: string; cnpj: string | null; type: string; periodo: string; status: string; document_date: string | null; created_at: string }>
) {
  const headers = ["Empresa", "CNPJ", "Tipo", "Período", "Status", "Data"];
  const rows = data.map((d) =>
    [d.empresa, d.cnpj ?? "", d.type, d.periodo, d.status, (d.document_date ?? d.created_at ?? "").slice(0, 10)].map((c) =>
      `"${String(c).replace(/"/g, '""')}"`
    ).join(",")
  );
  const csv = [headers.join(","), ...rows].join("\n");
  const blob = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `documentos-${new Date().toISOString().slice(0, 10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

export default function DocumentosPage() {
  const [filterFileKind, setFilterFileKind] = useState<"Todos" | "XML" | "PDF">("Todos");
  const [filterCategory, setFilterCategory] = useState<string>("Todos");
  const [search, setSearch] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [pageSize, setPageSize] = useState(10);
  const [currentPage, setCurrentPage] = useState(1);
  const [downloadingZip, setDownloadingZip] = useState(false);
  const { selectedCompanyIds } = useSelectedCompanyIds();
  const companyFilter = selectedCompanyIds.length > 0 ? selectedCompanyIds : null;

  const { data: documents = [], isLoading } = useQuery({
    queryKey: ["hub-documents-all", companyFilter],
    queryFn: () => getAllHubDocuments(companyFilter),
  });

  const normalize = (v: unknown) =>
    String(v ?? "")
      .trim()
      .toLowerCase()
      .normalize("NFD")
      .replace(/\p{Diacritic}/gu, "");

  const getCategoryKey = (doc: { source?: string; type?: string }) => {
    const source = String(doc.source || "").toLowerCase();
    const type = String(doc.type || "").toUpperCase();
    if (source === "certidoes") return "certidoes";
    if (source === "dp_guias" || source === "municipal_taxes") return "taxas_impostos";
    if (source === "fiscal") {
      if (type === "NFS") return "nfs";
      if (type === "NFE" || type === "NFC") return "nfe_nfc";
      return "fiscal_outros";
    }
    return "outros";
  };

  const categoryLabel: Record<string, string> = {
    certidoes: "Certidões",
    nfs: "NFS",
    nfe_nfc: "NFE/NFC",
    taxas_impostos: "Taxas e impostos",
    fiscal_outros: "Fiscal (outros)",
    outros: "Outros",
  };

  const getSubTypeLabel = (doc: { type?: string }) => {
    const t = String(doc.type || "").trim();
    const m = t.match(/-\s*(.+)$/);
    if (m && m[1]) return m[1].trim();
    return t;
  };

  const availableCategories = useMemo(() => {
    const keys = new Set<string>();
    for (const d of documents) keys.add(getCategoryKey(d as { source?: string; type?: string }));
    const ordered = ["certidoes", "nfs", "nfe_nfc", "taxas_impostos", "fiscal_outros", "outros"].filter((k) => keys.has(k));
    const remaining = [...keys].filter((k) => !ordered.includes(k)).sort((a, b) => a.localeCompare(b));
    return ["Todos", ...ordered, ...remaining].map((k) => (k === "Todos" ? "Todos" : categoryLabel[k] ?? k));
  }, [documents]);

  const categoryKeyByLabel = useMemo(() => {
    const m = new Map<string, string>();
    for (const [k, v] of Object.entries(categoryLabel)) m.set(v, k);
    return m;
  }, []);

  const selectedCategoryKey = filterCategory === "Todos" ? "Todos" : (categoryKeyByLabel.get(filterCategory) ?? filterCategory);

  const filtered = useMemo(() => documents.filter((doc) => {
    const k = getCategoryKey(doc as { source?: string; type?: string });
    const matchesCategory = selectedCategoryKey === "Todos" || k === selectedCategoryKey;
    const sub = getSubTypeLabel(doc as { type?: string });
    const filePath = String(doc.file_path ?? "");
    const lowerPath = filePath.toLowerCase();
    const fileKind = lowerPath.endsWith(".xml") ? "XML" : lowerPath.endsWith(".pdf") ? "PDF" : "OUTRO";
    const matchesFileKind = filterFileKind === "Todos" || fileKind === filterFileKind;
    const docDate = String(doc.document_date ?? doc.created_at ?? "").slice(0, 10);
    const matchesDateFrom = !dateFrom || (docDate && docDate >= dateFrom);
    const matchesDateTo = !dateTo || (docDate && docDate <= dateTo);
    const q = normalize(search);
    const digitsQuery = String(search).replace(/\D/g, "");
    const origin = String((doc as { origem?: string | null }).origem || "");
    const status = String((doc as { status?: string | null }).status || "");
    const matchesSearch =
      !q ||
      normalize(doc.empresa).includes(q) ||
      normalize(doc.type).includes(q) ||
      normalize(sub).includes(q) ||
      normalize(status).includes(q) ||
      normalize(origin).includes(q) ||
      normalize(doc.periodo).includes(q) ||
      normalize(filePath).includes(q) ||
      (digitsQuery.length > 0 && doc.cnpj && String(doc.cnpj).replace(/\D/g, "").includes(digitsQuery)) ||
      (doc.chave && normalize(doc.chave).includes(q));
    return matchesCategory && matchesFileKind && matchesSearch && matchesDateFrom && matchesDateTo;
  }), [documents, filterCategory, selectedCategoryKey, filterFileKind, search, dateFrom, dateTo]);

  // Quando filtros mudam, volta para a primeira página (evita “parece que não filtrou”).
  useEffect(() => {
    setCurrentPage(1);
  }, [filterCategory, filterFileKind, search, dateFrom, dateTo, pageSize]);

  const pagination = useMemo(() => {
    const total = filtered.length;
    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    const page = Math.min(currentPage, totalPages);
    const fromIndex = (page - 1) * pageSize;
    const toIndex = Math.min(fromIndex + pageSize, total);
    return {
      total,
      totalPages,
      currentPage: page,
      from: total ? fromIndex + 1 : 0,
      to: toIndex,
      list: filtered.slice(fromIndex, toIndex),
    };
  }, [filtered, pageSize, currentPage]);

  const pageDocuments = pagination.list;
  const canDownload = hasServerApi();

  const handleDownload = async (filePath: string | null) => {
    try {
      if (!filePath) throw new Error("Arquivo indisponível.");
      const suggestedName = filePath.split(/[\\/]/).pop() ?? "arquivo.pdf";
      await downloadServerFileByPath(filePath, suggestedName);
      toast.success("Download iniciado.");
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Erro ao baixar.");
    }
  };

  const handleZipDownload = async () => {
    if (!canDownload) {
      toast.error("Configure SERVER_API_URL para habilitar download em ZIP.");
      return;
    }
    const listWithFiles = filtered.filter((d) => d.file_path && String(d.file_path).trim());
    if (listWithFiles.length === 0) {
      toast.error("Nenhum documento com arquivo disponível na lista.");
      return;
    }
    setDownloadingZip(true);
    try {
      // Regra: baixar APENAS o que está listado (respeita XML/PDF e filtros atuais).
      const items = listWithFiles.map((d) => {
        const k = getCategoryKey(d as { source?: string; type?: string });
        const folder =
          k === "certidoes"
            ? "certidoes"
            : k === "nfs"
              ? "nfs"
              : k === "nfe_nfc"
                ? "nfe-nfc"
                : k === "taxas_impostos"
                  ? "taxas e impostos"
                  : "outros";
        return {
          companyName: d.empresa || "EMPRESA",
          category: folder,
          filePath: String(d.file_path || ""),
        };
      });
      await downloadListedFilesZipWithCategory(items, "documentos");
      toast.success(`Download em ZIP iniciado para ${items.length} arquivo(s) (somente os listados).`);
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Erro ao gerar ZIP.");
    } finally {
      setDownloadingZip(false);
    }
  };

  const getDownloadLabel = (filePath: string | null) => {
    if (!filePath) return "—";
    const lower = filePath.toLowerCase();
    if (lower.endsWith(".pdf")) return "PDF";
    if (lower.endsWith(".xml")) return "XML";
    return "Arquivo";
  };

  return (
    <div className="space-y-4 sm:space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-4 min-w-0">
        <div className="min-w-0">
          <h1 className="text-xl sm:text-2xl font-bold font-display tracking-tight text-foreground">Documentos</h1>
          <p className="text-xs sm:text-sm text-muted-foreground mt-1 line-clamp-2">
            {companyFilter ? `Filtrado por ${companyFilter.length} empresa(s)` : "Todas as empresas"} — Lista unificada paginada de documentos fiscais
          </p>
        </div>
      </div>

      <GlassCard className="overflow-hidden">
        <div className="p-3 sm:p-4 border-b border-border flex flex-col gap-3">
          <div className="flex flex-wrap items-center gap-2">
            <Filter className="h-4 w-4 text-muted-foreground shrink-0" aria-hidden />
          </div>
          <div className="flex flex-wrap gap-2">
            <select
              value={filterCategory}
              onChange={(e) => setFilterCategory(e.target.value)}
              className="rounded-xl border border-border bg-background px-3 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-ring focus:border-primary-icon touch-manipulation min-h-[44px]"
              title="Categoria"
            >
              {availableCategories.map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
            <select
              value={filterFileKind}
              onChange={(e) => setFilterFileKind(e.target.value as "Todos" | "XML" | "PDF")}
              className="rounded-xl border border-border bg-background px-3 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-ring focus:border-primary-icon touch-manipulation min-h-[44px]"
              title="Tipo de arquivo"
            >
              <option value="Todos">XML e PDF</option>
              <option value="XML">Somente XML</option>
              <option value="PDF">Somente PDF</option>
            </select>
          </div>
          <div className="flex flex-col sm:flex-row gap-2">
            <div className="relative min-w-0 flex-1">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground pointer-events-none" />
              <input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Buscar empresa ou CNPJ..."
                className="rounded-xl border border-border bg-background pl-10 pr-3 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-ring focus:border-primary-icon w-full min-w-0 touch-manipulation"
              />
            </div>
            <input
              type="date"
              value={dateFrom}
              onChange={(e) => {
                setDateFrom(e.target.value);
                setCurrentPage(1);
              }}
              className="rounded-xl border border-border bg-background px-3 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-ring focus:border-primary-icon touch-manipulation"
              title="Data inicial"
            />
            <input
              type="date"
              value={dateTo}
              onChange={(e) => {
                setDateTo(e.target.value);
                setCurrentPage(1);
              }}
              className="rounded-xl border border-border bg-background px-3 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-ring focus:border-primary-icon touch-manipulation"
              title="Data final"
            />
            <button
              onClick={() => exportToCsv(filtered)}
              className="flex items-center justify-center gap-2 rounded-xl border border-border px-4 py-3 text-sm font-medium hover:bg-muted active:bg-muted/80 transition-colors touch-manipulation min-h-[44px] shrink-0"
            >
              <Download className="h-4 w-4 shrink-0" />
              Exportar CSV
            </button>
            <Button
              type="button"
              onClick={handleZipDownload}
              disabled={downloadingZip || !canDownload || filtered.filter((doc) => doc.file_path).length === 0}
              className="min-h-[44px] rounded-xl px-4 py-3 text-sm"
            >
              <FileArchive className="mr-2 h-4 w-4" />
              {downloadingZip ? "Gerando ZIP…" : "Baixar ZIP dos documentos listados"}
            </Button>
          </div>
        </div>

        {isLoading ? (
          <div className="p-8 text-center text-sm text-muted-foreground">Carregando…</div>
        ) : (
          <div className="overflow-x-auto -webkit-overflow-scrolling-touch">
            <table className="w-full text-xs sm:text-xs min-w-[600px]">
              <thead>
                <tr className="border-b border-border bg-muted/50">
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">Empresa</th>
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">CNPJ</th>
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">Tipo</th>
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">Período</th>
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">Status</th>
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">Origem</th>
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">Data</th>
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">Download</th>
                </tr>
              </thead>
              <tbody>
                {pageDocuments.map((doc) => (
                  <tr key={doc.id} className="border-b border-border hover:bg-muted/30 active:bg-muted/50 transition-colors">
                    <td className="px-3 sm:px-4 py-3 font-medium">{doc.empresa}</td>
                    <td className="px-3 sm:px-4 py-3 text-muted-foreground">{doc.cnpj ?? "—"}</td>
                    <td className="px-3 sm:px-4 py-3">
                      <span className="rounded-md bg-primary/10 px-2 py-1 text-[10px] font-semibold text-primary-icon">{doc.type}</span>
                    </td>
                    <td className="px-3 sm:px-4 py-3">{doc.periodo}</td>
                    <td className="px-3 sm:px-4 py-3">
                      <StatusBadge status={doc.status} />
                    </td>
                    <td className="px-3 sm:px-4 py-3 text-muted-foreground">Automação</td>
                    <td className="px-3 sm:px-4 py-3 text-muted-foreground">{(doc.document_date ?? doc.created_at ?? "").toString().slice(0, 10)}</td>
                    <td className="px-3 sm:px-4 py-3">
                      {doc.file_path ? (
                        <button
                          type="button"
                          onClick={() => handleDownload(doc.file_path)}
                          className="inline-flex items-center gap-1 rounded-md px-2 py-1 text-[10px] font-medium bg-primary/10 text-primary-icon hover:bg-primary/20 transition-colors"
                        >
                          <Download className="h-3 w-3" /> {getDownloadLabel(doc.file_path)}
                        </button>
                      ) : (
                        <span className="text-muted-foreground text-[10px]">—</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {!isLoading && filtered.length === 0 && (
          <div className="p-12 text-center">
            <FileText className="h-10 w-10 text-muted-foreground/30 mx-auto mb-3" />
            <p className="text-sm text-muted-foreground">
              {documents.length === 0
                ? "Nenhum documento encontrado."
                : "Nenhum documento encontrado com os filtros aplicados."}
            </p>
          </div>
        )}

        {!isLoading && filtered.length > 0 && (
          <DataPagination
            currentPage={pagination.currentPage}
            totalPages={pagination.totalPages}
            totalItems={pagination.total}
            from={pagination.from}
            to={pagination.to}
            pageSize={pageSize}
            onPageChange={setCurrentPage}
            onPageSizeChange={(next) => {
              setPageSize(next);
              setCurrentPage(1);
            }}
          />
        )}
      </GlassCard>
    </div>
  );
}
