import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { Progress } from "@/components/ui/progress";
import { FileText, Download, Filter, Search, FileArchive } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { useQuery, keepPreviousData, useQueryClient } from "@tanstack/react-query";
import { useSelectedCompanyIds } from "@/hooks/useSelectedCompanies";
import { getUnifiedDocumentsPage, getUnifiedDocumentsZipPaths, type CursorPageToken } from "@/services/documentsService";
import { downloadListedFilesZipWithCategory, downloadServerFileByPath, hasServerApi } from "@/services/serverFileService";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import { CursorPagination } from "@/components/common/CursorPagination";
import { getVisibilityAwareRefetchInterval } from "@/lib/queryPolling";

type ExportableDocument = {
  empresa: string;
  cnpj: string | null;
  type: string;
  periodo: string | null;
  status: string | null;
  document_date: string | null;
  created_at: string;
};

function exportToCsv(data: ExportableDocument[]) {
  const headers = ["Empresa", "CNPJ", "Tipo", "Periodo", "Status", "Data"];
  const rows = data.map((item) =>
    [
      item.empresa,
      item.cnpj ?? "",
      item.type,
      item.periodo ?? "",
      item.status ?? "",
      String(item.document_date ?? item.created_at ?? "").slice(0, 10),
    ]
      .map((cell) => `"${String(cell).replace(/"/g, '""')}"`)
      .join(",")
  );
  const csv = [headers.join(","), ...rows].join("\n");
  const blob = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `documentos-pagina-${new Date().toISOString().slice(0, 10)}.csv`;
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 30_000);
}

function getCategoryKey(doc: { source?: string; type?: string }) {
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
}

const CATEGORY_LABEL: Record<string, string> = {
  certidoes: "Certidoes",
  nfs: "NFS",
  nfe_nfc: "NFE/NFC",
  taxas_impostos: "Taxas e impostos",
  fiscal_outros: "Fiscal (outros)",
  outros: "Outros",
};

const CATEGORY_OPTIONS = ["Todos", "certidoes", "nfs", "nfe_nfc", "taxas_impostos", "fiscal_outros", "outros"].map((key) =>
  key === "Todos" ? key : (CATEGORY_LABEL[key] ?? key)
);

export default function DocumentosPage() {
  const [filterFileKind, setFilterFileKind] = useState<"Todos" | "XML" | "PDF">("Todos");
  const [filterCategory, setFilterCategory] = useState<string>("Todos");
  const [search, setSearch] = useState("");
  const [dateFrom, setDateFrom] = useState("");
  const [dateTo, setDateTo] = useState("");
  const [pageSize, setPageSize] = useState(10);
  const [currentPage, setCurrentPage] = useState(1);
  const [cursorHistory, setCursorHistory] = useState<Array<CursorPageToken | null>>([null]);
  const [downloadingZip, setDownloadingZip] = useState(false);
  const [downloadProgress, setDownloadProgress] = useState(0);
  const queryClient = useQueryClient();
  const { selectedCompanyIds } = useSelectedCompanyIds();
  const companyFilter = selectedCompanyIds.length > 0 ? selectedCompanyIds : null;

  const categoryKeyByLabel = useMemo(() => {
    const entries = Object.entries(CATEGORY_LABEL).map(([key, label]) => [label, key] as const);
    return new Map(entries);
  }, []);

  const selectedCategoryKey = filterCategory === "Todos" ? "Todos" : (categoryKeyByLabel.get(filterCategory) ?? filterCategory);
  const canDownload = hasServerApi();

  useEffect(() => {
    setCurrentPage(1);
    setCursorHistory([null]);
  }, [filterCategory, filterFileKind, search, dateFrom, dateTo, pageSize]);

  const currentCursor = cursorHistory[currentPage - 1] ?? null;

  const { data: documentsPage, isLoading } = useQuery({
    queryKey: ["hub-documents-page", companyFilter, selectedCategoryKey, filterFileKind, search, dateFrom, dateTo, pageSize, currentPage, currentCursor?.id ?? null, currentCursor?.createdAt ?? null, currentCursor?.sortDate ?? null],
    queryFn: () =>
      getUnifiedDocumentsPage({
        companyIds: companyFilter,
        category: selectedCategoryKey,
        fileKind: filterFileKind,
        search,
        dateFrom,
        dateTo,
        cursor: currentCursor,
        limit: pageSize,
      }),
    placeholderData: keepPreviousData,
    staleTime: 25_000,
    refetchInterval: () => getVisibilityAwareRefetchInterval(),
    refetchIntervalInBackground: true,
  });

  useEffect(() => {
    if (!documentsPage?.nextCursor || !documentsPage?.hasMore) return;
    void queryClient.prefetchQuery({
      queryKey: [
        "hub-documents-page",
        companyFilter,
        selectedCategoryKey,
        filterFileKind,
        search,
        dateFrom,
        dateTo,
        pageSize,
        currentPage + 1,
        documentsPage.nextCursor.id ?? null,
        documentsPage.nextCursor.createdAt ?? null,
        documentsPage.nextCursor.sortDate ?? null,
      ],
      queryFn: () =>
        getUnifiedDocumentsPage({
          companyIds: companyFilter,
          category: selectedCategoryKey,
          fileKind: filterFileKind,
          search,
          dateFrom,
          dateTo,
          cursor: documentsPage.nextCursor,
          limit: pageSize,
        }),
      staleTime: 20_000,
    });
  }, [
    documentsPage?.nextCursor,
    documentsPage?.hasMore,
    queryClient,
    companyFilter,
    selectedCategoryKey,
    filterFileKind,
    search,
    dateFrom,
    dateTo,
    pageSize,
    currentPage,
  ]);

  const pageDocuments = documentsPage?.items ?? [];
  const hasMore = documentsPage?.hasMore ?? false;

  const handleDownload = async (filePath: string | null) => {
    try {
      if (!filePath) throw new Error("Arquivo indisponivel.");
      const suggestedName = filePath.split(/[\\/]/).pop() ?? "arquivo";
      await downloadServerFileByPath(filePath, suggestedName);
      toast.success("Download iniciado.");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Erro ao baixar.");
    }
  };

  const handleZipDownload = async () => {
    if (!canDownload) {
      toast.error("Nenhum servidor do escritorio esta disponivel para gerar o ZIP.");
      return;
    }

    setDownloadingZip(true);
    setDownloadProgress(0);
    try {
      // Busca todos os documentos que batem com os filtros atuais (não só a página), para o ZIP incluir a lista inteira.
      const zipPaths = await getUnifiedDocumentsZipPaths(
        {
          companyIds: companyFilter ?? undefined,
          category: selectedCategoryKey,
          fileKind: filterFileKind,
          search: search || undefined,
          dateFrom: dateFrom || undefined,
          dateTo: dateTo || undefined,
        },
        50_000
      );

      if (zipPaths.length === 0) {
        toast.error("Nenhum documento com arquivo disponivel para os filtros atuais.");
        return;
      }

      // Deduplicar por file_path para não enviar o mesmo arquivo mais de uma vez.
      const uniqueByPath = new Map<string, (typeof zipPaths)[number]>();
      for (const row of zipPaths) {
        if (!row.file_path || uniqueByPath.has(row.file_path)) continue;
        uniqueByPath.set(row.file_path, row);
      }

      const categoryToFolder = (key: string) =>
        key === "certidoes" ? "certidoes"
          : key === "nfs" ? "nfs"
          : key === "nfe_nfc" ? "nfe-nfc"
          : key === "taxas_impostos" ? "taxas e impostos"
          : "outros";

      const items = Array.from(uniqueByPath.values()).map((row) => ({
        companyName: row.empresa || "EMPRESA",
        category: categoryToFolder(row.category_key),
        filePath: row.file_path,
      }));

      await downloadListedFilesZipWithCategory(items, "documentos", (percent) => setDownloadProgress(percent));
      toast.success(`Download em ZIP iniciado: ${items.length} arquivo(s) da lista.`);
    } catch (error) {
      const msg = error instanceof Error ? error.message : "Erro ao gerar ZIP.";
      if (msg.includes("Rota não encontrada") || msg.includes("404")) {
        toast.error(
          "Servidor não reconheceu a rota. Reinicie o Servidor (stop.bat e start.bat) e confira na aba Rede (Filtro: Tudo) a requisição para office-server."
        );
      } else {
        toast.error(msg);
      }
    } finally {
      setDownloadingZip(false);
      setDownloadProgress(0);
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
            {companyFilter ? `Filtrado por ${companyFilter.length} empresa(s)` : "Todas as empresas"} - lista unificada de todo o dashboard (NFS, NFE/NFC, certidoes, taxas/impostos, etc.) com paginação server-side.
          </p>
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
        <div className="p-3 sm:p-4 border-b border-border flex flex-col gap-3">
          <div className="flex flex-wrap items-center gap-2">
            <Filter className="h-4 w-4 text-muted-foreground shrink-0" aria-hidden />
          </div>
          <div className="flex flex-wrap gap-2">
            <select
              value={filterCategory}
              onChange={(event) => setFilterCategory(event.target.value)}
              className="rounded-xl border border-border bg-background px-3 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-ring focus:border-primary-icon touch-manipulation min-h-[44px]"
              title="Categoria"
            >
              {CATEGORY_OPTIONS.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
            <select
              value={filterFileKind}
              onChange={(event) => setFilterFileKind(event.target.value as "Todos" | "XML" | "PDF")}
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
                onChange={(event) => setSearch(event.target.value)}
                placeholder="Buscar empresa, CNPJ ou chave..."
                className="rounded-xl border border-border bg-background pl-10 pr-3 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-ring focus:border-primary-icon w-full min-w-0 touch-manipulation"
              />
            </div>
            <input
              type="date"
              value={dateFrom}
              onChange={(event) => setDateFrom(event.target.value)}
              className="rounded-xl border border-border bg-background px-3 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-ring focus:border-primary-icon touch-manipulation"
              title="Data inicial"
            />
            <input
              type="date"
              value={dateTo}
              onChange={(event) => setDateTo(event.target.value)}
              className="rounded-xl border border-border bg-background px-3 py-3 text-sm focus:outline-none focus:ring-2 focus:ring-ring focus:border-primary-icon touch-manipulation"
              title="Data final"
            />
            <button
              onClick={() => exportToCsv(pageDocuments)}
              className="flex items-center justify-center gap-2 rounded-xl border border-border px-4 py-3 text-sm font-medium hover:bg-muted active:bg-muted/80 transition-colors touch-manipulation min-h-[44px] shrink-0"
            >
              <Download className="h-4 w-4 shrink-0" />
              Exportar CSV da pagina
            </button>
            <Button
              type="button"
              onClick={handleZipDownload}
              disabled={downloadingZip || !canDownload || pageDocuments.filter((item) => item.file_path && String(item.file_path).trim()).length === 0}
              className="min-h-[44px] rounded-xl px-4 py-3 text-sm"
            >
              <FileArchive className="mr-2 h-4 w-4" />
              {downloadingZip ? "Gerando ZIP..." : "Baixar ZIP da lista"}
            </Button>
          </div>
        </div>

        {isLoading && pageDocuments.length === 0 ? (
          <div className="p-8 text-center text-sm text-muted-foreground">Carregando...</div>
        ) : (
          <div className="overflow-x-auto -webkit-overflow-scrolling-touch">
            <table className="w-full text-xs sm:text-xs min-w-[600px]">
              <thead>
                <tr className="border-b border-border bg-muted/50">
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">Empresa</th>
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">CNPJ</th>
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">Tipo</th>
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">Periodo</th>
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">Status</th>
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">Origem</th>
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">Data</th>
                  <th className="text-left px-3 sm:px-4 py-3 font-medium text-muted-foreground">Download</th>
                </tr>
              </thead>
              <tbody>
                {pageDocuments.map((doc) => (
                  <tr key={`${doc.id}-${doc.file_path ?? ""}`} className="border-b border-border hover:bg-muted/30 active:bg-muted/50 transition-colors">
                    <td className="px-3 sm:px-4 py-3 font-medium">{doc.empresa}</td>
                    <td className="px-3 sm:px-4 py-3 text-muted-foreground">{doc.cnpj ?? "—"}</td>
                    <td className="px-3 sm:px-4 py-3">
                      <span className="rounded-md bg-primary/10 px-2 py-1 text-[10px] font-semibold text-primary-icon">{doc.type}</span>
                    </td>
                    <td className="px-3 sm:px-4 py-3">{doc.periodo ?? "—"}</td>
                    <td className="px-3 sm:px-4 py-3">
                      <StatusBadge status={doc.status} />
                    </td>
                    <td className="px-3 sm:px-4 py-3 text-muted-foreground">{doc.origem ?? "Automacao"}</td>
                    <td className="px-3 sm:px-4 py-3 text-muted-foreground">{String(doc.document_date ?? doc.created_at ?? "").slice(0, 10)}</td>
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

        {!isLoading && pageDocuments.length === 0 && (
          <div className="p-12 text-center">
            <FileText className="h-10 w-10 text-muted-foreground/30 mx-auto mb-3" />
            <p className="text-sm text-muted-foreground">Nenhum documento encontrado com os filtros aplicados.</p>
          </div>
        )}

        {pageDocuments.length > 0 && (
          <CursorPagination
            currentPage={currentPage}
            pageSize={pageSize}
            shownItems={pageDocuments.length}
            hasMore={hasMore}
            onFirst={() => {
              setCurrentPage(1);
              setCursorHistory([null]);
            }}
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
