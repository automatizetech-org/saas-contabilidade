import { useEffect, useMemo, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { Download, Loader2, RefreshCcw } from "lucide-react";
import { toast } from "sonner";
import { DataPagination } from "@/components/common/DataPagination";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  downloadSimplesGuideDocument,
  listSimplesGuideDocuments,
  scanSimplesGuideDocuments,
} from "@/features/fiscal-declaracoes/service";
import {
  formatCompetenceLabel,
  formatCurrencyFromCents,
  formatDateLabel,
  sanitizeDeclarationError,
} from "@/features/fiscal-declaracoes/helpers";
import type {
  DeclarationCompany,
  DeclarationGuideConsultModalState,
  DeclarationGuideDocumentSortKey,
} from "../types";

type DeclarationGuideDocumentsModalProps = {
  open: boolean;
  state: DeclarationGuideConsultModalState;
  companies: DeclarationCompany[];
  onOpenChange: (open: boolean) => void;
};

const PAGE_SIZE = 10;

function buildYearOptions(currentYear: number) {
  const years: string[] = [];
  for (let year = currentYear + 1; year >= 2018; year -= 1) {
    years.push(String(year));
  }
  return years;
}

export function DeclarationGuideDocumentsModal({
  open,
  state,
  companies,
  onOpenChange,
}: DeclarationGuideDocumentsModalProps) {
  const currentYear = new Date().getFullYear();
  const yearOptions = useMemo(() => buildYearOptions(currentYear), [currentYear]);
  const [selectedCompanyId, setSelectedCompanyId] = useState("");
  const [selectedYear, setSelectedYear] = useState(String(currentYear));
  const [currentPage, setCurrentPage] = useState(1);
  const [sortKey, setSortKey] = useState<DeclarationGuideDocumentSortKey>("competencia");
  const [sortDirection, setSortDirection] = useState<"asc" | "desc">("desc");
  const [busyDocumentId, setBusyDocumentId] = useState<string | null>(null);
  const [reindexing, setReindexing] = useState(false);

  useEffect(() => {
    if (!open) return;
    const presetCompanyId =
      state.presetCompanyId && companies.some((company) => company.id === state.presetCompanyId)
        ? state.presetCompanyId
        : companies[0]?.id ?? "";
    setSelectedCompanyId(presetCompanyId);
    setSelectedYear(
      /^\d{4}$/.test(String(state.presetYear ?? "").trim())
        ? String(state.presetYear).trim()
        : String(currentYear),
    );
    setCurrentPage(1);
    setSortKey("competencia");
    setSortDirection("desc");
  }, [companies, currentYear, open, state]);

  const documentsQuery = useQuery({
    queryKey: [
      "declaration-guide-documents-modal",
      selectedCompanyId,
      selectedYear,
      currentPage,
      sortKey,
      sortDirection,
    ],
    queryFn: () =>
      listSimplesGuideDocuments({
        companyIds: selectedCompanyId ? [selectedCompanyId] : [],
        year: selectedYear,
        page: currentPage,
        pageSize: PAGE_SIZE,
        sortKey,
        sortDirection,
        autoScan: true,
      }),
    enabled: open && Boolean(selectedCompanyId),
    placeholderData: keepPreviousData,
    staleTime: 10_000,
  });

  const selectedCompany = companies.find((company) => company.id === selectedCompanyId) ?? null;
  const totalItems = documentsQuery.data?.total ?? 0;
  const totalPages = Math.max(1, Math.ceil(totalItems / PAGE_SIZE));
  const from = totalItems === 0 ? 0 : (currentPage - 1) * PAGE_SIZE + 1;
  const to = totalItems === 0 ? 0 : Math.min(currentPage * PAGE_SIZE, totalItems);

  const handleDownload = async (documentId: string, fileName: string) => {
    setBusyDocumentId(documentId);
    try {
      await downloadSimplesGuideDocument({ documentId, suggestedName: fileName });
      toast.success("Download iniciado.");
    } catch (error) {
      toast.error(
        sanitizeDeclarationError(error, "Não foi possível baixar a guia selecionada."),
      );
    } finally {
      setBusyDocumentId(null);
    }
  };

  const handleReindex = async () => {
    if (!selectedCompanyId) return;
    setReindexing(true);
    try {
      const summary = await scanSimplesGuideDocuments({
        companyIds: [selectedCompanyId],
        year: selectedYear,
        force: true,
      });
      await documentsQuery.refetch();
      toast.success(
        `Reindexação concluída. ${summary.parsed} PDF(s) relido(s), ${summary.updated + summary.inserted} registro(s) atualizado(s).`,
      );
    } catch (error) {
      toast.error(
        sanitizeDeclarationError(error, "Não foi possível reindexar as guias desta empresa."),
      );
    } finally {
      setReindexing(false);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[90vh] max-w-5xl gap-0 overflow-hidden border-border bg-card p-0">
        <DialogHeader className="space-y-2 border-b border-border px-6 py-5">
          <div className="flex items-start justify-between gap-4">
            <div className="space-y-2">
              <DialogTitle className="text-xl font-display tracking-tight">Consultar guia</DialogTitle>
              <DialogDescription className="text-sm">
                Consulta das guias DAS já catalogadas, com leitura do PDF e fallback pelo indexador mesmo sem o robô ativo.
              </DialogDescription>
            </div>
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={!selectedCompanyId || reindexing}
              onClick={() => void handleReindex()}
            >
              {reindexing ? (
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              ) : (
                <RefreshCcw className="mr-2 h-4 w-4" />
              )}
              Reindexar PDFs
            </Button>
          </div>
        </DialogHeader>

        <div className="space-y-5 overflow-y-auto px-6 py-5">
          <div className="grid gap-4 md:grid-cols-3">
            <div className="space-y-2 md:col-span-2">
              <Label>Empresa</Label>
              <Select
                value={selectedCompanyId}
                onValueChange={(value) => {
                  setSelectedCompanyId(value);
                  setCurrentPage(1);
                }}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Selecione uma empresa" />
                </SelectTrigger>
                <SelectContent>
                  {companies.map((company) => (
                    <SelectItem key={company.id} value={company.id}>
                      {company.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label>Ano de referência</Label>
              <Select
                value={selectedYear}
                onValueChange={(value) => {
                  setSelectedYear(value);
                  setCurrentPage(1);
                }}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Selecione o ano" />
                </SelectTrigger>
                <SelectContent>
                  {yearOptions.map((year) => (
                    <SelectItem key={year} value={year}>
                      {year}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          <div className="grid gap-4 md:grid-cols-3">
            <div className="rounded-2xl border border-border bg-background/60 px-4 py-3 text-sm md:col-span-2">
              <div className="font-medium">{selectedCompany?.name ?? "Nenhuma empresa selecionada"}</div>
              <div className="mt-1 text-xs text-muted-foreground">
                {selectedCompany?.document || "Documento não informado"}
              </div>
            </div>

            <div className="grid gap-4 sm:grid-cols-2 md:grid-cols-2">
              <div className="space-y-2">
                <Label>Ordenar por</Label>
                <Select
                  value={sortKey}
                  onValueChange={(value) => {
                    setSortKey(value as DeclarationGuideDocumentSortKey);
                    setCurrentPage(1);
                  }}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="competencia">Competência</SelectItem>
                    <SelectItem value="vencimento">Vencimento</SelectItem>
                    <SelectItem value="valor">Valor</SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label>Direção</Label>
                <Select
                  value={sortDirection}
                  onValueChange={(value) => {
                    setSortDirection(value as "asc" | "desc");
                    setCurrentPage(1);
                  }}
                >
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="desc">Mais recente</SelectItem>
                    <SelectItem value="asc">Mais antigo</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          </div>

          {documentsQuery.isLoading || documentsQuery.isFetching ? (
            <div className="flex items-center gap-2 rounded-2xl border border-dashed border-border bg-background/50 px-4 py-5 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              Carregando guias catalogadas...
            </div>
          ) : (
            <div className="overflow-hidden rounded-2xl border border-border bg-background/60">
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Competência</TableHead>
                    <TableHead>Vencimento</TableHead>
                    <TableHead>Valor</TableHead>
                    <TableHead className="text-right">PDF</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {(documentsQuery.data?.items ?? []).length > 0 ? (
                    (documentsQuery.data?.items ?? []).map((item) => (
                      <TableRow key={item.documentId}>
                        <TableCell className="font-medium">
                          {formatCompetenceLabel(item.competence)}
                        </TableCell>
                        <TableCell>{formatDateLabel(item.dueDate)}</TableCell>
                        <TableCell>{formatCurrencyFromCents(item.amountCents)}</TableCell>
                        <TableCell className="text-right">
                          <Button
                            type="button"
                            size="sm"
                            disabled={busyDocumentId === item.documentId}
                            onClick={() => void handleDownload(item.documentId, item.fileName)}
                          >
                            {busyDocumentId === item.documentId ? (
                              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                            ) : (
                              <Download className="mr-2 h-4 w-4" />
                            )}
                            PDF
                          </Button>
                        </TableCell>
                      </TableRow>
                    ))
                  ) : (
                    <TableRow>
                      <TableCell colSpan={4} className="py-8 text-center text-sm text-muted-foreground">
                        Nenhuma guia catalogada para a empresa e o ano selecionados.
                      </TableCell>
                    </TableRow>
                  )}
                </TableBody>
              </Table>
            </div>
          )}

          <DataPagination
            currentPage={currentPage}
            totalPages={totalPages}
            totalItems={totalItems}
            from={from}
            to={to}
            pageSize={PAGE_SIZE}
            pageSizeOptions={[10]}
            onPageChange={setCurrentPage}
            onPageSizeChange={() => undefined}
          />
        </div>
      </DialogContent>
    </Dialog>
  );
}
