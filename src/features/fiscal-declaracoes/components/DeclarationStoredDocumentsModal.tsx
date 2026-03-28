import { useEffect, useMemo, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { CalendarDays, Download, FileText, Loader2 } from "lucide-react";
import { toast } from "sonner";
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
import { sanitizeDeclarationError } from "@/features/fiscal-declaracoes/helpers";
import { downloadDeclarationArtifact, listDeclarationArtifacts } from "@/features/fiscal-declaracoes/service";
import type {
  DeclarationArtifactListItem,
  DeclarationCompany,
  DeclarationStoredDocumentsModalState,
} from "../types";

type DeclarationStoredDocumentsModalProps = {
  open: boolean;
  state: DeclarationStoredDocumentsModalState;
  companies: DeclarationCompany[];
  onOpenChange: (open: boolean) => void;
};

function extractExtratoYearMonth(fileName: string) {
  const match = String(fileName ?? "").match(/(\d{4})-(\d{2})/);
  if (!match) return null;
  return { year: match[1], month: match[2] };
}

function extractDefisYear(fileName: string) {
  const match = String(fileName ?? "").match(/\b(20\d{2})\b/);
  return match ? match[1] : null;
}

function isDefisReceipt(fileName: string) {
  return String(fileName ?? "").toLowerCase().includes("recibo");
}

function isDefisDeclaration(fileName: string) {
  return String(fileName ?? "").toLowerCase().includes("declaracao");
}

function buildYearOptions(
  presetYear: string | null | undefined,
  extraYears: string[],
  currentYear: number,
) {
  const minYear = 2018;
  const highestYear = Math.max(
    currentYear,
    ...extraYears.map((year) => Number(year) || 0),
    Number(presetYear) || 0,
  );
  const values: string[] = [];
  for (let year = highestYear; year >= minYear; year -= 1) {
    values.push(String(year));
  }
  return values;
}

export function DeclarationStoredDocumentsModal({
  open,
  state,
  companies,
  onOpenChange,
}: DeclarationStoredDocumentsModalProps) {
  const currentYear = new Date().getFullYear();
  const [selectedCompanyId, setSelectedCompanyId] = useState("");
  const [selectedYear, setSelectedYear] = useState(String(currentYear));
  const [busyArtifactKey, setBusyArtifactKey] = useState<string | null>(null);

  useEffect(() => {
    if (!open) return;
    const firstCompanyId = companies[0]?.id ?? "";
    const presetCompanyId =
      state.presetCompanyId && companies.some((company) => company.id === state.presetCompanyId)
        ? state.presetCompanyId
        : firstCompanyId;
    setSelectedCompanyId(presetCompanyId);
    setSelectedYear(state.presetYear || String(currentYear));
  }, [companies, currentYear, open, state]);

  const documentsQuery = useQuery({
    queryKey: ["declaration-stored-documents-modal", state.action, selectedCompanyId],
    queryFn: () =>
      listDeclarationArtifacts({
        action: state.action,
        companyIds: selectedCompanyId ? [selectedCompanyId] : [],
        competence: null,
        limit: 500,
      }),
    enabled: open && Boolean(selectedCompanyId),
    placeholderData: keepPreviousData,
    staleTime: 10_000,
  });

  const selectedCompany =
    companies.find((company) => company.id === selectedCompanyId) ?? null;
  const companyItems = useMemo(
    () =>
      (documentsQuery.data?.items ?? []).filter((item) => item.company_id === selectedCompanyId),
    [documentsQuery.data?.items, selectedCompanyId],
  );

  const extratoYearOptions = useMemo(() => {
    const years = companyItems
      .map((item) => extractExtratoYearMonth(item.file_name)?.year ?? "")
      .filter(Boolean);
    return buildYearOptions(state.presetYear, years, currentYear);
  }, [companyItems, currentYear, state.presetYear]);

  const extratoRows = useMemo(() => {
    const artifactsByMonth = new Map<string, DeclarationArtifactListItem>();
    for (const item of companyItems) {
      const parsed = extractExtratoYearMonth(item.file_name);
      if (!parsed || parsed.year !== selectedYear) continue;
      artifactsByMonth.set(parsed.month, item);
    }

    return Array.from({ length: 12 }, (_, index) => {
      const month = String(index + 1).padStart(2, "0");
      return {
        month,
        label: `${month}/${selectedYear}`,
        artifact: artifactsByMonth.get(month) ?? null,
      };
    });
  }, [companyItems, selectedYear]);

  const defisRows = useMemo(() => {
    const years = new Map<
      string,
      { year: string; receipt: DeclarationArtifactListItem | null; declaration: DeclarationArtifactListItem | null }
    >();

    for (const item of companyItems) {
      const year = extractDefisYear(item.file_name);
      if (!year) continue;
      const current =
        years.get(year) ?? { year, receipt: null, declaration: null };
      if (isDefisReceipt(item.file_name)) current.receipt = item;
      if (isDefisDeclaration(item.file_name)) current.declaration = item;
      years.set(year, current);
    }

    return Array.from(years.values()).sort((left, right) => left.year.localeCompare(right.year));
  }, [companyItems]);

  const modalTitle =
    state.action === "simples_extrato" ? "Extrato do Simples Nacional" : "DEFIS";
  const modalDescription =
    state.action === "simples_extrato"
      ? "Selecione a empresa e o ano para visualizar os PDFs ja salvos no servidor."
      : "Selecione a empresa para visualizar os recibos e declaracoes anuais ja salvos no servidor.";

  const handleDownload = async (item: DeclarationArtifactListItem) => {
    setBusyArtifactKey(item.artifact_key);
    try {
      await downloadDeclarationArtifact({
        action: state.action,
        companyId: item.company_id,
        artifactKey: item.artifact_key,
        suggestedName: item.file_name,
      });
      toast.success("Download iniciado.");
    } catch (error) {
      toast.error(
        sanitizeDeclarationError(error, "Não foi possível baixar o PDF selecionado."),
      );
    } finally {
      setBusyArtifactKey(null);
    }
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[90vh] max-w-5xl gap-0 overflow-hidden border-border bg-card p-0">
        <DialogHeader className="space-y-2 border-b border-border px-6 py-5">
          <DialogTitle className="text-xl font-display tracking-tight">{modalTitle}</DialogTitle>
          <DialogDescription className="text-sm">{modalDescription}</DialogDescription>
        </DialogHeader>

        <div className="space-y-5 overflow-y-auto px-6 py-5">
          <div className="grid gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <Label>Empresa</Label>
              <Select value={selectedCompanyId} onValueChange={setSelectedCompanyId}>
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

            {state.action === "simples_extrato" ? (
              <div className="space-y-2">
                <Label>Ano-calendario</Label>
                <Select value={selectedYear} onValueChange={setSelectedYear}>
                  <SelectTrigger>
                    <SelectValue placeholder="Selecione o ano" />
                  </SelectTrigger>
                  <SelectContent>
                    {extratoYearOptions.map((year) => (
                      <SelectItem key={year} value={year}>
                        {year}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            ) : null}
          </div>

          {selectedCompany ? (
            <div className="rounded-2xl border border-border bg-background/60 px-4 py-3 text-sm">
              <div className="font-medium">{selectedCompany.name}</div>
              <div className="mt-1 text-xs text-muted-foreground">
                {selectedCompany.document || "Documento nao informado"}
              </div>
            </div>
          ) : null}

          {documentsQuery.isLoading || documentsQuery.isFetching ? (
            <div className="flex items-center gap-2 rounded-2xl border border-dashed border-border bg-background/50 px-4 py-5 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              Carregando documentos salvos...
            </div>
          ) : (
            <>
              {state.action === "simples_extrato" ? (
                <div className="max-h-[52vh] overflow-auto rounded-2xl border border-border bg-background/60">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>PA</TableHead>
                        <TableHead>Status</TableHead>
                        <TableHead className="text-right">PDF</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {extratoRows.map((row) => (
                        <TableRow key={row.month} className={!row.artifact ? "opacity-55" : undefined}>
                          <TableCell className="font-medium">{row.label}</TableCell>
                          <TableCell>
                            {row.artifact ? "Disponivel" : "Sem extrato salvo"}
                          </TableCell>
                          <TableCell className="text-right">
                            <Button
                              type="button"
                              size="sm"
                              variant={row.artifact ? "default" : "outline"}
                              disabled={!row.artifact || busyArtifactKey === row.artifact.artifact_key}
                              onClick={() => row.artifact && void handleDownload(row.artifact)}
                            >
                              {row.artifact && busyArtifactKey === row.artifact.artifact_key ? (
                                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                              ) : (
                                <Download className="mr-2 h-4 w-4" />
                              )}
                              PDF
                            </Button>
                          </TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              ) : (
                <div className="max-h-[52vh] overflow-auto rounded-2xl border border-border bg-background/60">
                  <Table>
                    <TableHeader>
                      <TableRow>
                        <TableHead>Ano-calendario</TableHead>
                        <TableHead className="text-right">Recibo</TableHead>
                        <TableHead className="text-right">Declaracao</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {defisRows.length > 0 ? (
                        defisRows.map((row) => (
                          <TableRow key={row.year}>
                            <TableCell className="font-medium">{row.year}</TableCell>
                            <TableCell className="text-right">
                              <Button
                                type="button"
                                size="sm"
                                variant={row.receipt ? "default" : "outline"}
                                disabled={!row.receipt || busyArtifactKey === row.receipt.artifact_key}
                                onClick={() => row.receipt && void handleDownload(row.receipt)}
                              >
                                {row.receipt && busyArtifactKey === row.receipt.artifact_key ? (
                                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                                ) : (
                                  <FileText className="mr-2 h-4 w-4" />
                                )}
                                PDF
                              </Button>
                            </TableCell>
                            <TableCell className="text-right">
                              <Button
                                type="button"
                                size="sm"
                                variant={row.declaration ? "default" : "outline"}
                                disabled={!row.declaration || busyArtifactKey === row.declaration.artifact_key}
                                onClick={() => row.declaration && void handleDownload(row.declaration)}
                              >
                                {row.declaration && busyArtifactKey === row.declaration.artifact_key ? (
                                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                                ) : (
                                  <FileText className="mr-2 h-4 w-4" />
                                )}
                                PDF
                              </Button>
                            </TableCell>
                          </TableRow>
                        ))
                      ) : (
                        <TableRow>
                          <TableCell colSpan={3} className="py-8 text-center text-sm text-muted-foreground">
                            Nenhum documento de DEFIS localizado para a empresa selecionada.
                          </TableCell>
                        </TableRow>
                      )}
                    </TableBody>
                  </Table>
                </div>
              )}

              {!documentsQuery.isLoading && !documentsQuery.isFetching && state.action === "simples_extrato" ? (
                <div className="flex items-center gap-2 text-xs text-muted-foreground">
                  <CalendarDays className="h-3.5 w-3.5" />
                  Os meses sem PDF salvo ficam desabilitados.
                </div>
              ) : null}
            </>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
