import { CheckCircle2, Clock3, FileText, Trash2, XCircle } from "lucide-react";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { DataPagination } from "@/components/common/DataPagination";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { DeclarationRunState } from "../types";
import { asObject, formatCompetenceLabel } from "../helpers";

function formatDateTime(value: string | null | undefined) {
  const raw = String(value ?? "").trim();
  if (!raw) return "-";
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return "-";
  return date.toLocaleString("pt-BR");
}

function summarizeRun(run: DeclarationRunState) {
  const successCount = run.items.filter((item) => item.status === "sucesso").length;
  const errorCount = run.items.filter((item) => item.status === "erro").length;
  const processingCount = run.items.filter((item) => item.status === "processando").length;
  return {
    total: run.items.length,
    successCount,
    errorCount,
    processingCount,
    status: !run.terminal ? "processando" : errorCount > 0 ? "divergente" : "sucesso",
  };
}

function getRunReferenceLabel(run: DeclarationRunState) {
  const firstMeta = asObject(run.items[0]?.meta ?? null);
  const rawReference = String(firstMeta.competencia ?? firstMeta.competence ?? "").trim();
  if (!rawReference) return "-";
  if (run.action === "simples_defis") {
    const yearMatch = rawReference.match(/^(\d{4})/);
    return yearMatch?.[1] ?? "-";
  }
  return formatCompetenceLabel(rawReference);
}

type DeclarationRunHistoryTableProps = {
  runs: DeclarationRunState[];
  loading?: boolean;
  totalItems: number;
  currentPage: number;
  pageSize: number;
  onPageChange: (page: number) => void;
  onPageSizeChange: (pageSize: number) => void;
  onDownloadPrimaryArtifact?: (run: DeclarationRunState) => void;
  onClearAll?: () => void;
};

export function DeclarationRunHistoryTable({
  runs,
  loading = false,
  totalItems,
  currentPage,
  pageSize,
  onPageChange,
  onPageSizeChange,
  onDownloadPrimaryArtifact,
  onClearAll,
}: DeclarationRunHistoryTableProps) {
  const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
  const from = totalItems === 0 ? 0 : (currentPage - 1) * pageSize + 1;
  const to = totalItems === 0 ? 0 : Math.min(currentPage * pageSize, totalItems);

  return (
    <GlassCard className="border border-border/70 p-0">
      <div className="border-b border-border/70 px-6 py-5">
        <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <h3 className="text-lg font-semibold font-display tracking-tight">Solicitacoes do processamento</h3>
            <p className="text-sm text-muted-foreground">
              Historico compartilhado do escritorio com paginacao padrao do SaaS.
            </p>
          </div>
          <div className="flex items-center gap-2">
            {loading ? (
              <span className="inline-flex items-center gap-2 text-xs text-muted-foreground">
                <Clock3 className="h-3.5 w-3.5 animate-pulse" />
                Atualizando...
              </span>
            ) : null}
            {runs.length > 0 && onClearAll ? (
              <Button type="button" variant="outline" size="sm" onClick={onClearAll}>
                <Trash2 className="mr-2 h-4 w-4" />
                Limpar historico
              </Button>
            ) : null}
          </div>
        </div>
      </div>

      {runs.length > 0 ? (
        <>
          <Table className="min-w-[980px] text-xs">
            <TableHeader>
              <TableRow>
                <TableHead>Rotina</TableHead>
                <TableHead>Competencia</TableHead>
                <TableHead>Iniciado em</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-center">Empresas</TableHead>
                <TableHead className="text-center">Sucesso</TableHead>
                <TableHead className="text-center">Erro</TableHead>
                <TableHead className="text-center">Em andamento</TableHead>
                <TableHead className="text-right">Acoes</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {runs.map((run) => {
                const summary = summarizeRun(run);
                const hasArtifact = run.items.some(
                  (item) => item.artifact?.filePath || item.artifact?.url || item.artifact?.artifactKey,
                );
                return (
                  <TableRow key={run.runId}>
                    <TableCell>
                      <p className="font-medium">{run.title}</p>
                    </TableCell>
                    <TableCell>{getRunReferenceLabel(run)}</TableCell>
                    <TableCell>{formatDateTime(run.startedAt)}</TableCell>
                    <TableCell>
                      <StatusBadge status={summary.status} />
                    </TableCell>
                    <TableCell className="text-center">{summary.total}</TableCell>
                    <TableCell className="text-center">
                      <span className="inline-flex items-center gap-1 text-success">
                        <CheckCircle2 className="h-3.5 w-3.5" />
                        {summary.successCount}
                      </span>
                    </TableCell>
                    <TableCell className="text-center">
                      <span className="inline-flex items-center gap-1 text-destructive">
                        <XCircle className="h-3.5 w-3.5" />
                        {summary.errorCount}
                      </span>
                    </TableCell>
                    <TableCell className="text-center">
                      <span className="inline-flex items-center gap-1 text-info">
                        <Clock3 className="h-3.5 w-3.5" />
                        {summary.processingCount}
                      </span>
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end gap-2">
                        <Button
                          type="button"
                          variant="default"
                          size="sm"
                          disabled={!hasArtifact}
                          onClick={() => onDownloadPrimaryArtifact?.(run)}
                        >
                          <FileText className="mr-2 h-4 w-4" />
                          PDF
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
          <DataPagination
            currentPage={currentPage}
            totalPages={totalPages}
            totalItems={totalItems}
            from={from}
            to={to}
            pageSize={pageSize}
            onPageChange={onPageChange}
            onPageSizeChange={onPageSizeChange}
          />
        </>
      ) : (
        <div className="px-6 py-10 text-center text-sm text-muted-foreground">
          Nenhuma solicitacao persistida para este escritorio ate o momento.
        </div>
      )}
    </GlassCard>
  );
}
