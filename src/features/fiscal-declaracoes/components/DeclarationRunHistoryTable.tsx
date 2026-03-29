import { Clock3, Download, FileArchive, Trash2 } from "lucide-react";
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
import type { DeclarationRunHistoryEntry } from "../types";

function getRoutineLabel(entry: DeclarationRunHistoryEntry) {
  if (entry.mode === "recalcular") {
    return `${entry.title} (Recalculo)`;
  }
  return entry.title;
}

type DeclarationRunHistoryTableProps = {
  entries: DeclarationRunHistoryEntry[];
  loading?: boolean;
  totalItems: number;
  currentPage: number;
  pageSize: number;
  onPageChange: (page: number) => void;
  onPageSizeChange: (pageSize: number) => void;
  onDownloadArtifact?: (entry: DeclarationRunHistoryEntry) => void;
  onDownloadAllZip?: () => void;
  zipBusy?: boolean;
  onClearAll?: () => void;
};

export function DeclarationRunHistoryTable({
  entries,
  loading = false,
  totalItems,
  currentPage,
  pageSize,
  onPageChange,
  onPageSizeChange,
  onDownloadArtifact,
  onDownloadAllZip,
  zipBusy = false,
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
              Cada linha representa uma empresa da fila, com status e download individual em tempo real.
            </p>
          </div>
          <div className="flex items-center gap-2">
            {loading ? (
              <span className="inline-flex items-center gap-2 text-xs text-muted-foreground">
                <Clock3 className="h-3.5 w-3.5 animate-pulse" />
                Atualizando...
              </span>
            ) : null}
            {entries.length > 0 && onDownloadAllZip ? (
              <Button
                type="button"
                size="sm"
                onClick={onDownloadAllZip}
                disabled={zipBusy}
                className="min-h-[44px] rounded-xl px-4 py-3 text-sm"
              >
                <FileArchive className="mr-2 h-4 w-4" />
                {zipBusy ? "Gerando ZIP..." : "Baixar ZIP da lista"}
              </Button>
            ) : null}
            {entries.length > 0 && onClearAll ? (
              <Button type="button" variant="outline" size="sm" onClick={onClearAll}>
                <Trash2 className="mr-2 h-4 w-4" />
                Limpar historico
              </Button>
            ) : null}
          </div>
        </div>
      </div>

      {entries.length > 0 ? (
        <>
          <Table className="min-w-[980px] text-xs">
            <TableHeader>
              <TableRow>
                <TableHead>Rotina</TableHead>
                <TableHead>Nome da empresa</TableHead>
                <TableHead>Competencia</TableHead>
                <TableHead>Vencimento</TableHead>
                <TableHead>Valor</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Acoes</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {entries.map((entry) => {
                const hasArtifact = entry.status === "sucesso" && Boolean(
                  entry.artifact?.filePath || entry.artifact?.url || entry.artifact?.artifactKey,
                );
                return (
                  <TableRow key={entry.entryId}>
                    <TableCell>
                      <p className="font-medium">{getRoutineLabel(entry)}</p>
                    </TableCell>
                    <TableCell>
                      <div className="space-y-1">
                        <p className="font-medium">{entry.companyName}</p>
                        <p className="text-[11px] text-muted-foreground">
                          {entry.companyDocument || "CNPJ nao informado"}
                        </p>
                      </div>
                    </TableCell>
                    <TableCell>{entry.referenceLabel}</TableCell>
                    <TableCell>{entry.dueDateLabel}</TableCell>
                    <TableCell>{entry.amountLabel}</TableCell>
                    <TableCell>
                      <div className="space-y-1">
                        <StatusBadge status={entry.status} />
                        <p className="max-w-[260px] truncate text-[11px] text-muted-foreground">
                          {entry.message || "-"}
                        </p>
                      </div>
                    </TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end gap-2">
                        <Button
                          type="button"
                          variant="default"
                          size="sm"
                          disabled={!hasArtifact}
                          onClick={() => onDownloadArtifact?.(entry)}
                        >
                          <Download className="mr-2 h-4 w-4" />
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
