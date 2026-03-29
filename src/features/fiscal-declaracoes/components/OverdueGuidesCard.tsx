import { useEffect, useMemo, useState } from "react";
import { AlertTriangle, FileClock, RefreshCcw } from "lucide-react";
import { DataPagination } from "@/components/common/DataPagination";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { OverdueGuide } from "../types";
import { formatCompetenceLabel, formatCurrencyFromCents, formatDateLabel } from "../helpers";

type OverdueGuidesCardProps = {
  guides: OverdueGuide[];
  busy?: boolean;
  onRecalculate: (guide: OverdueGuide) => void;
};

export function OverdueGuidesCard({
  guides,
  busy = false,
  onRecalculate,
}: OverdueGuidesCardProps) {
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);

  useEffect(() => {
    setCurrentPage(1);
  }, [guides.length, pageSize]);

  const pagination = useMemo(() => {
    const total = guides.length;
    const totalPages = Math.max(1, Math.ceil(total / pageSize));
    const safePage = Math.min(currentPage, totalPages);
    const fromIndex = (safePage - 1) * pageSize;
    const toIndex = Math.min(fromIndex + pageSize, total);
    return {
      total,
      totalPages,
      currentPage: safePage,
      from: total === 0 ? 0 : fromIndex + 1,
      to: total === 0 ? 0 : toIndex,
      items: guides.slice(fromIndex, toIndex),
    };
  }, [currentPage, guides, pageSize]);

  return (
    <GlassCard className="border border-border/70 p-0">
      <div className="space-y-4 border-b border-border/70 p-6">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-destructive/10 text-destructive">
                <FileClock className="h-5 w-5" />
              </div>
              <div>
                <h3 className="text-lg font-semibold font-display tracking-tight">Guias vencidas</h3>
                <p className="text-sm text-muted-foreground">
                  Lista completa dos débitos vencidos retornados pelo robô de consulta de débitos do Simples Nacional.
                </p>
              </div>
            </div>
          </div>
          <div className="rounded-2xl border border-border bg-background/60 px-4 py-3 text-right">
            <p className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Total</p>
            <p className="mt-1 text-2xl font-semibold font-display">{guides.length}</p>
          </div>
        </div>

        <div className="rounded-2xl border border-amber-500/20 bg-amber-500/5 p-4 text-sm text-amber-100">
          <p className="flex items-start gap-2 text-amber-700 dark:text-amber-300">
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
            O recálculo reutiliza o mesmo modal de emissão, já com a empresa selecionada e o campo de novo vencimento habilitado.
          </p>
        </div>
      </div>

      {guides.length === 0 ? (
        <div className="rounded-b-2xl border-t border-dashed border-border bg-muted/20 px-6 py-8 text-sm text-muted-foreground">
          Nenhum débito vencido disponível para as empresas selecionadas no momento.
        </div>
      ) : (
        <>
          <Table className="min-w-[1320px] text-xs">
            <TableHeader>
              <TableRow>
                <TableHead>Empresa</TableHead>
                <TableHead>Período de Apuração</TableHead>
                <TableHead>Data de Vencimento</TableHead>
                <TableHead className="text-right">Débito Declarado (R$)</TableHead>
                <TableHead className="text-right">Principal (R$)</TableHead>
                <TableHead className="text-right">Multa (R$)</TableHead>
                <TableHead className="text-right">Juros (R$)</TableHead>
                <TableHead className="text-right">Total (R$)</TableHead>
                <TableHead>Nº Parcelamento (exigibilidade suspensa)</TableHead>
                <TableHead className="text-right">Ações</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {pagination.items.map((guide) => (
                <TableRow key={guide.id}>
                  <TableCell>
                    <div className="space-y-1">
                      <p className="font-medium">{guide.companyName}</p>
                      <p className="text-[11px] text-muted-foreground">
                        {guide.companyDocument || "CNPJ não informado"}
                      </p>
                    </div>
                  </TableCell>
                  <TableCell>{formatCompetenceLabel(guide.competence)}</TableCell>
                  <TableCell>{formatDateLabel(guide.dueDate)}</TableCell>
                  <TableCell className="text-right">
                    {formatCurrencyFromCents(guide.declaredAmountCents ?? null)}
                  </TableCell>
                  <TableCell className="text-right">
                    {formatCurrencyFromCents(guide.principalAmountCents ?? null)}
                  </TableCell>
                  <TableCell className="text-right">
                    {formatCurrencyFromCents(guide.penaltyAmountCents ?? null)}
                  </TableCell>
                  <TableCell className="text-right">
                    {formatCurrencyFromCents(guide.interestAmountCents ?? null)}
                  </TableCell>
                  <TableCell className="text-right">
                    {formatCurrencyFromCents(guide.totalAmountCents ?? guide.amountCents)}
                  </TableCell>
                  <TableCell>{guide.installmentLabel || guide.suspendedExigibilityLabel || "-"}</TableCell>
                  <TableCell className="text-right">
                    <div className="flex justify-end">
                      <Button type="button" disabled={busy} onClick={() => onRecalculate(guide)}>
                        <RefreshCcw className="h-4 w-4" />
                        Recalcular
                      </Button>
                    </div>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
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
        </>
      )}
    </GlassCard>
  );
}
