import { useEffect, useMemo, useState } from "react";
import { ArrowDownUp, FileClock, RefreshCcw } from "lucide-react";
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

type OverdueGuideFilter = "all" | "active" | "installment";
type OverdueGuideSortKey =
  | null
  | "companyName"
  | "competence"
  | "dueDate"
  | "declaredAmount"
  | "principalAmount"
  | "penaltyAmount"
  | "interestAmount"
  | "totalAmount"
  | "installmentLabel";
type OverdueGuideSortDirection = "asc" | "desc" | null;
type OverdueGuideSortState = {
  key: OverdueGuideSortKey;
  direction: OverdueGuideSortDirection;
};

type OverdueGuidesCardProps = {
  guides: OverdueGuide[];
  busy?: boolean;
  onRecalculate: (guide: OverdueGuide) => void;
};

function isGuideInInstallment(guide: OverdueGuide): boolean {
  const rawInstallment = String(
    guide.installmentLabel ?? guide.suspendedExigibilityLabel ?? guide.referenceLabel ?? "",
  ).trim();
  if (!rawInstallment) return false;
  const digits = rawInstallment.replace(/\D/g, "");
  if (!digits) return false;
  return !/^0+$/.test(digits);
}

function cycleSort(
  current: OverdueGuideSortState,
  key: Exclude<OverdueGuideSortKey, null>,
): OverdueGuideSortState {
  if (current.key !== key) return { key, direction: "desc" };
  if (current.direction === "desc") return { key, direction: "asc" };
  if (current.direction === "asc") return { key: null, direction: null };
  return { key, direction: "desc" };
}

function compareGuides(left: OverdueGuide, right: OverdueGuide, sort: OverdueGuideSortState): number {
  if (!sort.key || !sort.direction) return 0;

  const getValue = (guide: OverdueGuide): number | string => {
    switch (sort.key) {
      case "companyName":
        return String(guide.companyName || "").toLowerCase();
      case "competence":
        return String(guide.competence || "");
      case "dueDate":
        return String(guide.dueDate || "");
      case "declaredAmount":
        return Number(guide.declaredAmountCents ?? -1);
      case "principalAmount":
        return Number(guide.principalAmountCents ?? -1);
      case "penaltyAmount":
        return Number(guide.penaltyAmountCents ?? -1);
      case "interestAmount":
        return Number(guide.interestAmountCents ?? -1);
      case "totalAmount":
        return Number(guide.totalAmountCents ?? guide.amountCents ?? -1);
      case "installmentLabel": {
        const digits = String(
          guide.installmentLabel ?? guide.suspendedExigibilityLabel ?? guide.referenceLabel ?? "",
        ).replace(/\D/g, "");
        return digits || "0";
      }
      default:
        return "";
    }
  };

  const leftValue = getValue(left);
  const rightValue = getValue(right);
  const result =
    typeof leftValue === "number" && typeof rightValue === "number"
      ? leftValue - rightValue
      : String(leftValue).localeCompare(String(rightValue), "pt-BR");
  return sort.direction === "desc" ? result * -1 : result;
}

function SortHeader({
  label,
  column,
  sort,
  onToggle,
  align = "left",
}: {
  label: string;
  column: Exclude<OverdueGuideSortKey, null>;
  sort: OverdueGuideSortState;
  onToggle: (key: Exclude<OverdueGuideSortKey, null>) => void;
  align?: "left" | "right";
}) {
  const active = sort.key === column ? (sort.direction === "desc" ? " ↓" : sort.direction === "asc" ? " ↑" : "") : "";

  return (
    <button
      type="button"
      onClick={() => onToggle(column)}
      className={[
        "inline-flex items-center gap-1 font-medium text-muted-foreground hover:text-foreground",
        align === "right" ? "ml-auto flex justify-end text-right" : "text-left",
      ].join(" ")}
    >
      <span>{label}{active}</span>
      <ArrowDownUp className={`h-3.5 w-3.5 ${sort.key === column ? "text-foreground" : "opacity-50"}`} />
    </button>
  );
}

export function OverdueGuidesCard({
  guides,
  busy = false,
  onRecalculate,
}: OverdueGuidesCardProps) {
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const [filter, setFilter] = useState<OverdueGuideFilter>("active");
  const [sort, setSort] = useState<OverdueGuideSortState>({ key: null, direction: null });

  const filterCounts = useMemo(() => {
    const installment = guides.filter((guide) => isGuideInInstallment(guide)).length;
    const active = guides.length - installment;
    return {
      all: guides.length,
      active,
      installment,
    };
  }, [guides]);

  const filteredGuides = useMemo(() => {
    if (filter === "active") {
      return guides.filter((guide) => !isGuideInInstallment(guide));
    }
    if (filter === "installment") {
      return guides.filter((guide) => isGuideInInstallment(guide));
    }
    return guides;
  }, [filter, guides]);

  const sortedGuides = useMemo(() => {
    return [...filteredGuides].sort((left, right) => compareGuides(left, right, sort));
  }, [filteredGuides, sort]);

  useEffect(() => {
    setCurrentPage(1);
  }, [sortedGuides.length, pageSize, sort]);

  const pagination = useMemo(() => {
    const total = sortedGuides.length;
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
      items: sortedGuides.slice(fromIndex, toIndex),
    };
  }, [currentPage, sortedGuides, pageSize]);

  const filterOptions: Array<{
    value: OverdueGuideFilter;
    label: string;
    count: number;
  }> = [
    { value: "all", label: "Todos", count: filterCounts.all },
    { value: "active", label: "Debitos ativos", count: filterCounts.active },
    { value: "installment", label: "Em parcelamento", count: filterCounts.installment },
  ];

  return (
    <GlassCard className="border border-border/70 p-0">
      <div className="space-y-4 border-b border-border/70 p-6">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
          <div className="space-y-2">
            <div className="flex items-center gap-2">
              <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-destructive/10 text-destructive">
                <FileClock className="h-5 w-5" />
              </div>
              <div>
                <h3 className="font-display text-lg font-semibold tracking-tight">Guias vencidas</h3>
                <p className="text-sm text-muted-foreground">
                  Lista completa dos debitos vencidos retornados pelo robo de consulta de debitos do
                  Simples Nacional.
                </p>
              </div>
            </div>
          </div>

          <div className="flex flex-col gap-3 sm:min-w-[340px] sm:items-end">
            <div className="min-w-[220px] rounded-3xl border border-border/80 bg-gradient-to-br from-background via-background to-muted/40 px-5 py-4 text-right shadow-sm">
              <p className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                Total exibido
              </p>
              <div className="mt-2 flex items-end justify-end gap-2">
                <p className="font-display text-3xl font-semibold leading-none">{filteredGuides.length}</p>
                <p className="pb-0.5 text-xs text-muted-foreground">de {guides.length}</p>
              </div>
            </div>

            <div className="flex flex-wrap justify-end gap-2">
              {filterOptions.map((option) => {
                const active = filter === option.value;
                return (
                  <button
                    key={option.value}
                    type="button"
                    onClick={() => setFilter(option.value)}
                    className={[
                      "inline-flex items-center gap-2 rounded-full border px-3 py-1.5 text-xs font-medium transition-colors",
                      active
                        ? "border-primary bg-primary text-primary-foreground shadow-sm"
                        : "border-border bg-background text-muted-foreground hover:border-primary/40 hover:text-foreground",
                    ].join(" ")}
                  >
                    <span>{option.label}</span>
                    <span
                      className={[
                        "rounded-full px-1.5 py-0.5 text-[10px]",
                        active
                          ? "bg-primary-foreground/15 text-primary-foreground"
                          : "bg-muted text-foreground",
                      ].join(" ")}
                    >
                      {option.count}
                    </span>
                  </button>
                );
              })}
            </div>
          </div>
        </div>
      </div>

      {filteredGuides.length === 0 ? (
        <div className="rounded-b-2xl border-t border-dashed border-border bg-muted/20 px-6 py-8 text-sm text-muted-foreground">
          {guides.length === 0
            ? "Nenhum debito vencido disponivel para as empresas selecionadas no momento."
            : "Nenhum registro corresponde ao filtro selecionado."}
        </div>
      ) : (
        <>
          <Table className="min-w-[1320px] text-xs">
            <TableHeader>
              <TableRow>
                <TableHead>
                  <SortHeader label="Empresa" column="companyName" sort={sort} onToggle={(key) => setSort((current) => cycleSort(current, key))} />
                </TableHead>
                <TableHead>
                  <SortHeader label="Periodo de Apuracao" column="competence" sort={sort} onToggle={(key) => setSort((current) => cycleSort(current, key))} />
                </TableHead>
                <TableHead>
                  <SortHeader label="Data de Vencimento" column="dueDate" sort={sort} onToggle={(key) => setSort((current) => cycleSort(current, key))} />
                </TableHead>
                <TableHead className="text-right">
                  <SortHeader label="Debito Declarado (R$)" column="declaredAmount" sort={sort} onToggle={(key) => setSort((current) => cycleSort(current, key))} align="right" />
                </TableHead>
                <TableHead className="text-right">
                  <SortHeader label="Principal (R$)" column="principalAmount" sort={sort} onToggle={(key) => setSort((current) => cycleSort(current, key))} align="right" />
                </TableHead>
                <TableHead className="text-right">
                  <SortHeader label="Multa (R$)" column="penaltyAmount" sort={sort} onToggle={(key) => setSort((current) => cycleSort(current, key))} align="right" />
                </TableHead>
                <TableHead className="text-right">
                  <SortHeader label="Juros (R$)" column="interestAmount" sort={sort} onToggle={(key) => setSort((current) => cycleSort(current, key))} align="right" />
                </TableHead>
                <TableHead className="text-right">
                  <SortHeader label="Total (R$)" column="totalAmount" sort={sort} onToggle={(key) => setSort((current) => cycleSort(current, key))} align="right" />
                </TableHead>
                <TableHead>
                  <SortHeader label="No Parcelamento (exigibilidade suspensa)" column="installmentLabel" sort={sort} onToggle={(key) => setSort((current) => cycleSort(current, key))} />
                </TableHead>
                <TableHead className="text-right">Acoes</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {pagination.items.map((guide) => {
                const installmentLabel =
                  guide.installmentLabel || guide.suspendedExigibilityLabel || "-";

                return (
                  <TableRow key={guide.id}>
                    <TableCell>
                      <div className="space-y-1">
                        <p className="font-medium">{guide.companyName}</p>
                        <p className="text-[11px] text-muted-foreground">
                          {guide.companyDocument || "CNPJ nao informado"}
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
                    <TableCell>{installmentLabel}</TableCell>
                    <TableCell className="text-right">
                      <div className="flex justify-end">
                        <Button type="button" disabled={busy} onClick={() => onRecalculate(guide)}>
                          <RefreshCcw className="h-4 w-4" />
                          Recalcular
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                );
              })}
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
