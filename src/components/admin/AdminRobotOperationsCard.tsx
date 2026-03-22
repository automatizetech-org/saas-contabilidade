import { useEffect, useMemo, useState } from "react";
import { Building2, Search, ShieldAlert } from "lucide-react";
import { format } from "date-fns";
import { ptBR } from "date-fns/locale";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { getRobotEligibilityReport } from "@/lib/robotEligibility";
import { formatCountdownLabel, getNextRobotRunAt } from "@/lib/robotExecutionPlanning";
import type { CompanyRobotConfig } from "@/services/companiesService";
import type { Company } from "@/services/profilesService";
import type { Robot } from "@/services/robotsService";
import { type ScheduleRule } from "@/services/scheduleRulesService";
import { cn } from "@/utils";

export function AdminRobotOperationsCard({
  robot,
  companies,
  companyConfigsByRobot,
  scheduleRule,
}: {
  robot: Robot;
  companies: Company[];
  companyConfigsByRobot: Map<string, Map<string, CompanyRobotConfig>>;
  scheduleRule: ScheduleRule | null;
}) {
  const [search, setSearch] = useState("");
  const [countdownMs, setCountdownMs] = useState(0);
  const [companiesModalOpen, setCompaniesModalOpen] = useState(false);

  const companyIds = useMemo(() => companies.map((company) => company.id), [companies]);
  const eligibility = useMemo(
    () =>
      getRobotEligibilityReport({
        robot,
        selectedCompanyIds: companyIds,
        companies,
        companyConfigsByRobot,
      }),
    [companyConfigsByRobot, companies, companyIds, robot],
  );
  const blockedByCompanyId = useMemo(
    () => new Map(eligibility.skipped.map((issue) => [issue.companyId, issue.reason])),
    [eligibility.skipped],
  );
  const eligibleCompanyIds = eligibility.eligibleCompanyIds;
  const hasActiveSchedule = robot.status !== "inactive" && scheduleRule?.status === "active";
  const nextRunAt = hasActiveSchedule ? getNextRobotRunAt(scheduleRule) : null;
  const nextRunAtKey = nextRunAt?.toISOString() ?? null;

  useEffect(() => {
    if (!nextRunAt) {
      setCountdownMs(0);
      return;
    }
    const tick = () => setCountdownMs(Math.max(0, nextRunAt.getTime() - Date.now()));
    tick();
    const timer = window.setInterval(tick, 1000);
    return () => window.clearInterval(timer);
  }, [nextRunAt, nextRunAtKey]);

  const filteredCompanies = useMemo(() => {
    const term = search.trim().toLowerCase();
    if (!term) return companies;
    return companies.filter((company) =>
      `${company.name} ${company.document ?? ""}`.toLowerCase().includes(term),
    );
  }, [companies, search]);

  return (
    <GlassCard className="border border-border/70 p-5">
      <div className="space-y-5">
        <div className="flex flex-col gap-3 xl:flex-row xl:items-start xl:justify-between">
          <div className="space-y-2">
            <div className="flex flex-wrap items-center gap-2">
              <h4 className="text-base font-semibold font-display tracking-tight">{robot.display_name}</h4>
              <Badge className="border-border bg-background text-muted-foreground">{robot.technical_id}</Badge>
              <Badge
                className={cn(
                  robot.status === "processing"
                    ? "border-amber-500/30 bg-amber-500/10 text-amber-100"
                    : robot.status === "active"
                      ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-100"
                      : "border-border bg-background text-muted-foreground",
                )}
              >
                {robot.status === "processing" ? "Executando" : robot.status === "active" ? "Ativo" : "Inativo"}
              </Badge>
            </div>
            <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
              <span>Segmento: {robot.segment_path || "Nao configurado"}</span>
              <span>Datas: {robot.date_execution_mode === "competencia" ? "Competencia" : "Intervalo"}</span>
              <span>Elegiveis agora: {eligibleCompanyIds.length}/{companies.length}</span>
            </div>
          </div>

          <div className="rounded-2xl border border-border bg-background/70 px-4 py-3">
            <p className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Proxima execucao</p>
            <p className="mt-1 text-sm font-semibold">
              {nextRunAt ? format(nextRunAt, "dd/MM/yyyy HH:mm", { locale: ptBR }) : "Sem agenda ativa"}
            </p>
          </div>
        </div>

        {nextRunAt ? (
          <div className="rounded-2xl border border-primary/20 bg-primary/5 px-4 py-4">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="text-[11px] uppercase tracking-[0.12em] text-primary-icon">Contagem regressiva</p>
                <p className="mt-1 font-mono text-xl text-primary-icon">{formatCountdownLabel(countdownMs)}</p>
              </div>
              <div className="text-right text-xs text-muted-foreground">
                <p>{format(nextRunAt, "EEEE, d 'de' MMMM", { locale: ptBR })}</p>
                <p>{format(nextRunAt, "'as' HH:mm", { locale: ptBR })}</p>
              </div>
            </div>
          </div>
        ) : null}

        <div className="rounded-2xl border border-border bg-background/40 p-4">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div className="min-w-0">
              <p className="text-sm font-medium">Lista de empresas</p>
              <p className="text-xs text-muted-foreground">
                Abra o painel somente leitura deste robo para acompanhar elegibilidade e bloqueios.
              </p>
            </div>
            <Button
              type="button"
              variant="outline"
              size="sm"
              className="w-full sm:w-auto"
              onClick={() => setCompaniesModalOpen(true)}
            >
              Lista de empresas
            </Button>
          </div>
        </div>
      </div>

      <Dialog
        open={companiesModalOpen}
        onOpenChange={(open) => {
          setCompaniesModalOpen(open);
          if (!open) setSearch("");
        }}
      >
        <DialogContent aria-describedby={undefined} className="max-h-[85vh] max-w-3xl overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Lista de empresas</DialogTitle>
          </DialogHeader>

          <div className="space-y-4">
            <div>
              <p className="text-sm font-medium">{robot.display_name}</p>
              <p className="text-xs text-muted-foreground">
                Painel somente de leitura para acompanhar elegibilidade e bloqueios deste robo.
              </p>
            </div>

            <div className="relative w-full sm:w-[260px]">
              <Search className="absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground" />
              <Input
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder="Buscar empresa..."
                className="h-9 pl-8 text-xs"
              />
            </div>

            <div className="rounded-2xl border border-border bg-background/70 px-4 py-3 text-xs text-muted-foreground">
              {hasActiveSchedule
                ? "A agenda deste robo esta ativa e a fila global acima mostra o acompanhamento."
                : "Sem agenda ativa para este robo no momento."}
            </div>

            <div className="max-h-[52vh] space-y-2 overflow-y-auto pr-1">
              {filteredCompanies.map((company) => {
                const eligible = eligibleCompanyIds.includes(company.id);
                const blockedReason = blockedByCompanyId.get(company.id) ?? null;
                return (
                  <div
                    key={company.id}
                    className={cn(
                      "flex items-start gap-3 rounded-2xl border px-4 py-3 transition-colors",
                      eligible ? "border-primary/30 bg-primary/5" : "border-border bg-background/70",
                    )}
                  >
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-2">
                        <p className="truncate text-sm font-medium">{company.name}</p>
                        <Badge
                          className={
                            eligible
                              ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-100"
                              : "border-amber-500/30 bg-amber-500/10 text-amber-100"
                          }
                        >
                          {eligible ? "Elegivel" : "Bloqueada"}
                        </Badge>
                      </div>
                      <div className="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-muted-foreground">
                        <span className="inline-flex items-center gap-1">
                          <Building2 className="h-3.5 w-3.5" />
                          {company.document || "CNPJ nao informado"}
                        </span>
                        {blockedReason ? (
                          <span className="inline-flex items-center gap-1 text-amber-200">
                            <ShieldAlert className="h-3.5 w-3.5" />
                            {blockedReason}
                          </span>
                        ) : null}
                      </div>
                    </div>
                  </div>
                );
              })}

              {filteredCompanies.length === 0 ? (
                <div className="rounded-2xl border border-border bg-background/70 px-4 py-6 text-center text-sm text-muted-foreground">
                  Nenhuma empresa encontrada para esta busca.
                </div>
              ) : null}
            </div>
          </div>
        </DialogContent>
      </Dialog>
    </GlassCard>
  );
}
