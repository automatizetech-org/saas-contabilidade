import { useEffect, useMemo, useRef, useState } from "react";
import { Building2, CalendarDays, RefreshCcw } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Separator } from "@/components/ui/separator";
import { cn } from "@/utils";
import type {
  DeclarationActionKind,
  DeclarationCompany,
  DeclarationGuideModalState,
  DeclarationGuideSubmitInput,
} from "../types";
import { formatCompetenceLabel, formatYearLabel } from "../helpers";

type DeclarationExecutionModalProps = {
  open: boolean;
  state: DeclarationGuideModalState;
  companies: DeclarationCompany[];
  defaultCompetence: string;
  busy?: boolean;
  onOpenChange: (open: boolean) => void;
  onSubmit: (input: DeclarationGuideSubmitInput) => void | Promise<void>;
  onOpenConsultGuide?: () => void;
};

function getModalCopy(action: DeclarationActionKind, recalculateByDefault: boolean) {
  if (action === "simples_extrato") {
    return {
      title: "Solicitar extrato",
      description: "Selecione as empresas e a competencia desejada para solicitar o extrato do Simples Nacional.",
      confirmLabel: "Solicitar extrato",
      summaryFlow: "Solicitacao de extrato",
      showRecalculate: false,
      referenceLabel: "Competencia",
    };
  }
  if (action === "simples_defis") {
    return {
      title: "Solicitar DEFIS",
      description: "Selecione as empresas e o ano desejado para solicitar a DEFIS anual.",
      confirmLabel: "Solicitar DEFIS",
      summaryFlow: "Solicitacao de DEFIS",
      showRecalculate: false,
      referenceLabel: "Ano",
    };
  }
  return {
    title: recalculateByDefault ? "Recalcular guia" : "Emitir guia",
    description: "Selecione as empresas e a competencia para emissao normal ou recalculo da guia.",
    confirmLabel: recalculateByDefault ? "Confirmar recalculo" : "Confirmar emissao",
    summaryFlow: recalculateByDefault ? "Recalculo" : "Emissao padrao",
    showRecalculate: true,
    referenceLabel: "Competencia",
  };
}

function normalizeModalReference(action: DeclarationActionKind, value: string, defaultCompetence: string) {
  const raw = String(value || "").trim();
  if (action === "simples_defis") {
    if (/^\d{4}$/.test(raw)) return raw;
    return defaultCompetence.slice(0, 4);
  }
  const slashMatch = raw.match(/^(\d{2})\/(\d{4})$/);
  if (slashMatch) return `${slashMatch[2]}-${slashMatch[1]}`;
  if (/^\d{4}-\d{2}$/.test(raw)) return raw;
  return defaultCompetence;
}

function formatReferenceLabel(action: DeclarationActionKind, value: string) {
  return action === "simples_defis" ? formatYearLabel(value) : formatCompetenceLabel(value);
}

function formatCompetenceInputValue(action: DeclarationActionKind, value: string) {
  const normalized = normalizeModalReference(action, value, value);
  if (action === "simples_defis") return normalized;
  return /^\d{4}-\d{2}$/.test(normalized) ? formatCompetenceLabel(normalized) : value;
}

function parseCompetenceInput(action: DeclarationActionKind, value: string) {
  const raw = String(value || "").replace(/\s+/g, "");
  if (action === "simples_defis") {
    return raw.replace(/\D/g, "").slice(0, 4);
  }
  const digits = raw.replace(/\D/g, "").slice(0, 6);
  if (digits.length <= 2) return digits;
  return `${digits.slice(0, 2)}/${digits.slice(2)}`;
}

function normalizeCompetenceSubmitValue(action: DeclarationActionKind, value: string, fallback: string) {
  return normalizeModalReference(action, value, fallback);
}

export function DeclarationExecutionModal({
  open,
  state,
  companies,
  defaultCompetence,
  busy = false,
  onOpenChange,
  onSubmit,
  onOpenConsultGuide,
}: DeclarationExecutionModalProps) {
  const modalCopy = getModalCopy(state.action, state.recalculateByDefault);
  const [search, setSearch] = useState("");
  const [selectedCompanyIds, setSelectedCompanyIds] = useState<string[]>([]);
  const [competence, setCompetence] = useState(formatCompetenceInputValue(state.action, defaultCompetence));
  const [recalculate, setRecalculate] = useState(false);
  const [recalculateDueDate, setRecalculateDueDate] = useState("");
  const dueDateRef = useRef<HTMLInputElement | null>(null);
  const currentYear = new Date().getFullYear();

  useEffect(() => {
    if (!open) return;
    const presetCompanyId =
      state.presetCompanyId && companies.some((company) => company.id === state.presetCompanyId)
        ? state.presetCompanyId
        : "";
    setSearch("");
    setSelectedCompanyIds(presetCompanyId ? [presetCompanyId] : []);
    const normalizedReference = normalizeModalReference(
      state.action,
      state.presetCompetence || defaultCompetence,
      defaultCompetence,
    );
    setCompetence(
      formatCompetenceInputValue(
        state.action,
        normalizedReference,
      ),
    );
    setRecalculate(state.action === "simples_emitir_guia" ? state.recalculateByDefault : false);
    setRecalculateDueDate(state.action === "simples_emitir_guia" ? state.presetDueDate || "" : "");
  }, [companies, defaultCompetence, open, state]);

  useEffect(() => {
    if (!open || !recalculate || !modalCopy.showRecalculate) return;
    const timer = window.setTimeout(() => dueDateRef.current?.focus(), 60);
    return () => window.clearTimeout(timer);
  }, [modalCopy.showRecalculate, open, recalculate]);

  const filteredCompanies = useMemo(() => {
    const term = search.trim().toLowerCase();
    if (!term) return companies;
    return companies.filter((company) => {
      const haystack = `${company.name} ${company.document ?? ""}`.toLowerCase();
      return haystack.includes(term);
    });
  }, [companies, search]);

  const selectionSummary =
    selectedCompanyIds.length === 0
      ? "Nenhuma empresa selecionada"
      : selectedCompanyIds.length === 1
        ? "1 empresa selecionada"
        : `${selectedCompanyIds.length} empresas selecionadas`;
  const normalizedCompetence = normalizeCompetenceSubmitValue(state.action, competence, defaultCompetence);

  const submit = () => {
    onSubmit({
      companyIds: selectedCompanyIds,
      competence: normalizedCompetence,
      recalculate: modalCopy.showRecalculate ? recalculate : false,
      recalculateDueDate: modalCopy.showRecalculate && recalculate ? recalculateDueDate : null,
    });
  };

  return (
    <Dialog open={open} onOpenChange={(next) => (!busy ? onOpenChange(next) : undefined)}>
      <DialogContent className="max-w-3xl gap-0 overflow-hidden border-border bg-card p-0">
        <DialogHeader className="space-y-2 border-b border-border px-6 py-5">
          <div className="flex items-start justify-between gap-4">
            <div className="space-y-2">
              <DialogTitle className="text-xl font-display tracking-tight">{modalCopy.title}</DialogTitle>
              <DialogDescription className="text-sm">{modalCopy.description}</DialogDescription>
            </div>
            {state.action === "simples_emitir_guia" && onOpenConsultGuide ? (
              <Button type="button" variant="outline" size="sm" disabled={busy} onClick={onOpenConsultGuide}>
                Consultar guia
              </Button>
            ) : null}
          </div>
        </DialogHeader>

        <div className="grid gap-0 lg:grid-cols-[1.25fr_0.9fr]">
          <div className="space-y-4 px-6 py-5">
            <div className="flex items-center justify-between gap-3">
              <div>
                <p className="text-sm font-medium text-foreground">Empresas disponiveis</p>
                <p className="text-xs text-muted-foreground">Somente empresas visiveis no contexto atual podem ser acionadas.</p>
              </div>
              <span className="rounded-full bg-muted px-3 py-1 text-[11px] font-medium text-muted-foreground">
                {selectionSummary}
              </span>
            </div>

            <Input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Buscar empresa por nome ou CNPJ..."
              disabled={busy}
            />

            <div className="flex flex-wrap gap-2">
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={busy || companies.length === 0}
                onClick={() => setSelectedCompanyIds(companies.map((company) => company.id))}
              >
                Selecionar todas
              </Button>
              <Button
                type="button"
                variant="ghost"
                size="sm"
                disabled={busy || selectedCompanyIds.length === 0}
                onClick={() => setSelectedCompanyIds([])}
              >
                Limpar
              </Button>
            </div>

            <div className="max-h-[320px] overflow-y-auto rounded-2xl border border-border bg-background/70">
              {filteredCompanies.length === 0 ? (
                <div className="px-4 py-6 text-sm text-muted-foreground">
                  Nenhuma empresa encontrada para o filtro informado.
                </div>
              ) : (
                <div className="divide-y divide-border">
                  {filteredCompanies.map((company) => {
                    const checked = selectedCompanyIds.includes(company.id);
                    return (
                      <label
                        key={company.id}
                        className={cn(
                          "flex cursor-pointer items-start gap-3 px-4 py-3 transition-colors",
                          checked ? "bg-primary/5" : "hover:bg-muted/40",
                          busy && "cursor-not-allowed opacity-70",
                        )}
                      >
                        <Checkbox
                          checked={checked}
                          disabled={busy}
                          onCheckedChange={(next) => {
                            const isChecked = Boolean(next);
                            setSelectedCompanyIds((current) =>
                              isChecked
                                ? Array.from(new Set([...current, company.id]))
                                : current.filter((companyId) => companyId !== company.id),
                            );
                          }}
                        />
                        <div className="min-w-0">
                          <p className="truncate text-sm font-medium">{company.name}</p>
                          <div className="mt-1 flex flex-wrap items-center gap-2 text-[11px] text-muted-foreground">
                            <span className="inline-flex items-center gap-1">
                              <Building2 className="h-3.5 w-3.5" />
                              {company.document || "CNPJ nao informado"}
                            </span>
                            {!company.active ? (
                              <span className="rounded-full bg-warning/15 px-2 py-0.5 text-warning">
                                Inativa
                              </span>
                            ) : null}
                          </div>
                        </div>
                      </label>
                    );
                  })}
                </div>
              )}
            </div>
          </div>

          <div className="border-t border-border px-6 py-5 lg:border-l lg:border-t-0">
            <div className="space-y-5">
              <div className="space-y-2">
                <Label htmlFor="declaration-competence">{modalCopy.referenceLabel}</Label>
                {state.action === "simples_defis" ? (
                  <Input
                    id="declaration-competence"
                    type="number"
                    min={2018}
                    max={currentYear + 1}
                    value={competence}
                    disabled={busy}
                    onChange={(event) => setCompetence(event.target.value)}
                  />
                ) : (
                  <Input
                    id="declaration-competence"
                    type="text"
                    value={competence}
                    placeholder="MM/AAAA"
                    inputMode="numeric"
                    disabled={busy}
                    onChange={(event) => setCompetence(parseCompetenceInput(state.action, event.target.value))}
                  />
                )}
                <p className="text-xs text-muted-foreground">
                  {state.action === "simples_defis"
                    ? `Ano sugerido: ${defaultCompetence.slice(0, 4)}.`
                    : `Competencia sugerida: ${formatCompetenceLabel(defaultCompetence)}. Informe no formato MM/AAAA.`}
                </p>
              </div>

              {modalCopy.showRecalculate ? (
                <>
                  <Separator />

                  <div className="rounded-2xl border border-border bg-background/60 p-4">
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <Label htmlFor="declaration-recalculate" className="text-sm">
                          Recalcular guia
                        </Label>
                        <p className="mt-1 text-xs text-muted-foreground">
                          Ative quando precisar reemitir com uma nova data de vencimento.
                        </p>
                      </div>
                      <Switch
                        id="declaration-recalculate"
                        checked={recalculate}
                        disabled={busy}
                        onCheckedChange={setRecalculate}
                      />
                    </div>

                    <div className={cn("mt-4 space-y-2", !recalculate && "opacity-60")}>
                      <Label htmlFor="declaration-recalculate-date">Nova data de vencimento</Label>
                      <Input
                        id="declaration-recalculate-date"
                        ref={dueDateRef}
                        type="date"
                        value={recalculateDueDate}
                        disabled={!recalculate || busy}
                        onChange={(event) => setRecalculateDueDate(event.target.value)}
                      />
                      <p className="flex items-center gap-1.5 text-xs text-muted-foreground">
                        <RefreshCcw className="h-3.5 w-3.5" />
                        O recalculo usa a mesma estrutura da emissao, com ajustes de vencimento.
                      </p>
                    </div>
                  </div>
                </>
              ) : null}

              <div className="rounded-2xl border border-dashed border-border bg-muted/20 p-4">
                <p className="text-xs font-medium uppercase tracking-[0.12em] text-muted-foreground">
                  Resumo da acao
                </p>
                <div className="mt-3 space-y-2 text-sm">
                  <div className="flex items-center justify-between gap-4">
                    <span className="text-muted-foreground">Empresas</span>
                    <strong>{selectedCompanyIds.length}</strong>
                  </div>
                  <div className="flex items-center justify-between gap-4">
                    <span className="text-muted-foreground">{modalCopy.referenceLabel}</span>
                    <strong className="inline-flex items-center gap-1">
                      <CalendarDays className="h-4 w-4 text-primary-icon" />
                      {formatReferenceLabel(state.action, normalizedCompetence)}
                    </strong>
                  </div>
                  <div className="flex items-center justify-between gap-4">
                    <span className="text-muted-foreground">Fluxo</span>
                    <strong>{modalCopy.showRecalculate && recalculate ? "Recalculo" : modalCopy.summaryFlow}</strong>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        <DialogFooter className="border-t border-border px-6 py-4">
          <Button type="button" variant="ghost" disabled={busy} onClick={() => onOpenChange(false)}>
            Cancelar
          </Button>
          <Button type="button" disabled={busy || selectedCompanyIds.length === 0} onClick={submit}>
            {busy ? "Enviando..." : modalCopy.showRecalculate && recalculate ? "Confirmar recalculo" : modalCopy.confirmLabel}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
