import { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { PieChart, Pie, Cell, ResponsiveContainer, Tooltip } from "recharts";
import { toast } from "sonner";
import { ArrowDownUp, Copy, Download, FileCheck, FileClock, Landmark, Link2, Plus, QrCode, ReceiptText, Trash2, Wallet } from "lucide-react";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatsCard } from "@/components/dashboard/StatsCard";
import { DataPagination } from "@/components/common/DataPagination";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useCompanies } from "@/hooks/useCompanies";
import { fetchCnpjPublica } from "@/services/cnpjPublicaService";
import { formatCpfOrCnpjInput, isValidCpfOrCnpj } from "@/lib/brazilDocuments";
import { cn } from "@/utils";
import {
  createIrClient,
  deleteIrClient,
  downloadBoletoPdf,
  generateIrCharge,
  getIrClients,
  getIrOverviewSummary,
  type IrClient,
  type IrChargeType,
  type IrDeclarationStatus,
  type IrPaymentStatus,
  updateIrClient,
} from "@/services/irService";

type SortKey =
  | "nome"
  | "cpf_cnpj"
  | "responsavel_ir"
  | "valor_servico"
  | "vencimento"
  | "status_pagamento"
  | "status_declaracao"
  | "created_at"
  | null;
type SortDirection = "desc" | "asc" | null;
type SortState = { key: SortKey; direction: SortDirection };
type UnifiedFilters = {
  search: string;
  paymentStatus: "Todos" | IrPaymentStatus;
  declarationStatus: "Todos" | IrDeclarationStatus;
  dateFrom: string;
  dateTo: string;
  minValue: string;
  maxValue: string;
};
type PaymentFilters = { status: "Todos" | IrPaymentStatus; dateFrom: string; dateTo: string; minValue: string; maxValue: string };
type ExecutionFilters = { status: "Todos" | IrDeclarationStatus; dateFrom: string; dateTo: string; minValue: string; maxValue: string };
type IrFormState = {
  nome: string;
  cpf_cnpj: string;
  responsavel_ir: string;
  vencimento: string;
  valor_servico: string;
  observacoes: string;
  status_pagamento: IrPaymentStatus;
};
type IrEditFormState = IrFormState & { id: string };
type ObservationAutocompleteContext = {
  suggestions: string[];
  replaceStart: number;
  replaceEnd: number;
};

const IR_PAYMENT_OPTIONS: IrPaymentStatus[] = ["PIX", "DINHEIRO", "TRANSFERÊNCIA POUPANÇA", "PERMUTA", "A PAGAR"];
const IR_PAYMENT_OPTIONS_WITH_CHARGES: IrPaymentStatus[] = ["PIX", "BOLETO", ...IR_PAYMENT_OPTIONS.slice(1)];
const OBSERVATION_KEYWORDS = ["HONORARIO", "PRO BONO", "PERMUTA", "IRRF+MEI", "LIVRO CAIXA"];
const emptyForm: IrFormState = { nome: "", cpf_cnpj: "", responsavel_ir: "", vencimento: "", valor_servico: "", observacoes: "", status_pagamento: "A PAGAR" };
const emptyUnifiedFilters: UnifiedFilters = { search: "", paymentStatus: "Todos", declarationStatus: "Todos", dateFrom: "", dateTo: "", minValue: "", maxValue: "" };
const emptyPaymentFilters: PaymentFilters = { status: "Todos", dateFrom: "", dateTo: "", minValue: "", maxValue: "" };
const emptyExecutionFilters: ExecutionFilters = { status: "Todos", dateFrom: "", dateTo: "", minValue: "", maxValue: "" };
const paymentStatusOptions: Array<"Todos" | IrPaymentStatus> = ["Todos", ...IR_PAYMENT_OPTIONS_WITH_CHARGES];
const executionStatusOptions: Array<"Todos" | IrDeclarationStatus> = ["Todos", "Concluido", "Pendente"];
const emptyEditForm: IrEditFormState = { id: "", nome: "", cpf_cnpj: "", responsavel_ir: "", vencimento: "", valor_servico: "", observacoes: "", status_pagamento: "A PAGAR" };
const NEW_RESPONSIBLE_VALUE = "__novo_responsavel__";

function formatCurrency(value: number) {
  return value.toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
}

function normalizeCurrencyInput(value: string) {
  return value.replace(",", ".").replace(/[^\d.]/g, "");
}

function formatCurrencyInput(value: string) {
  const digits = value.replace(/\D/g, "");
  if (!digits) return "";
  const numericValue = Number(digits) / 100;
  return numericValue.toLocaleString("pt-BR", { style: "currency", currency: "BRL" });
}

function onlyDigits(value: string) {
  return value.replace(/\D/g, "");
}

function formatIsoDate(value: string | null | undefined) {
  return value ? String(value).slice(0, 10) : "";
}

function formatDateLabel(value: string | null | undefined) {
  if (!value) return "-";
  const [year, month, day] = String(value).slice(0, 10).split("-");
  if (!year || !month || !day) return String(value);
  return `${day}/${month}/${year}`;
}

function getTodayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function isReceivedPaymentType(status: IrPaymentStatus) {
  return status !== "A PAGAR";
}

function getPaymentTriggerClass(status: IrPaymentStatus) {
  return isReceivedPaymentType(status) ? "ir-status-trigger--success" : "ir-status-trigger--warning";
}

function getChargeStatusLabel(client: IrClient) {
  if (client.payment_charge_status === "paid") return "Pago automaticamente";
  if (client.payment_charge_status === "pending" && client.payment_charge_type === "PIX") return "PIX gerado";
  if (client.payment_charge_status === "pending" && client.payment_charge_type === "BOLETO") return "Boleto gerado";
  if (client.payment_charge_status === "pending" && client.payment_charge_type === "BOLETO_HIBRIDO") return "Boleto hibrido gerado";
  if (client.payment_charge_status === "failed") return "Falha na cobranca";
  if (client.payment_charge_status === "cancelled") return "Cobranca cancelada";
  return "Sem cobranca";
}

function getChargeStatusClass(client: IrClient) {
  if (client.payment_charge_status === "paid") return "text-emerald-600";
  if (client.payment_charge_status === "pending") return "text-amber-600";
  if (client.payment_charge_status === "failed") return "text-destructive";
  return "text-muted-foreground";
}

function withPaymentTypeAndDate<T extends { status_pagamento: IrPaymentStatus; vencimento: string }>(current: T, status: IrPaymentStatus): T {
  return {
    ...current,
    status_pagamento: status,
    vencimento: status !== "A PAGAR" && !current.vencimento ? getTodayIsoDate() : current.vencimento,
  };
}

function normalizeAutocompleteValue(value: string) {
  return value.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toLowerCase();
}

function getObservationAutocompleteContext(value: string, caretPosition: number, companyNames: string[]): ObservationAutocompleteContext | null {
  const textBeforeCaret = value.slice(0, caretPosition);
  const lineStart = textBeforeCaret.lastIndexOf("\n") + 1;
  const linePrefix = textBeforeCaret.slice(lineStart);
  const companyMatch = linePrefix.match(/\/empresa(?:\s+(.*))?$/i);
  const companyWordMatch = linePrefix.match(/^empresa(?:\s+(.*))?$/i);

  if (companyMatch) {
    const query = (companyMatch[1] ?? "").trim();
    const normalizedQuery = normalizeAutocompleteValue(query);
    const suggestions = companyNames
      .filter((company) => !normalizedQuery || normalizeAutocompleteValue(company).startsWith(normalizedQuery))
      .slice(0, 6);

    return suggestions.length
      ? {
          suggestions: suggestions.map((company) => `EMPRESA: ${company.toUpperCase()}`),
          replaceStart: lineStart,
          replaceEnd: caretPosition,
        }
      : null;
  }

  if (companyWordMatch) {
    const query = (companyWordMatch[1] ?? "").trim();
    const normalizedQuery = normalizeAutocompleteValue(query);
    const suggestions = companyNames
      .filter((company) => !normalizedQuery || normalizeAutocompleteValue(company).startsWith(normalizedQuery))
      .slice(0, 6);

    return suggestions.length
      ? {
          suggestions: suggestions.map((company) => `EMPRESA: ${company.toUpperCase()}`),
          replaceStart: lineStart,
          replaceEnd: caretPosition,
        }
      : null;
  }

  const keywordMatch = textBeforeCaret.match(/(?:^|\s)([^\s\n]+)$/);
  const currentToken = keywordMatch?.[1] ?? "";
  if (!currentToken) return null;

  const normalizedToken = normalizeAutocompleteValue(currentToken);
  const suggestions = OBSERVATION_KEYWORDS
    .filter((keyword) => normalizeAutocompleteValue(keyword).startsWith(normalizedToken))
    .slice(0, 6);

  if (!suggestions.length) return null;

  return {
    suggestions,
    replaceStart: caretPosition - currentToken.length,
    replaceEnd: caretPosition,
  };
}

function cycleSort(current: SortState, key: Exclude<SortKey, null>): SortState {
  if (current.key !== key) return { key, direction: "desc" };
  if (current.direction === "desc") return { key, direction: "asc" };
  if (current.direction === "asc") return { key: null, direction: null };
  return { key, direction: "desc" };
}

function compareClients(a: IrClient, b: IrClient, sort: SortState) {
  if (!sort.key || !sort.direction) return 0;
  const getValue = (client: IrClient) => {
    if (sort.key === "nome") return String(client.nome || "").toLowerCase();
    if (sort.key === "cpf_cnpj") return onlyDigits(client.cpf_cnpj || "");
    if (sort.key === "responsavel_ir") return String(client.responsavel_ir || "").toLowerCase();
    if (sort.key === "valor_servico") return Number(client.valor_servico || 0);
    if (sort.key === "vencimento") return String(client.vencimento || "");
    if (sort.key === "status_pagamento") return String(client.status_pagamento || "");
    if (sort.key === "status_declaracao") return String(client.status_declaracao || "");
    return new Date(client.created_at).getTime();
  };
  const aValue = getValue(a);
  const bValue = getValue(b);
  const result =
    typeof aValue === "number" && typeof bValue === "number"
      ? aValue - bValue
      : String(aValue).localeCompare(String(bValue), "pt-BR");
  return sort.direction === "desc" ? result * -1 : result;
}

function SortHeader({ label, column, sort, onToggle }: { label: string; column: Exclude<SortKey, null>; sort: SortState; onToggle: (key: Exclude<SortKey, null>) => void }) {
  const active = sort.key === column ? (sort.direction === "desc" ? " ↓" : sort.direction === "asc" ? " ↑" : "") : "";
  return (
    <button type="button" onClick={() => onToggle(column)} className="inline-flex items-center gap-1 text-left font-medium text-muted-foreground hover:text-foreground">
      <span>{label}{active}</span>
      <ArrowDownUp className={`h-3.5 w-3.5 ${sort.key === column ? "text-foreground" : "opacity-50"}`} />
    </button>
  );
}

function ObservationTextarea({
  id,
  value,
  onChange,
  rows,
  placeholder,
  companyNames,
}: {
  id: string;
  value: string;
  onChange: (value: string) => void;
  rows: number;
  placeholder?: string;
  companyNames: string[];
}) {
  const textareaRef = useRef<HTMLTextAreaElement | null>(null);
  const [caretPosition, setCaretPosition] = useState(value.length);
  const [selectedSuggestionIndex, setSelectedSuggestionIndex] = useState(0);

  const autocomplete = useMemo(
    () => getObservationAutocompleteContext(value, caretPosition, companyNames),
    [value, caretPosition, companyNames],
  );

  useEffect(() => {
    setSelectedSuggestionIndex(0);
  }, [autocomplete?.replaceStart, autocomplete?.replaceEnd, autocomplete?.suggestions.join("|")]);

  const applySuggestion = (suggestion: string) => {
    if (!autocomplete) return;
    const nextValue = `${value.slice(0, autocomplete.replaceStart)}${suggestion}${value.slice(autocomplete.replaceEnd)}`;
    const nextCaret = autocomplete.replaceStart + suggestion.length;
    onChange(nextValue);
    setCaretPosition(nextCaret);
    window.requestAnimationFrame(() => {
      textareaRef.current?.focus();
      textareaRef.current?.setSelectionRange(nextCaret, nextCaret);
    });
  };

  return (
    <div className="relative">
      <Textarea
        id={id}
        ref={textareaRef}
        value={value}
        rows={rows}
        placeholder={placeholder}
        onChange={(event) => {
          onChange(event.target.value);
          setCaretPosition(event.target.selectionStart ?? event.target.value.length);
        }}
        onClick={(event) => setCaretPosition(event.currentTarget.selectionStart ?? value.length)}
        onKeyUp={(event) => setCaretPosition(event.currentTarget.selectionStart ?? value.length)}
        onSelect={(event) => setCaretPosition(event.currentTarget.selectionStart ?? value.length)}
        onKeyDown={(event) => {
          if (!autocomplete?.suggestions.length) return;
          if (event.key === "Tab" || event.key === "Enter") {
            event.preventDefault();
            applySuggestion(autocomplete.suggestions[selectedSuggestionIndex] ?? autocomplete.suggestions[0]);
            return;
          }
          if (event.key === "ArrowDown") {
            event.preventDefault();
            setSelectedSuggestionIndex((current) => (current + 1) % autocomplete.suggestions.length);
            return;
          }
          if (event.key === "ArrowUp") {
            event.preventDefault();
            setSelectedSuggestionIndex((current) => (current - 1 + autocomplete.suggestions.length) % autocomplete.suggestions.length);
          }
        }}
      />
      {autocomplete?.suggestions.length ? (
        <div className="absolute left-0 right-0 top-full z-20 mt-2 rounded-md border border-border bg-background shadow-lg">
          {autocomplete.suggestions.map((suggestion, index) => (
            <button
              key={suggestion}
              type="button"
              className={cn(
                "flex w-full items-center justify-between px-3 py-2 text-left text-sm",
                index === selectedSuggestionIndex ? "bg-muted text-foreground" : "text-muted-foreground hover:bg-muted/60",
              )}
              onMouseDown={(event) => {
                event.preventDefault();
                applySuggestion(suggestion);
              }}
            >
              <span className="truncate">{suggestion}</span>
              {index === selectedSuggestionIndex ? <span className="text-[11px] uppercase tracking-wide text-muted-foreground">Tab</span> : null}
            </button>
          ))}
        </div>
      ) : null}
    </div>
  );
}

export default function IRPage() {
  const queryClient = useQueryClient();
  const [globalResponsible, setGlobalResponsible] = useState("Todos");
  const [tablePageSize, setTablePageSize] = useState(10);
  const [tableCurrentPage, setTableCurrentPage] = useState(1);
  const [tableFilters, setTableFilters] = useState(emptyUnifiedFilters);
  const [tableSort, setTableSort] = useState<SortState>({ key: null, direction: null });
  const [paymentPageSize, setPaymentPageSize] = useState(10);
  const [paymentCurrentPage, setPaymentCurrentPage] = useState(1);
  const [executionPageSize, setExecutionPageSize] = useState(10);
  const [executionCurrentPage, setExecutionCurrentPage] = useState(1);
  const [form, setForm] = useState<IrFormState>(emptyForm);
  const [notesDraft, setNotesDraft] = useState<Record<string, string>>({});
  const [paymentFilters, setPaymentFilters] = useState(emptyPaymentFilters);
  const [executionFilters, setExecutionFilters] = useState(emptyExecutionFilters);
  const [paymentSort, setPaymentSort] = useState<SortState>({ key: null, direction: null });
  const [executionSort, setExecutionSort] = useState<SortState>({ key: null, direction: null });
  const [autofillLoading, setAutofillLoading] = useState(false);
  const [enhancedUiEnabled, setEnhancedUiEnabled] = useState(true);
  const [showCreateOverlay, setShowCreateOverlay] = useState(false);
  const [isEditDialogOpen, setIsEditDialogOpen] = useState(false);
  const [editForm, setEditForm] = useState<IrEditFormState>(emptyEditForm);
  const [isNewResponsibleMode, setIsNewResponsibleMode] = useState(false);
  const [isEditNewResponsibleMode, setIsEditNewResponsibleMode] = useState(false);
  const [isObservationDialogOpen, setIsObservationDialogOpen] = useState(false);
  const [observationClientId, setObservationClientId] = useState<string>("");
  const [observationClientName, setObservationClientName] = useState("");
  const [isChargeDialogOpen, setIsChargeDialogOpen] = useState(false);
  const [chargeClient, setChargeClient] = useState<IrClient | null>(null);
  const [chargeType, setChargeType] = useState<IrChargeType>("PIX");

  const { data: clients = [], isLoading } = useQuery({ queryKey: ["ir-clients"], queryFn: getIrClients });
  const { data: overviewSummary } = useQuery({
    queryKey: ["ir-overview-summary", globalResponsible],
    queryFn: () => getIrOverviewSummary(globalResponsible === "Todos" ? null : globalResponsible),
  });
  const { data: companies = [] } = useCompanies();

  const companyNames = useMemo<string[]>(
    () =>
      Array.from(
        new Set(
          (companies as Array<{ name?: string | null }>)
            .map((company) => company.name?.trim() || "")
            .filter(Boolean),
        ),
      ).sort((a, b) => a.localeCompare(b, "pt-BR")),
    [companies],
  );

  useEffect(() => setNotesDraft(Object.fromEntries(clients.map((client) => [client.id, client.observacoes ?? ""]))), [clients]);
  useEffect(() => {
    if (!chargeClient) return;
    const freshClient = clients.find((client) => client.id === chargeClient.id);
    if (freshClient) {
      setChargeClient(freshClient);
    }
  }, [clients, chargeClient]);

  const refreshIrData = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["ir-clients"] }),
      queryClient.invalidateQueries({ queryKey: ["ir-overview-summary"] }),
    ]);
  };

  const createClientMutation = useMutation({
    mutationFn: async () => {
      const valorServico = Number(normalizeCurrencyInput(form.valor_servico));
      const normalizedCpfCnpj = onlyDigits(form.cpf_cnpj);
      if (!form.nome.trim() || !form.cpf_cnpj.trim() || Number.isNaN(valorServico)) {
        throw new Error("Preencha nome, CPF/CNPJ e valor do serviço.");
      }
      if (!isValidCpfOrCnpj(normalizedCpfCnpj)) {
        throw new Error("Informe um CPF ou CNPJ válido.");
      }
      if (clients.some((client) => onlyDigits(client.cpf_cnpj || "") === normalizedCpfCnpj)) {
        throw new Error("Já existe um cliente de IR cadastrado com este CPF/CNPJ.");
      }
      return createIrClient({
        nome: form.nome,
        cpf_cnpj: form.cpf_cnpj,
        responsavel_ir: form.responsavel_ir,
        vencimento: form.vencimento || null,
        valor_servico: valorServico,
        status_pagamento: form.vencimento ? form.status_pagamento : "A PAGAR",
        observacoes: form.observacoes,
      });
    },
    onSuccess: async () => {
      setForm(emptyForm);
      setIsNewResponsibleMode(false);
      await refreshIrData();
      toast.success("Cliente de IR cadastrado.");
    },
    onError: (error) => toast.error(error instanceof Error ? error.message : "Não foi possível cadastrar o cliente."),
  });

  useEffect(() => {
    if (!showCreateOverlay) return undefined;
    const timeoutId = window.setTimeout(() => {
      setShowCreateOverlay(false);
      createClientMutation.mutate();
    }, 2000);
    return () => window.clearTimeout(timeoutId);
  }, [createClientMutation, showCreateOverlay]);

  const updateClientMutation = useMutation({
    mutationFn: async ({ id, updates, successMessage }: { id: string; updates: Partial<Pick<IrClient, "status_pagamento" | "status_declaracao" | "observacoes" | "valor_servico" | "nome" | "cpf_cnpj" | "responsavel_ir" | "vencimento">>; successMessage: string }) => {
      await updateIrClient(id, updates);
      return successMessage;
    },
    onSuccess: async (message) => {
      await refreshIrData();
      toast.success(message);
    },
    onError: (error) => toast.error(error instanceof Error ? error.message : "Não foi possível atualizar o cliente."),
  });

  const deleteClientMutation = useMutation({
    mutationFn: async (id: string) => {
      await deleteIrClient(id);
    },
    onSuccess: async () => {
      await refreshIrData();
      toast.success("Cliente de IR excluído.");
    },
    onError: (error) => toast.error(error instanceof Error ? error.message : "Não foi possível excluir o cliente."),
  });

  const generateChargeMutation = useMutation({
    mutationFn: async ({ clientId, nextChargeType }: { clientId: string; nextChargeType: IrChargeType }) => {
      return generateIrCharge({ clientId, chargeType: nextChargeType });
    },
    onSuccess: async (result, variables) => {
      await refreshIrData();
      if ((variables.nextChargeType === "BOLETO" || variables.nextChargeType === "BOLETO_HIBRIDO") && result.boletoPdfBase64) {
        downloadBoletoPdf(`boleto-ir-${result.client.nome}.pdf`, result.boletoPdfBase64);
        toast.success("Boleto gerado e baixado.");
      } else if (variables.nextChargeType === "PIX") {
        if (result.pixCopyPaste) {
          await navigator.clipboard.writeText(result.pixCopyPaste);
          toast.success("PIX gerado e codigo copiado.");
        } else {
          toast.success("PIX gerado.");
        }
      }
      setChargeClient(result.client);
    },
    onError: (error) => toast.error(error instanceof Error ? error.message : "Nao foi possivel gerar a cobranca."),
  });

  const responsibleOptions = useMemo(() => Array.from(new Set(clients.map((c) => c.responsavel_ir?.trim() || "").filter(Boolean))).sort((a, b) => a.localeCompare(b, "pt-BR")), [clients]);
  const clientsByResponsible = useMemo(() => globalResponsible === "Todos" ? clients : clients.filter((c) => (c.responsavel_ir?.trim() || "") === globalResponsible), [clients, globalResponsible]);
  const paymentSummary = useMemo(() => ({
    paid: clientsByResponsible.filter((c) => isReceivedPaymentType(c.status_pagamento)).length,
    pending: clientsByResponsible.filter((c) => c.status_pagamento === "A PAGAR").length,
    totalValue: clientsByResponsible.reduce((sum, c) => sum + Number(c.valor_servico || 0), 0),
  }), [clientsByResponsible]);
  const executionSummary = useMemo(() => ({
    concluido: clientsByResponsible.filter((c) => c.status_declaracao === "Concluido").length,
    pendente: clientsByResponsible.filter((c) => c.status_declaracao !== "Concluido").length,
    total: clientsByResponsible.length,
  }), [clientsByResponsible]);
  const paymentValueSummary = useMemo(() => ({
    paid: clientsByResponsible.filter((c) => isReceivedPaymentType(c.status_pagamento)).reduce((sum, c) => sum + Number(c.valor_servico || 0), 0),
    pending: clientsByResponsible.filter((c) => c.status_pagamento === "A PAGAR").reduce((sum, c) => sum + Number(c.valor_servico || 0), 0),
  }), [clientsByResponsible]);

  const unifiedClients = useMemo(() => [...clientsByResponsible].filter((client) => {
    const createdAt = formatIsoDate(client.created_at);
    const value = Number(client.valor_servico || 0);
    const search = tableFilters.search.trim().toLowerCase();
    const matchesSearch = !search || [
      client.nome,
      client.cpf_cnpj,
      client.responsavel_ir,
      client.observacoes,
    ].some((field) => String(field || "").toLowerCase().includes(search));
    if (!matchesSearch) return false;
    if (tableFilters.paymentStatus !== "Todos" && client.status_pagamento !== tableFilters.paymentStatus) return false;
    if (tableFilters.declarationStatus !== "Todos" && client.status_declaracao !== tableFilters.declarationStatus) return false;
    if (tableFilters.dateFrom && createdAt < tableFilters.dateFrom) return false;
    if (tableFilters.dateTo && createdAt > tableFilters.dateTo) return false;
    if (tableFilters.minValue && value < Number(normalizeCurrencyInput(tableFilters.minValue) || "0")) return false;
    if (tableFilters.maxValue && value > Number(normalizeCurrencyInput(tableFilters.maxValue) || "0")) return false;
    return true;
  }).sort((a, b) => compareClients(a, b, tableSort)), [clientsByResponsible, tableFilters, tableSort]);

  const filteredPayments = useMemo(() => [...clientsByResponsible].filter((client) => {
    const createdAt = formatIsoDate(client.created_at);
    const value = Number(client.valor_servico || 0);
    if (paymentFilters.status !== "Todos" && client.status_pagamento !== paymentFilters.status) return false;
    if (paymentFilters.dateFrom && createdAt < paymentFilters.dateFrom) return false;
    if (paymentFilters.dateTo && createdAt > paymentFilters.dateTo) return false;
    if (paymentFilters.minValue && value < Number(normalizeCurrencyInput(paymentFilters.minValue) || "0")) return false;
    if (paymentFilters.maxValue && value > Number(normalizeCurrencyInput(paymentFilters.maxValue) || "0")) return false;
    return true;
  }).sort((a, b) => compareClients(a, b, paymentSort)), [clientsByResponsible, paymentFilters, paymentSort]);

  const filteredExecutions = useMemo(() => [...clientsByResponsible].filter((client) => {
    const createdAt = formatIsoDate(client.created_at);
    const value = Number(client.valor_servico || 0);
    if (executionFilters.status !== "Todos" && client.status_declaracao !== executionFilters.status) return false;
    if (executionFilters.dateFrom && createdAt < executionFilters.dateFrom) return false;
    if (executionFilters.dateTo && createdAt > executionFilters.dateTo) return false;
    if (executionFilters.minValue && value < Number(normalizeCurrencyInput(executionFilters.minValue) || "0")) return false;
    if (executionFilters.maxValue && value > Number(normalizeCurrencyInput(executionFilters.maxValue) || "0")) return false;
    return true;
  }).sort((a, b) => compareClients(a, b, executionSort)), [clientsByResponsible, executionFilters, executionSort]);

  const paymentPagination = useMemo(() => {
    const total = filteredPayments.length;
    const totalPages = Math.max(1, Math.ceil(total / paymentPageSize));
    const page = Math.min(paymentCurrentPage, totalPages);
    const fromIndex = (page - 1) * paymentPageSize;
    const toIndex = Math.min(fromIndex + paymentPageSize, total);
    return { list: filteredPayments.slice(fromIndex, toIndex), total, totalPages, currentPage: page, from: total ? fromIndex + 1 : 0, to: toIndex };
  }, [filteredPayments, paymentCurrentPage, paymentPageSize]);

  const unifiedPagination = useMemo(() => {
    const total = unifiedClients.length;
    const totalPages = Math.max(1, Math.ceil(total / tablePageSize));
    const page = Math.min(tableCurrentPage, totalPages);
    const fromIndex = (page - 1) * tablePageSize;
    const toIndex = Math.min(fromIndex + tablePageSize, total);
    return { list: unifiedClients.slice(fromIndex, toIndex), total, totalPages, currentPage: page, from: total ? fromIndex + 1 : 0, to: toIndex };
  }, [tableCurrentPage, tablePageSize, unifiedClients]);

  const executionPagination = useMemo(() => {
    const total = filteredExecutions.length;
    const totalPages = Math.max(1, Math.ceil(total / executionPageSize));
    const page = Math.min(executionCurrentPage, totalPages);
    const fromIndex = (page - 1) * executionPageSize;
    const toIndex = Math.min(fromIndex + executionPageSize, total);
    return { list: filteredExecutions.slice(fromIndex, toIndex), total, totalPages, currentPage: page, from: total ? fromIndex + 1 : 0, to: toIndex };
  }, [filteredExecutions, executionCurrentPage, executionPageSize]);

  const progressData = [
    { name: "Concluídos", value: executionSummary.concluido, color: "hsl(214, 84%, 56%)" },
    { name: "Pendentes", value: executionSummary.pendente, color: "hsl(38, 92%, 50%)" },
  ];
  const paymentValueData = [
    { name: "Recebido", value: paymentValueSummary.paid, color: "hsl(160, 84%, 39%)" },
    { name: "A PAGAR", value: paymentValueSummary.pending, color: "hsl(38, 92%, 50%)" },
  ];
  const completionPercent = executionSummary.total ? Math.round((executionSummary.concluido / executionSummary.total) * 100) : 0;
  const totalPaymentValue = paymentValueSummary.paid + paymentValueSummary.pending;
  const paidValuePercent = totalPaymentValue ? Math.round((paymentValueSummary.paid / totalPaymentValue) * 100) : 0;
  const progressChartData = (overviewSummary?.progressData ?? progressData).map((item, index) => ({
    ...item,
    color: index === 0 ? "hsl(214, 84%, 56%)" : "hsl(38, 92%, 50%)",
  }));
  const paymentChartData = (overviewSummary?.paymentValueData ?? paymentValueData).map((item, index) => ({
    ...item,
    color: index === 0 ? "hsl(160, 84%, 39%)" : "hsl(38, 92%, 50%)",
  }));
  const overviewCards = overviewSummary?.cards ?? {
    clientesIr: clientsByResponsible.length,
    recebidos: paymentSummary.paid,
    aPagar: paymentSummary.pending,
    concluidoPercent: completionPercent,
    concluidoTotal: executionSummary.concluido,
    clientesTotal: executionSummary.total,
    valorTotal: paymentSummary.totalValue,
  };
  const paidPercentDisplay = overviewSummary?.paidValuePercent ?? paidValuePercent;

  const handleCpfCnpjAutofill = async () => {
    const digits = onlyDigits(form.cpf_cnpj);
    if (!isValidCpfOrCnpj(digits) || digits.length !== 14 || autofillLoading) return;
    setAutofillLoading(true);
    try {
      const data = await fetchCnpjPublica(digits);
      if (data?.razao_social) {
        setForm((current) => ({ ...current, nome: current.nome.trim() || data.razao_social }));
        toast.success("Nome preenchido automaticamente pelo CNPJ.");
      }
    } catch {
      toast.error("Não foi possível consultar o CNPJ informado.");
    } finally {
      setAutofillLoading(false);
    }
  };

  const handleCreateClient = () => {
    if (createClientMutation.isPending || autofillLoading || showCreateOverlay) return;
    if (enhancedUiEnabled) {
      setShowCreateOverlay(true);
      return;
    }
    createClientMutation.mutate();
  };

  const handleDeleteClient = (client: IrClient) => {
    if (deleteClientMutation.isPending) return;
    const confirmed = window.confirm(`Deseja realmente excluir o cliente de IR "${client.nome}"?`);
    if (!confirmed) return;
    deleteClientMutation.mutate(client.id);
  };

  const handlePaymentTypeUpdate = (client: IrClient, status: IrPaymentStatus) => {
    updateClientMutation.mutate({
      id: client.id,
      updates: {
        status_pagamento: status as IrClient["status_pagamento"],
        vencimento: status !== "A PAGAR" && !client.vencimento ? getTodayIsoDate() : client.vencimento,
      },
      successMessage: "Tipo de pagamento atualizado.",
    });
  };

  const handleEditClient = (client: IrClient) => {
    const shouldUseNewResponsibleMode = !!client.responsavel_ir && !responsibleOptions.includes(client.responsavel_ir);
      setEditForm({
        id: client.id,
        nome: client.nome || "",
        cpf_cnpj: formatCpfOrCnpjInput(client.cpf_cnpj || ""),
        responsavel_ir: client.responsavel_ir || "",
        vencimento: client.vencimento || "",
        valor_servico: formatCurrency(Number(client.valor_servico || 0)),
      observacoes: client.observacoes || "",
      status_pagamento: client.status_pagamento,
    });
    setIsEditNewResponsibleMode(shouldUseNewResponsibleMode);
    setIsEditDialogOpen(true);
  };

  const handleSaveEditedClient = () => {
    const normalizedCpfCnpj = onlyDigits(editForm.cpf_cnpj);
    const valorServico = Number(normalizeCurrencyInput(editForm.valor_servico));
    if (!editForm.nome.trim() || !editForm.cpf_cnpj.trim() || Number.isNaN(valorServico)) {
      toast.error("Preencha nome, CPF/CNPJ e valor do serviço.");
      return;
    }
    if (!isValidCpfOrCnpj(normalizedCpfCnpj)) {
      toast.error("Informe um CPF ou CNPJ válido.");
      return;
    }
    if (clients.some((client) => client.id !== editForm.id && onlyDigits(client.cpf_cnpj || "") === normalizedCpfCnpj)) {
      toast.error("Já existe um cliente de IR cadastrado com este CPF/CNPJ.");
      return;
    }
    updateClientMutation.mutate({
      id: editForm.id,
      updates: {
        nome: editForm.nome,
        cpf_cnpj: editForm.cpf_cnpj,
        responsavel_ir: editForm.responsavel_ir,
        vencimento: editForm.vencimento || null,
        valor_servico: valorServico,
        observacoes: editForm.observacoes,
        status_pagamento: (editForm.vencimento ? editForm.status_pagamento : "A PAGAR") as IrClient["status_pagamento"],
      },
      successMessage: "Cliente de IR atualizado.",
    }, {
      onSuccess: () => {
        setIsEditDialogOpen(false);
        setIsEditNewResponsibleMode(false);
        setEditForm(emptyEditForm);
      },
    });
  };

  const handleOpenChargeDialog = (client: IrClient) => {
    setChargeClient(client);
    setChargeType(client.payment_charge_type ?? "PIX");
    setIsChargeDialogOpen(true);
  };

  const handleGenerateCharge = async () => {
    if (!chargeClient || generateChargeMutation.isPending) return;
    const result = await generateChargeMutation.mutateAsync({ clientId: chargeClient.id, nextChargeType: chargeType });
    if (chargeType === "PIX" && result.paymentLink) {
      window.open(result.paymentLink, "_blank", "noopener,noreferrer");
    }
  };

  const handleCopyPixLink = async () => {
    if (!chargeClient?.payment_link) return;
    await navigator.clipboard.writeText(chargeClient.payment_link);
    toast.success("Link do PIX copiado.");
  };

  const handleCopyPixCode = async () => {
    if (!chargeClient?.payment_pix_copy_paste) return;
    await navigator.clipboard.writeText(chargeClient.payment_pix_copy_paste);
    toast.success("Codigo PIX copiado.");
  };

  const handleDownloadExistingBoleto = () => {
    if (!chargeClient?.payment_boleto_pdf_base64) return;
    downloadBoletoPdf(`boleto-ir-${chargeClient.nome}.pdf`, chargeClient.payment_boleto_pdf_base64);
  };

  const handleOpenObservationDialog = (client: IrClient) => {
    setObservationClientId(client.id);
    setObservationClientName(client.nome);
    setIsObservationDialogOpen(true);
  };

  const handleSaveObservation = () => {
    if (!observationClientId) return;
    updateClientMutation.mutate({
      id: observationClientId,
      updates: { observacoes: notesDraft[observationClientId] ?? "" },
      successMessage: "Observações atualizadas.",
    }, {
      onSuccess: () => {
        setIsObservationDialogOpen(false);
        setObservationClientId("");
        setObservationClientName("");
      },
    });
  };

  return (
    <div className="space-y-6">
      {showCreateOverlay && typeof document !== "undefined" && createPortal(
        <div className="ir-create-overlay" aria-hidden="true">
          <div className="ir-create-overlay__backdrop" />
          <div className="ir-create-overlay__particle ir-create-overlay__particle--left" />
          <div className="ir-create-overlay__particle ir-create-overlay__particle--right" />
          <div className="ir-create-overlay__particle ir-create-overlay__particle--bottom" />
          <div className="ir-create-overlay__content">
            <strong className="ir-create-overlay__title">AÍ É LOUCURA!!</strong>
          </div>
        </div>,
        document.body,
      )}
      <div className="flex flex-col gap-4 xl:flex-row xl:items-end xl:justify-between">
        <div>
          <h1 className="text-2xl font-bold font-display tracking-tight">IR</h1>
          <p className="text-sm text-muted-foreground mt-1">Gestão de clientes de imposto de renda, recebimentos e execução das declarações.</p>
        </div>
        <div className="w-full max-w-sm space-y-2">
          <Label htmlFor="ir-global-responsavel">Responsável pelo IR</Label>
          <Select value={globalResponsible} onValueChange={setGlobalResponsible}>
            <SelectTrigger id="ir-global-responsavel"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="Todos">Todos</SelectItem>
              {responsibleOptions.map((responsavel) => <SelectItem key={responsavel} value={responsavel}>{responsavel}</SelectItem>)}
            </SelectContent>
          </Select>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <StatsCard title="Clientes IR" value={overviewCards.clientesIr.toString()} icon={Landmark} className="ir-stat-card ir-stat-card--neutral" description="Base ativa para acompanhamento do módulo." />
        <StatsCard title="Recebidos" value={overviewCards.recebidos.toString()} icon={Wallet} changeType="positive" className="ir-stat-card ir-stat-card--success" description="Clientes com recebimento marcado em um tipo de pagamento." />
        <StatsCard title="A PAGAR" value={overviewCards.aPagar.toString()} icon={FileClock} changeType="negative" className="ir-stat-card ir-stat-card--warning" description="Clientes sem data de recebimento ou ainda pendentes." />
        <StatsCard title="Concluídos" value={`${overviewCards.concluidoPercent}%`} icon={FileCheck} change={`${overviewCards.concluidoTotal}/${overviewCards.clientesTotal}`} changeType="positive" className="ir-stat-card ir-stat-card--info" description="Percentual total de declarações concluídas." />
      </div>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
        <GlassCard className="xl:col-span-2 p-6">
          <div className="mb-5">
            <h3 className="text-sm font-semibold font-display">Cadastro de clientes IR</h3>
            <p className="text-xs text-muted-foreground mt-1">Clientes de IR são cadastrados separadamente das empresas.</p>
          </div>
          <form className="grid grid-cols-1 md:grid-cols-2 gap-4" onSubmit={(event) => { event.preventDefault(); handleCreateClient(); }}>
            <div className="space-y-2"><Label htmlFor="ir-nome">Nome</Label><Input id="ir-nome" value={form.nome} onChange={(event) => setForm((current) => ({ ...current, nome: event.target.value }))} placeholder="Nome do cliente" /></div>
            <div className="space-y-2">
              <Label htmlFor="ir-cpf-cnpj">CPF ou CNPJ</Label>
              <Input id="ir-cpf-cnpj" value={form.cpf_cnpj} onChange={(event) => setForm((current) => ({ ...current, cpf_cnpj: formatCpfOrCnpjInput(event.target.value) }))} onKeyDown={(event) => { if (event.key === "Tab") void handleCpfCnpjAutofill(); }} onBlur={() => { if (onlyDigits(form.cpf_cnpj).length === 14) void handleCpfCnpjAutofill(); }} placeholder="000.000.000-00" />
              <p className="text-[11px] text-muted-foreground">Autopreenchimento disponível para CNPJ. Para CPF não há integração pública confiável nesta implementação.</p>
            </div>
            <div className="space-y-2">
              <Label htmlFor="ir-responsavel">Responsável pelo IR</Label>
              <Select
                value={isNewResponsibleMode ? NEW_RESPONSIBLE_VALUE : (form.responsavel_ir || "")}
                onValueChange={(value) => {
                  if (value === NEW_RESPONSIBLE_VALUE) {
                    setIsNewResponsibleMode(true);
                    setForm((current) => ({ ...current, responsavel_ir: "" }));
                    return;
                  }
                  setIsNewResponsibleMode(false);
                  setForm((current) => ({ ...current, responsavel_ir: value }));
                }}
              >
                <SelectTrigger id="ir-responsavel">
                  <SelectValue placeholder="Selecione um responsável" />
                </SelectTrigger>
                <SelectContent>
                  {responsibleOptions.map((responsavel) => <SelectItem key={responsavel} value={responsavel}>{responsavel}</SelectItem>)}
                  <SelectItem value={NEW_RESPONSIBLE_VALUE}>Cadastrar novo responsável</SelectItem>
                </SelectContent>
              </Select>
              {isNewResponsibleMode && (
                <Input
                  value={form.responsavel_ir}
                  onChange={(event) => setForm((current) => ({ ...current, responsavel_ir: event.target.value }))}
                  placeholder="Nome do novo responsável"
                />
              )}
            </div>
            <div className="space-y-2"><Label htmlFor="ir-valor">Valor do serviço</Label><Input id="ir-valor" value={form.valor_servico} onChange={(event) => setForm((current) => ({ ...current, valor_servico: formatCurrencyInput(event.target.value) }))} placeholder="R$ 150,00" inputMode="numeric" /></div>
            <div className="space-y-2">
              <Label htmlFor="ir-tipo-pagamento">Tipo de Pagamento</Label>
              <Select value={form.status_pagamento} onValueChange={(value) => setForm((current) => withPaymentTypeAndDate(current, value as IrPaymentStatus))}>
                <SelectTrigger id="ir-tipo-pagamento" className={cn("ir-status-trigger", getPaymentTriggerClass(form.status_pagamento))}>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {IR_PAYMENT_OPTIONS_WITH_CHARGES.map((status) => <SelectItem key={status} value={status}>{status}</SelectItem>)}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2"><Label htmlFor="ir-vencimento">Data de Recebimento</Label><Input id="ir-vencimento" type="date" className="ir-date-input" value={form.vencimento} onChange={(event) => setForm((current) => ({ ...current, vencimento: event.target.value }))} /></div>
            <div className="md:col-span-2 space-y-2">
              <Label htmlFor="ir-observacoes">Observação</Label>
              <ObservationTextarea
                id="ir-observacoes"
                value={form.observacoes}
                onChange={(value) => setForm((current) => ({ ...current, observacoes: value }))}
                placeholder="Digite empresa ou palavras-chave para autocomplete."
                rows={4}
                companyNames={companyNames}
              />
            </div>
            <div className="md:col-span-2 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
              <label className="inline-flex items-center gap-3 rounded-full border border-border/70 bg-background/70 px-4 py-2 text-sm text-muted-foreground shadow-sm backdrop-blur-sm">
                <input
                  type="checkbox"
                  checked={enhancedUiEnabled}
                  onChange={(event) => setEnhancedUiEnabled(event.target.checked)}
                  className="h-4 w-4 rounded border-border bg-background accent-primary"
                />
                <span>Interface Aprimorada</span>
              </label>
              <Button type="submit" disabled={createClientMutation.isPending || autofillLoading || showCreateOverlay}><Plus className="mr-2 h-4 w-4" />Cadastrar cliente</Button>
            </div>
          </form>
        </GlassCard>

        <GlassCard className="p-6">
          <h3 className="text-sm font-semibold font-display">Progresso do IR</h3>
          <p className="text-xs text-muted-foreground mt-1">Percentual de declarações concluídas e panorama de recebimento.</p>
          <div className="h-[220px] mt-4">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={progressChartData} cx="50%" cy="50%" innerRadius={54} outerRadius={82} paddingAngle={3} dataKey="value" label={({ value }) => value} labelLine={false}>
                  {progressChartData.map((item) => <Cell key={item.name} fill={item.color} />)}
                </Pie>
                <Tooltip formatter={(value: number) => [value, "clientes"]} contentStyle={{ background: "var(--ap-tooltip-bg)", color: "var(--ap-tooltip-text)", border: "1px solid var(--ap-tooltip-border)", borderRadius: "10px", fontSize: "12px" }} />
              </PieChart>
            </ResponsiveContainer>
          </div>
          <div className="space-y-2 text-xs">
            {progressChartData.map((item) => <div key={item.name} className="flex items-center justify-between"><span className="flex items-center gap-2"><span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: item.color }} />{item.name}</span><span className="font-medium">{item.value}</span></div>)}
            <div className="rounded-lg border border-border bg-muted/40 px-3 py-2 mt-4"><p className="text-[11px] text-muted-foreground">Valor total de serviços</p><p className="text-sm font-semibold mt-1">{formatCurrency(overviewCards.valorTotal)}</p></div>
            <div className="mt-5 border-t border-border pt-5">
              <div>
                <h4 className="text-sm font-semibold font-display">Fluxo Financeiro</h4>
                <p className="text-[11px] text-muted-foreground mt-1">Valor recebido versus valor ainda a receber.</p>
              </div>
              <div className="h-[220px] mt-4">
                <ResponsiveContainer width="100%" height="100%">
                  <PieChart>
                    <Pie data={paymentChartData} cx="50%" cy="50%" innerRadius={54} outerRadius={82} paddingAngle={3} dataKey="value" labelLine={false}>
                      {paymentChartData.map((item) => <Cell key={item.name} fill={item.color} />)}
                    </Pie>
                    <Tooltip formatter={(value: number) => [formatCurrency(Number(value)), "valor"]} contentStyle={{ background: "var(--ap-tooltip-bg)", color: "var(--ap-tooltip-text)", border: "1px solid var(--ap-tooltip-border)", borderRadius: "10px", fontSize: "12px" }} />
                  </PieChart>
                </ResponsiveContainer>
              </div>
              <div className="space-y-2 mt-3">
                {paymentChartData.map((item) => <div key={item.name} className="flex items-center justify-between"><span className="flex items-center gap-2"><span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: item.color }} />{item.name}</span><span className="font-medium">{formatCurrency(item.value)}</span></div>)}
                <div className="rounded-lg border border-border bg-muted/40 px-3 py-2 mt-4">
                  <p className="text-[11px] text-muted-foreground">Percentual recebido</p>
                  <p className="text-sm font-semibold mt-1">{paidPercentDisplay}%</p>
                </div>
              </div>
            </div>
          </div>
        </GlassCard>
      </div>

      <GlassCard className="overflow-visible">
        <div className="p-4 border-b border-border space-y-4">
          <div>
            <h3 className="text-sm font-semibold font-display">Clientes IR</h3>
            <p className="text-xs text-muted-foreground mt-1">Tabela única com situação financeira, execução da declaração e observações.</p>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-7 gap-3">
            <div className="space-y-1"><Label className="text-[11px]">Pesquisa</Label><Input value={tableFilters.search} onChange={(event) => { setTableFilters((current) => ({ ...current, search: event.target.value })); setTableCurrentPage(1); }} placeholder="Nome, CPF/CNPJ, responsável..." /></div>
            <div className="space-y-1"><Label className="text-[11px]">Data inicial</Label><Input type="date" className="ir-date-input" value={tableFilters.dateFrom} onChange={(event) => { setTableFilters((current) => ({ ...current, dateFrom: event.target.value })); setTableCurrentPage(1); }} /></div>
            <div className="space-y-1"><Label className="text-[11px]">Data final</Label><Input type="date" className="ir-date-input" value={tableFilters.dateTo} onChange={(event) => { setTableFilters((current) => ({ ...current, dateTo: event.target.value })); setTableCurrentPage(1); }} /></div>
            <div className="space-y-1"><Label className="text-[11px]">Valor mínimo</Label><Input value={tableFilters.minValue} onChange={(event) => { setTableFilters((current) => ({ ...current, minValue: event.target.value })); setTableCurrentPage(1); }} placeholder="0,00" /></div>
            <div className="space-y-1"><Label className="text-[11px]">Valor máximo</Label><Input value={tableFilters.maxValue} onChange={(event) => { setTableFilters((current) => ({ ...current, maxValue: event.target.value })); setTableCurrentPage(1); }} placeholder="999,99" /></div>
            <div className="space-y-1"><Label className="text-[11px]">Tipo de Pagamento</Label><Select value={tableFilters.paymentStatus} onValueChange={(value) => { setTableFilters((current) => ({ ...current, paymentStatus: value as UnifiedFilters["paymentStatus"] })); setTableCurrentPage(1); }}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent>{paymentStatusOptions.map((status) => <SelectItem key={status} value={status}>{status}</SelectItem>)}</SelectContent></Select></div>
            <div className="space-y-1"><Label className="text-[11px]">Declaração</Label><Select value={tableFilters.declarationStatus} onValueChange={(value) => { setTableFilters((current) => ({ ...current, declarationStatus: value as UnifiedFilters["declarationStatus"] })); setTableCurrentPage(1); }}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent>{executionStatusOptions.map((status) => <SelectItem key={status} value={status}>{status === "Concluido" ? "Concluído" : status}</SelectItem>)}</SelectContent></Select></div>
          </div>
        </div>
        <div className="xl:hidden grid grid-cols-1 gap-3 p-3 md:grid-cols-2">
          {unifiedPagination.list.map((client) => (
            <div key={client.id} className="ir-mobile-card">
              <div className="ir-mobile-card__header">
                <div className="min-w-0">
                  <div className="ir-mobile-card__title">{client.nome}</div>
                  <div className="ir-mobile-card__subtitle">{client.cpf_cnpj}</div>
                </div>
                <span className="ir-value-chip shrink-0">{formatCurrency(Number(client.valor_servico || 0))}</span>
              </div>

              <div className="ir-mobile-card__meta">
                <div className="ir-mobile-card__meta-item">
                  <span className="ir-mobile-card__meta-label">Responsável</span>
                  <span className="ir-mobile-card__meta-value">{client.responsavel_ir || "-"}</span>
                </div>
                <div className="ir-mobile-card__meta-item">
                  <span className="ir-mobile-card__meta-label">Data de Recebimento</span>
                  <span className="ir-date-chip">{formatDateLabel(client.vencimento)}</span>
                </div>
              </div>

              <div className="grid grid-cols-1 gap-3">
                <div className="space-y-1.5">
                  <Label className="text-[11px] text-muted-foreground">Tipo de Pagamento</Label>
                  <Select value={client.status_pagamento} onValueChange={(value) => handlePaymentTypeUpdate(client, value as IrPaymentStatus)}>
                    <SelectTrigger className={cn("w-full min-w-0 ir-status-trigger", getPaymentTriggerClass(client.status_pagamento))}><SelectValue /></SelectTrigger>
                    <SelectContent>{IR_PAYMENT_OPTIONS_WITH_CHARGES.map((status) => <SelectItem key={status} value={status}>{status}</SelectItem>)}</SelectContent>
                  </Select>
                </div>
                <div className="space-y-1.5">
                  <Label className="text-[11px] text-muted-foreground">Status da declaração</Label>
                  <Select value={client.status_declaracao} onValueChange={(value) => updateClientMutation.mutate({ id: client.id, updates: { status_declaracao: value as IrDeclarationStatus }, successMessage: "Status da declaração atualizado." })}>
                    <SelectTrigger className={cn("w-full min-w-0 ir-status-trigger", client.status_declaracao === "Concluido" ? "ir-status-trigger--info" : "ir-status-trigger--warning")}><SelectValue /></SelectTrigger>
                    <SelectContent>{executionStatusOptions.filter((status) => status !== "Todos").map((status) => <SelectItem key={status} value={status}>{status === "Concluido" ? "Concluído" : status}</SelectItem>)}</SelectContent>
                  </Select>
                </div>
              </div>

              <div className="grid grid-cols-3 gap-2">
                <Button type="button" variant="outline" className="col-span-3 w-full min-w-0 px-3" onClick={() => handleOpenChargeDialog(client)}>
                  {client.payment_charge_status === "pending" ? "Ver cobranca" : "Gerar cobranca"}
                </Button>
                <div className={cn("col-span-3 text-[11px] font-medium", getChargeStatusClass(client))}>{getChargeStatusLabel(client)}</div>
                <Button type="button" variant="outline" className="col-span-3 sm:col-span-1 w-full min-w-0 px-3" onClick={() => handleOpenObservationDialog(client)}>
                  Observacoes
                </Button>
                <Button type="button" variant="outline" className="h-10 w-full min-w-0 px-3" onClick={() => handleEditClient(client)}>
                  Editar
                </Button>
                <Button type="button" variant="outline" size="icon" className="h-10 w-full shrink-0 border-destructive/30 text-destructive hover:bg-destructive/10" onClick={() => handleDeleteClient(client)} disabled={deleteClientMutation.isPending}>
                  <Trash2 className="h-4 w-4" />
                </Button>
              </div>
            </div>
          ))}
        </div>

        <div className="hidden overflow-x-auto xl:block">
          <table className="min-w-[1480px] w-full table-fixed text-[11px]">
            <thead><tr className="border-b border-border bg-muted/50">
              <th className="w-[320px] px-3 py-3"><SortHeader label="Nome" column="nome" sort={tableSort} onToggle={(key) => setTableSort((current) => cycleSort(current, key))} /></th>
              <th className="w-[180px] px-3 py-3"><SortHeader label="Responsável" column="responsavel_ir" sort={tableSort} onToggle={(key) => setTableSort((current) => cycleSort(current, key))} /></th>
              <th className="w-[130px] px-3 py-3"><SortHeader label="Valor" column="valor_servico" sort={tableSort} onToggle={(key) => setTableSort((current) => cycleSort(current, key))} /></th>
              <th className="w-[170px] px-3 py-3"><SortHeader label="Data de Recebimento" column="vencimento" sort={tableSort} onToggle={(key) => setTableSort((current) => cycleSort(current, key))} /></th>
              <th className="w-[210px] px-3 py-3"><SortHeader label="Tipo de Pagamento" column="status_pagamento" sort={tableSort} onToggle={(key) => setTableSort((current) => cycleSort(current, key))} /></th>
              <th className="w-[210px] px-3 py-3"><SortHeader label="Declaração" column="status_declaracao" sort={tableSort} onToggle={(key) => setTableSort((current) => cycleSort(current, key))} /></th>
              <th className="w-[130px] text-left px-3 py-3 font-medium text-muted-foreground">Observações</th>
              <th className="w-[240px] text-left px-3 py-3 font-medium text-muted-foreground">Ações</th>
            </tr></thead>
            <tbody>{unifiedPagination.list.map((client) => (
              <tr key={client.id} className="ir-table-row border-b border-border hover:bg-muted/30 transition-colors">
                <td className="px-3 py-3 align-top">
                  <div className="font-medium break-words leading-snug">{client.nome}</div>
                  <div className="text-[11px] text-muted-foreground mt-1 break-all">{client.cpf_cnpj}</div>
                </td>
                <td className="px-3 py-3 text-muted-foreground align-top break-words">{client.responsavel_ir || "-"}</td>
                <td className="px-3 py-3 align-top"><span className="ir-value-chip">{formatCurrency(Number(client.valor_servico || 0))}</span></td>
                <td className="px-3 py-3 text-muted-foreground align-top"><span className="ir-date-chip">{formatDateLabel(client.vencimento)}</span></td>
                <td className="px-3 py-3 align-top">
                  <Select value={client.status_pagamento} onValueChange={(value) => handlePaymentTypeUpdate(client, value as IrPaymentStatus)}>
                    <SelectTrigger className={cn("w-full min-w-0 ir-status-trigger", getPaymentTriggerClass(client.status_pagamento))}><SelectValue /></SelectTrigger>
                    <SelectContent>{IR_PAYMENT_OPTIONS_WITH_CHARGES.map((status) => <SelectItem key={status} value={status}>{status}</SelectItem>)}</SelectContent>
                  </Select>
                </td>
                <td className="px-3 py-3 align-top">
                  <Select value={client.status_declaracao} onValueChange={(value) => updateClientMutation.mutate({ id: client.id, updates: { status_declaracao: value as IrDeclarationStatus }, successMessage: "Status da declaração atualizado." })}>
                    <SelectTrigger className={cn("w-full min-w-0 ir-status-trigger", client.status_declaracao === "Concluido" ? "ir-status-trigger--info" : "ir-status-trigger--warning")}><SelectValue /></SelectTrigger>
                    <SelectContent>{executionStatusOptions.filter((status) => status !== "Todos").map((status) => <SelectItem key={status} value={status}>{status === "Concluido" ? "Concluído" : status}</SelectItem>)}</SelectContent>
                  </Select>
                </td>
                <td className="px-3 py-3 align-top">
                  <div className="space-y-2">
                    <Button type="button" variant="outline" className="w-full min-w-[88px] px-3" onClick={() => handleOpenObservationDialog(client)}>
                      Obs.
                    </Button>
                    <p className={cn("text-[11px] font-medium leading-snug", getChargeStatusClass(client))}>{getChargeStatusLabel(client)}</p>
                  </div>
                </td>
                <td className="px-3 py-3 align-top">
                  <div className="flex items-center gap-2 whitespace-nowrap">
                    <Button type="button" variant="outline" className="h-9 min-w-[92px] px-3 shrink-0" onClick={() => handleOpenChargeDialog(client)}>
                      Cobrar
                    </Button>
                    <Button type="button" variant="outline" className="h-9 min-w-[78px] px-3 shrink-0" onClick={() => handleEditClient(client)}>
                      Editar
                    </Button>
                    <Button type="button" variant="outline" size="icon" className="h-9 w-9 shrink-0 border-destructive/30 text-destructive hover:bg-destructive/10" onClick={() => handleDeleteClient(client)} disabled={deleteClientMutation.isPending}>
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                </td>
              </tr>
            ))}</tbody>
          </table>
        </div>
        {unifiedPagination.total > 0 && <DataPagination currentPage={unifiedPagination.currentPage} totalPages={unifiedPagination.totalPages} totalItems={unifiedPagination.total} from={unifiedPagination.from} to={unifiedPagination.to} pageSize={tablePageSize} onPageChange={setTableCurrentPage} onPageSizeChange={(next) => { setTablePageSize(next); setTableCurrentPage(1); }} />}
        {!isLoading && unifiedPagination.total === 0 && <div className="p-8 text-center text-sm text-muted-foreground">Nenhum cliente encontrado com os filtros aplicados.</div>}
      </GlassCard>

      <Dialog open={isChargeDialogOpen} onOpenChange={setIsChargeDialogOpen}>
        <DialogContent className="sm:max-w-xl">
          <DialogHeader>
            <DialogTitle>Gerar cobranca IR</DialogTitle>
            <DialogDescription>{chargeClient?.nome || "Cliente IR"}</DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
              <Button type="button" variant={chargeType === "PIX" ? "default" : "outline"} className="justify-start gap-2" onClick={() => setChargeType("PIX")}>
                <QrCode className="h-4 w-4" />PIX
              </Button>
              <Button type="button" variant={chargeType === "BOLETO" ? "default" : "outline"} className="justify-start gap-2" onClick={() => setChargeType("BOLETO")}>
                <ReceiptText className="h-4 w-4" />Boleto
              </Button>
              <Button type="button" variant={chargeType === "BOLETO_HIBRIDO" ? "default" : "outline"} className="justify-start gap-2" onClick={() => setChargeType("BOLETO_HIBRIDO")}>
                <ReceiptText className="h-4 w-4" />Boleto hibrido
              </Button>
            </div>
            <div className="rounded-lg border border-border bg-muted/40 p-3 text-sm">
              <p><strong>Valor:</strong> {chargeClient ? formatCurrency(Number(chargeClient.valor_servico || 0)) : "-"}</p>
              <p className={cn("mt-1 font-medium", chargeClient ? getChargeStatusClass(chargeClient) : "text-muted-foreground")}>{chargeClient ? getChargeStatusLabel(chargeClient) : "Sem cliente selecionado"}</p>
              {chargeClient?.payment_payer_name ? <p className="mt-1 text-xs text-muted-foreground">Pagador: {chargeClient.payment_payer_name}</p> : null}
            </div>
            {chargeClient?.payment_charge_status === "pending" ? (
              <div className="space-y-3 rounded-lg border border-border p-3">
                {chargeClient.payment_charge_type === "PIX" ? (
                  <>
                    <div className="flex flex-wrap gap-2">
                      <Button type="button" variant="outline" onClick={handleCopyPixCode} disabled={!chargeClient.payment_pix_copy_paste}>
                        <Copy className="mr-2 h-4 w-4" />Copiar codigo PIX
                      </Button>
                      <Button type="button" variant="outline" onClick={handleCopyPixLink} disabled={!chargeClient.payment_link}>
                        <Link2 className="mr-2 h-4 w-4" />Copiar link PIX
                      </Button>
                    </div>
                    {chargeClient.payment_link ? <a className="text-sm text-primary-icon underline underline-offset-4 break-all hover:text-primary-icon" href={chargeClient.payment_link} target="_blank" rel="noreferrer">{chargeClient.payment_link}</a> : null}
                  </>
                ) : (
                  <>
                    <div className="flex flex-wrap gap-2">
                      <Button type="button" variant="outline" onClick={handleDownloadExistingBoleto} disabled={!chargeClient.payment_boleto_pdf_base64}>
                        <Download className="mr-2 h-4 w-4" />Baixar boleto
                      </Button>
                      {chargeClient.payment_pix_copy_paste ? (
                        <Button type="button" variant="outline" onClick={handleCopyPixCode}>
                          <Copy className="mr-2 h-4 w-4" />Copiar QR PIX
                        </Button>
                      ) : null}
                      {chargeClient.payment_link ? (
                        <Button type="button" variant="outline" onClick={handleCopyPixLink}>
                          <Link2 className="mr-2 h-4 w-4" />Copiar link
                        </Button>
                      ) : null}
                    </div>
                    {chargeClient.payment_boleto_digitable_line ? <p className="text-xs text-muted-foreground break-all">Linha digitavel: {chargeClient.payment_boleto_digitable_line}</p> : null}
                    {chargeClient.payment_charge_type === "BOLETO_HIBRIDO" && chargeClient.payment_pix_copy_paste ? (
                      <p className="text-xs text-muted-foreground break-all">QR PIX disponivel junto com o boleto.</p>
                    ) : null}
                  </>
                )}
              </div>
            ) : null}
          </div>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setIsChargeDialogOpen(false)}>Fechar</Button>
            <Button type="button" onClick={handleGenerateCharge} disabled={!chargeClient || generateChargeMutation.isPending}>
              {generateChargeMutation.isPending ? "Gerando..." : chargeType === "PIX" ? "Gerar PIX" : chargeType === "BOLETO_HIBRIDO" ? "Gerar boleto hibrido" : "Gerar boleto"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={isEditDialogOpen} onOpenChange={setIsEditDialogOpen}>
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Editar Cliente IR</DialogTitle>
            <DialogDescription>Atualize os dados principais do cliente já cadastrado.</DialogDescription>
          </DialogHeader>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="ir-edit-nome">Nome</Label>
              <Input id="ir-edit-nome" value={editForm.nome} onChange={(event) => setEditForm((current) => ({ ...current, nome: event.target.value }))} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="ir-edit-cpf-cnpj">CPF ou CNPJ</Label>
              <Input id="ir-edit-cpf-cnpj" value={editForm.cpf_cnpj} onChange={(event) => setEditForm((current) => ({ ...current, cpf_cnpj: formatCpfOrCnpjInput(event.target.value) }))} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="ir-edit-responsavel">Responsável pelo IR</Label>
              <Select
                value={isEditNewResponsibleMode ? NEW_RESPONSIBLE_VALUE : (editForm.responsavel_ir || "")}
                onValueChange={(value) => {
                  if (value === NEW_RESPONSIBLE_VALUE) {
                    setIsEditNewResponsibleMode(true);
                    setEditForm((current) => ({ ...current, responsavel_ir: "" }));
                    return;
                  }
                  setIsEditNewResponsibleMode(false);
                  setEditForm((current) => ({ ...current, responsavel_ir: value }));
                }}
              >
                <SelectTrigger id="ir-edit-responsavel">
                  <SelectValue placeholder="Selecione um responsável" />
                </SelectTrigger>
                <SelectContent>
                  {responsibleOptions.map((responsavel) => <SelectItem key={responsavel} value={responsavel}>{responsavel}</SelectItem>)}
                  <SelectItem value={NEW_RESPONSIBLE_VALUE}>Cadastrar novo responsável</SelectItem>
                </SelectContent>
              </Select>
              {isEditNewResponsibleMode && (
                <Input
                  value={editForm.responsavel_ir}
                  onChange={(event) => setEditForm((current) => ({ ...current, responsavel_ir: event.target.value }))}
                  placeholder="Nome do novo responsável"
                />
              )}
            </div>
            <div className="space-y-2">
              <Label htmlFor="ir-edit-valor">Valor do serviço</Label>
              <Input id="ir-edit-valor" value={editForm.valor_servico} onChange={(event) => setEditForm((current) => ({ ...current, valor_servico: formatCurrencyInput(event.target.value) }))} inputMode="numeric" />
            </div>
            <div className="space-y-2">
              <Label htmlFor="ir-edit-tipo-pagamento">Tipo de Pagamento</Label>
              <Select value={editForm.status_pagamento} onValueChange={(value) => setEditForm((current) => withPaymentTypeAndDate(current, value as IrPaymentStatus))}>
                <SelectTrigger id="ir-edit-tipo-pagamento" className={cn("ir-status-trigger", getPaymentTriggerClass(editForm.status_pagamento))}>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {IR_PAYMENT_OPTIONS_WITH_CHARGES.map((status) => <SelectItem key={status} value={status}>{status}</SelectItem>)}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="ir-edit-vencimento">Data de Recebimento</Label>
              <Input id="ir-edit-vencimento" type="date" className="ir-date-input" value={editForm.vencimento} onChange={(event) => setEditForm((current) => ({ ...current, vencimento: event.target.value }))} />
            </div>
            <div className="md:col-span-2 space-y-2">
              <Label htmlFor="ir-edit-observacoes">Observação</Label>
              <ObservationTextarea
                id="ir-edit-observacoes"
                value={editForm.observacoes}
                onChange={(value) => setEditForm((current) => ({ ...current, observacoes: value }))}
                rows={4}
                companyNames={companyNames}
              />
            </div>
          </div>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setIsEditDialogOpen(false)}>Cancelar</Button>
            <Button type="button" onClick={handleSaveEditedClient} disabled={updateClientMutation.isPending}>Salvar alterações</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={isObservationDialogOpen} onOpenChange={setIsObservationDialogOpen}>
        <DialogContent className="sm:max-w-xl">
          <DialogHeader>
            <DialogTitle>Observações</DialogTitle>
            <DialogDescription>{observationClientName || "Cliente IR"}</DialogDescription>
          </DialogHeader>
          <div className="space-y-2">
            <Label htmlFor="ir-observation-modal">Anotações do cliente</Label>
            <ObservationTextarea
              id="ir-observation-modal"
              value={notesDraft[observationClientId] ?? ""}
              onChange={(value) => setNotesDraft((current) => ({ ...current, [observationClientId]: value }))}
              rows={8}
              companyNames={companyNames}
            />
          </div>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setIsObservationDialogOpen(false)}>Fechar</Button>
            <Button type="button" onClick={handleSaveObservation} disabled={updateClientMutation.isPending}>Salvar observações</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
