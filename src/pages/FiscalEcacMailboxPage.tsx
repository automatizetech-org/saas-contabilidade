import { useMemo, useState } from "react";
import { keepPreviousData, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Archive, BellRing, CheckCheck, Inbox, MailWarning, Paperclip, RefreshCw } from "lucide-react";
import { toast } from "sonner";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatsCard } from "@/components/dashboard/StatsCard";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useSelectedCompanyIds } from "@/hooks/useSelectedCompanies";
import { getVisibilityAwareRefetchInterval } from "@/lib/queryPolling";
import {
  archiveEcacMailboxMessage,
  getEcacMailboxMessages,
  getEcacMailboxMessagesQueryKey,
  getEcacMailboxSummary,
  getEcacMailboxSummaryQueryKey,
  markEcacMailboxMessagesAsRead,
  type EcacMailboxMessage,
  type EcacMailboxStatusFilter,
} from "@/services/ecacMailboxService";

const dateTimeFormatter = new Intl.DateTimeFormat("pt-BR", {
  dateStyle: "short",
  timeStyle: "short",
});

type ParsedField = { label: string; value: string };

const MODAL_HEADER_FIELDS = [
  "Remetente",
  "Destinatário",
  "ID da Mensagem",
  "Tipo de Comunicação",
  "Enviada em",
  "Exibição até",
];

const FIELD_PATTERNS: Array<{ label: string; pattern: RegExp }> = [
  { label: "Remetente", pattern: /Remetente:\s*(.+?)(?=\s+Destinatário:|\s+ID da Mensagem:|$)/i },
  { label: "Destinatário", pattern: /Destinatário:\s*(.+?)(?=\s+ID da Mensagem:|\s+Tipo de Comunicação:|$)/i },
  { label: "ID da Mensagem", pattern: /ID da Mensagem:\s*([0-9./-]+)/i },
  { label: "Tipo de Comunicação", pattern: /Tipo de Comunicação:\s*(.+?)(?=\s+Enviada em:|\s+Exibição até:|$)/i },
  { label: "Enviada em", pattern: /Enviada em:\s*([0-9/:-\s]+)/i },
  { label: "Exibição até", pattern: /Exibição até:\s*([0-9/:-\s]+)/i },
  { label: "Protocolo", pattern: /Protocolo:\s*([0-9./-]+)/i },
  { label: "Número do Processo/Procedimento", pattern: /Número do Processo\/Procedimento:\s*([0-9./-]+)/i },
  { label: "Número", pattern: /Número:\s*([0-9./-]+)/i },
  { label: "Interessado", pattern: /Interessado:\s*(.+?)(?=\s+Solicitante:|\s+Área de Concentração do Serviço:|$)/i },
  { label: "Solicitante", pattern: /Solicitante:\s*(.+?)(?=\s+Relação do Solicitante com o Processo:|\s+Responsável pelo Envio:|$)/i },
  { label: "Relação do Solicitante com o Processo", pattern: /Relação do Solicitante com o Processo:\s*(.+?)(?=\s+Responsável pelo Envio:|\s+Papel do Responsável pelo Envio:|$)/i },
  { label: "Responsável pelo Envio", pattern: /Responsável pelo Envio:\s*(.+?)(?=\s+Papel do Responsável pelo Envio:|\s+Data e Hora em que a solicitação foi transmitida:|$)/i },
  { label: "Papel do Responsável pelo Envio", pattern: /Papel do Responsável pelo Envio:\s*(.+?)(?=\s+Data e Hora em que a solicitação foi transmitida:|\s+Identificador do Envio:|$)/i },
  { label: "Data e Hora em que a solicitação foi transmitida", pattern: /Data e Hora em que a solicitação foi transmitida:\s*(.+?)(?=\s+Identificador do Envio:|$)/i },
  { label: "Identificador do Envio", pattern: /Identificador do Envio:\s*(.+?)(?=\s+Foi gerado|\s+O termo pode ser consultado|$)/i },
];

function formatDateTime(value: string | null) {
  if (!value) return "—";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return dateTimeFormatter.format(parsed);
}

function formatShortDate(value: string | null) {
  if (!value) return "—";
  const parsed = new Date(value);
  if (!Number.isNaN(parsed.getTime())) {
    return parsed.toLocaleDateString("pt-BR");
  }
  return value;
}

function getPayloadRecord(payload: EcacMailboxMessage["payload"]) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return null;
  return payload as Record<string, unknown>;
}

function getPayloadText(payload: EcacMailboxMessage["payload"], keys: string[]) {
  const record = getPayloadRecord(payload);
  if (!record) return "";
  for (const key of keys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }
  return "";
}

function normalizeCompactText(text: string) {
  return String(text || "").replace(/\s+/g, " ").trim();
}

function compactSenderName(value: string | null) {
  const text = normalizeCompactText(value || "");
  if (!text) return "Receita Federal do Brasil";
  const cleaned = text.split(/Destinatário:|ID da Mensagem:|Tipo de Comunicação:/i)[0]?.trim();
  return cleaned || text;
}

function compactCompanyLabel(name: string, document: string | null) {
  return document ? `${name} • ${document}` : name;
}

function parseAttachments(message: EcacMailboxMessage) {
  const record = getPayloadRecord(message.payload);
  const raw = record?.attachments;
  if (!Array.isArray(raw)) return [] as string[];
  return raw
    .map((item) => (item && typeof item === "object" ? (item as Record<string, unknown>).name : ""))
    .filter((item): item is string => typeof item === "string" && item.trim().length > 0);
}

function parseMessageContent(message: EcacMailboxMessage) {
  const raw = normalizeCompactText(
    getPayloadText(message.payload, ["body", "detail_visible_text", "preview", "texto"]) || message.subject,
  );

  const fields = FIELD_PATTERNS.map((item) => {
    const match = raw.match(item.pattern);
    return { label: item.label, value: match?.[1]?.trim() ?? "" };
  }).filter((item) => item.value) as ParsedField[];

  const modalHeaderFields = MODAL_HEADER_FIELDS.map((label) => ({
    label,
    value: fields.find((field) => field.label === label)?.value ?? "",
  })).filter((item) => item.value);

  const secondaryFields = fields.filter((field) => !MODAL_HEADER_FIELDS.includes(field.label));

  let body = raw
    .replace(/^Lista de mensagens recebidas.*?Excluir\s*/i, "")
    .replace(/\s+Dados da Primeira Leitura.*$/i, "")
    .replace(/\s+(Prezado\(a\)|Prezados\(as\)|Prezado|Prezados|ATENÇÃO|Importante:)/g, "\n\n$1")
    .replace(/\s+(Justificativa:|Protocolo:|Número do Processo\/Procedimento:|Número:|Interessado:|Solicitante:|Relação do Solicitante com o Processo:|Responsável pelo Envio:|Papel do Responsável pelo Envio:|Data e Hora em que a solicitação foi transmitida:|Identificador do Envio:)/g, "\n$1")
    .replace(/\s+(- Portal e-CAC \/)/g, "\n$1")
    .replace(/\s+(SECRETARIA ESPECIAL DA RECEITA FEDERAL DO BRASIL)/g, "\n\n$1")
    .trim();

  for (const field of fields) {
    const escapedLabel = field.label.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    const escapedValue = field.value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    body = body.replace(new RegExp(`${escapedLabel}:\\s*${escapedValue}`, "i"), "");
  }

  body = body.replace(/\n{3,}/g, "\n\n").trim();

  const paragraphs = body
    .split(/\n{2,}/)
    .map((item) => item.trim())
    .filter(Boolean);

  return {
    modalHeaderFields,
    secondaryFields,
    paragraphs,
    attachments: parseAttachments(message),
    senderName: compactSenderName(message.senderName),
  };
}

function RowStatus({ message }: { message: EcacMailboxMessage }) {
  if (!message.isRead) {
    return <span className="inline-block h-2.5 w-2.5 rounded-full bg-amber-400" />;
  }

  if (message.status === "arquivado") {
    return <span className="inline-block h-2.5 w-2.5 rounded-full bg-slate-500" />;
  }

  return <span className="inline-block h-2.5 w-2.5 rounded-full bg-emerald-400" />;
}

export default function FiscalEcacMailboxPage() {
  const queryClient = useQueryClient();
  const { selectedCompanyIds } = useSelectedCompanyIds();
  const companyFilter = selectedCompanyIds.length > 0 ? selectedCompanyIds : null;
  const [search, setSearch] = useState("");
  const [status, setStatus] = useState<EcacMailboxStatusFilter>("all");
  const [selectedMessage, setSelectedMessage] = useState<EcacMailboxMessage | null>(null);

  const summaryQuery = useQuery({
    queryKey: getEcacMailboxSummaryQueryKey(companyFilter),
    queryFn: () => getEcacMailboxSummary(companyFilter),
    placeholderData: keepPreviousData,
    refetchInterval: () => getVisibilityAwareRefetchInterval(),
    refetchIntervalInBackground: true,
  });

  const messagesQuery = useQuery({
    queryKey: getEcacMailboxMessagesQueryKey(companyFilter, search, status),
    queryFn: () => getEcacMailboxMessages({ companyIds: companyFilter, search, status, limit: 150 }),
    placeholderData: keepPreviousData,
    refetchInterval: () => getVisibilityAwareRefetchInterval(),
    refetchIntervalInBackground: true,
  });

  const unreadVisibleIds = useMemo(
    () => (messagesQuery.data ?? []).filter((item) => !item.isRead).map((item) => item.id),
    [messagesQuery.data],
  );

  const refreshMailboxQueries = async () => {
    await Promise.all([
      queryClient.invalidateQueries({ queryKey: ["ecac-mailbox-summary"] }),
      queryClient.invalidateQueries({ queryKey: ["ecac-mailbox-messages"] }),
    ]);
  };

  const markAllAsReadMutation = useMutation({
    mutationFn: () => markEcacMailboxMessagesAsRead(unreadVisibleIds),
    onSuccess: async () => {
      await refreshMailboxQueries();
      toast.success("Mensagens marcadas como lidas.");
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Não foi possível marcar as mensagens como lidas.");
    },
  });

  const markOneAsReadMutation = useMutation({
    mutationFn: (id: string) => markEcacMailboxMessagesAsRead([id]),
    onSuccess: async () => {
      await refreshMailboxQueries();
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Não foi possível marcar a mensagem como lida.");
    },
  });

  const archiveMessageMutation = useMutation({
    mutationFn: (id: string) => archiveEcacMailboxMessage(id),
    onSuccess: async () => {
      await refreshMailboxQueries();
      toast.success("Mensagem arquivada.");
    },
    onError: (error) => {
      toast.error(error instanceof Error ? error.message : "Não foi possível arquivar a mensagem.");
    },
  });

  const summary = summaryQuery.data ?? {
    totalMessages: 0,
    unreadMessages: 0,
    messagesToday: 0,
    lastReceivedAt: null,
  };

  const messages = messagesQuery.data ?? [];
  const selectedContent = selectedMessage ? parseMessageContent(selectedMessage) : null;

  const openMessage = (message: EcacMailboxMessage) => {
    setSelectedMessage(message);
    if (!message.isRead) {
      markOneAsReadMutation.mutate(message.id);
    }
  };

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="max-w-3xl">
          <div className="flex flex-wrap items-center gap-2">
            <h1 className="text-2xl font-bold font-display tracking-tight">Caixa Postal E-CAC</h1>
            {summary.unreadMessages > 0 ? (
              <Badge className="border-amber-500/30 bg-amber-500/15 text-amber-200 hover:bg-amber-500/15">
                {summary.unreadMessages} nova{summary.unreadMessages > 1 ? "s" : ""}
              </Badge>
            ) : (
              <Badge variant="secondary">Sem novidades</Badge>
            )}
          </div>
          <p className="mt-1 text-sm text-muted-foreground">
            A listagem segue a lógica visual do e-CAC: uma linha por mensagem e leitura detalhada em modal.
          </p>
        </div>

        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            className="gap-2"
            onClick={() => refreshMailboxQueries()}
            disabled={summaryQuery.isFetching || messagesQuery.isFetching}
          >
            <RefreshCw className="h-4 w-4" />
            Atualizar
          </Button>
          <Button
            size="sm"
            className="gap-2"
            onClick={() => markAllAsReadMutation.mutate()}
            disabled={unreadVisibleIds.length === 0 || markAllAsReadMutation.isPending}
          >
            <CheckCheck className="h-4 w-4" />
            Marcar visíveis como lidas
          </Button>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
        <StatsCard title="Não lidas" value={summary.unreadMessages} icon={BellRing} />
        <StatsCard title="Total capturado" value={summary.totalMessages} icon={Inbox} />
        <StatsCard title="Chegaram hoje" value={summary.messagesToday} icon={MailWarning} />
        <StatsCard title="Última captura" value={formatDateTime(summary.lastReceivedAt)} icon={RefreshCw} animateValue={false} />
      </div>

      <GlassCard className="p-4">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
          <div className="flex flex-1 flex-col gap-3 sm:flex-row">
            <Input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Buscar por empresa, assunto, remetente ou protocolo..."
              className="sm:max-w-md"
            />
            <Select value={status} onValueChange={(value) => setStatus(value as EcacMailboxStatusFilter)}>
              <SelectTrigger className="sm:w-[220px]">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="all">Todas</SelectItem>
                <SelectItem value="unread">Somente novas</SelectItem>
                <SelectItem value="read">Lidas</SelectItem>
                <SelectItem value="archived">Arquivadas</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="text-xs text-muted-foreground">{messages.length} mensagem(ns) exibida(s)</div>
        </div>
      </GlassCard>

      {messagesQuery.isLoading && messages.length === 0 ? (
        <GlassCard className="p-8 text-center text-sm text-muted-foreground">
          Carregando caixa postal...
        </GlassCard>
      ) : messages.length === 0 ? (
        <GlassCard className="p-8 text-center text-sm text-muted-foreground">
          Nenhuma mensagem encontrada para os filtros atuais.
        </GlassCard>
      ) : (
        <GlassCard className="overflow-hidden p-0">
          <Table className="min-w-[1200px]">
            <TableHeader className="bg-background/50">
              <TableRow className="hover:bg-transparent">
                <TableHead className="w-[40px]" />
                <TableHead className="w-[180px]">Remetente</TableHead>
                <TableHead>Assunto</TableHead>
                <TableHead className="w-[120px]">Enviada em</TableHead>
                <TableHead className="w-[120px]">Exibição até</TableHead>
                <TableHead className="w-[120px]">Data de 1ª leitura</TableHead>
                <TableHead className="w-[150px]">Destinatário</TableHead>
                <TableHead className="w-[120px]">ID Mensagem</TableHead>
                <TableHead className="w-[150px]">Tipo de Comunicação</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {messages.map((message) => {
                const parsed = parseMessageContent(message);
                const sentAt = parsed.modalHeaderFields.find((field) => field.label === "Enviada em")?.value ?? formatShortDate(message.receivedAt);
                const visibleUntil = parsed.modalHeaderFields.find((field) => field.label === "Exibição até")?.value ?? "—";
                const recipient = parsed.modalHeaderFields.find((field) => field.label === "Destinatário")?.value ?? "—";
                const communicationType = parsed.modalHeaderFields.find((field) => field.label === "Tipo de Comunicação")?.value ?? message.category ?? "—";

                return (
                  <TableRow
                    key={message.id}
                    className="cursor-pointer hover:bg-background/40"
                    onClick={() => openMessage(message)}
                  >
                    <TableCell className="align-top">
                      <RowStatus message={message} />
                    </TableCell>
                    <TableCell className="align-top">
                      <div className="text-sm text-primary">{compactSenderName(message.senderName)}</div>
                    </TableCell>
                    <TableCell className="align-top">
                      <div className="text-sm text-primary">{message.subject}</div>
                    </TableCell>
                    <TableCell className="align-top text-sm">{sentAt}</TableCell>
                    <TableCell className="align-top text-sm">{visibleUntil}</TableCell>
                    <TableCell className="align-top text-sm">{formatShortDate(message.readAt || null)}</TableCell>
                    <TableCell className="align-top text-sm">{recipient}</TableCell>
                    <TableCell className="align-top text-sm">{message.externalMessageId}</TableCell>
                    <TableCell className="align-top text-sm">{communicationType}</TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </GlassCard>
      )}

      <Dialog open={Boolean(selectedMessage)} onOpenChange={(open) => !open && setSelectedMessage(null)}>
        <DialogContent className="max-h-[92vh] max-w-[1600px] overflow-hidden border-border bg-background p-0">
          {selectedMessage && selectedContent ? (
            <div className="flex max-h-[92vh] flex-col">
              <DialogHeader className="border-b border-border/70 bg-background/70 px-6 py-4">
                <div className="flex items-center justify-between gap-4">
                  <div className="min-w-0">
                    <DialogTitle className="truncate text-base font-medium text-foreground">
                      {selectedMessage.subject}
                    </DialogTitle>
                    <DialogDescription className="mt-1 text-xs">
                      {compactCompanyLabel(selectedMessage.companyName, selectedMessage.companyDocument)}
                    </DialogDescription>
                  </div>
                  <div className="flex items-center gap-2">
                    <Badge variant="outline">{selectedMessage.isRead ? "Lida" : "Nova"}</Badge>
                    <Button
                      variant="outline"
                      size="sm"
                      className="gap-2"
                      onClick={() => archiveMessageMutation.mutate(selectedMessage.id)}
                      disabled={archiveMessageMutation.isPending || selectedMessage.status === "arquivado"}
                    >
                      <Archive className="h-4 w-4" />
                      Arquivar
                    </Button>
                  </div>
                </div>
              </DialogHeader>

              <div className="overflow-y-auto px-6 py-5">
                <div className="grid gap-x-8 gap-y-4 border-b border-border/60 pb-5 md:grid-cols-3 xl:grid-cols-6">
                  {selectedContent.modalHeaderFields.map((field) => (
                    <div key={field.label} className="min-w-0">
                      <div className="text-xs font-semibold text-primary">{field.label}</div>
                      <div className="mt-1 break-words text-sm text-foreground">{field.value}</div>
                    </div>
                  ))}
                </div>

                <div className="space-y-6 py-5">
                  {selectedContent.paragraphs.map((paragraph, index) => (
                    <p key={`${selectedMessage.id}-${index}`} className="whitespace-pre-wrap text-[15px] leading-8 text-foreground">
                      {paragraph}
                    </p>
                  ))}
                </div>

                {selectedContent.secondaryFields.length > 0 ? (
                  <div className="space-y-3 border-t border-border/60 pt-5">
                    {selectedContent.secondaryFields.map((field) => (
                      <div key={field.label} className="text-[15px] leading-8 text-foreground">
                        <span className="font-medium">{field.label}:</span> {field.value}
                      </div>
                    ))}
                  </div>
                ) : null}

                {selectedContent.attachments.length > 0 ? (
                  <div className="space-y-3 border-t border-border/60 pt-5">
                    <div className="text-xs font-semibold uppercase tracking-[0.16em] text-muted-foreground">
                      Anexos
                    </div>
                    {selectedContent.attachments.map((attachment) => (
                      <div key={attachment} className="flex items-center gap-2 text-sm text-foreground">
                        <Paperclip className="h-4 w-4 text-muted-foreground" />
                        <span>{attachment}</span>
                      </div>
                    ))}
                  </div>
                ) : null}
              </div>
            </div>
          ) : null}
        </DialogContent>
      </Dialog>
    </div>
  );
}
