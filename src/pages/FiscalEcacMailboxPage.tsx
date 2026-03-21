import { useMemo, useState } from "react";
import { keepPreviousData, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Archive, BellRing, CheckCheck, Inbox, MailWarning, RefreshCw } from "lucide-react";
import { toast } from "sonner";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatsCard } from "@/components/dashboard/StatsCard";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
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

function formatDateTime(value: string | null) {
  if (!value) return "—";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return dateTimeFormatter.format(parsed);
}

function getPayloadPreview(payload: EcacMailboxMessage["payload"]) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) return null;
  const record = payload as Record<string, unknown>;
  const candidateKeys = ["preview", "message", "content", "description", "descricao", "body", "texto"];

  for (const key of candidateKeys) {
    const value = record[key];
    if (typeof value === "string" && value.trim()) {
      return value.trim();
    }
  }

  return null;
}

function MessageStatusBadge({ message }: { message: EcacMailboxMessage }) {
  if (!message.isRead) {
    return <Badge className="border-amber-500/30 bg-amber-500/15 text-amber-200 hover:bg-amber-500/15">Nova</Badge>;
  }

  if (message.status === "arquivado") {
    return <Badge variant="outline" className="border-border bg-background/50">Arquivada</Badge>;
  }

  return <Badge variant="secondary">Lida</Badge>;
}

export default function FiscalEcacMailboxPage() {
  const queryClient = useQueryClient();
  const { selectedCompanyIds } = useSelectedCompanyIds();
  const companyFilter = selectedCompanyIds.length > 0 ? selectedCompanyIds : null;
  const [search, setSearch] = useState("");
  const [status, setStatus] = useState<EcacMailboxStatusFilter>("all");

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

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div>
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
            Mensagens capturadas pelo robô da caixa postal da Receita. O painel sinaliza novidades enquanto existirem itens não lidos.
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
          <div className="text-xs text-muted-foreground">
            {messages.length} mensagem(ns) exibida(s)
          </div>
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
        <div className="space-y-4">
          {messages.map((message) => {
            const preview = getPayloadPreview(message.payload);

            return (
              <GlassCard
                key={message.id}
                className={message.isRead ? "p-5" : "border border-amber-500/30 bg-amber-500/5 p-5"}
              >
                <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
                  <div className="min-w-0 space-y-3">
                    <div className="flex flex-wrap items-center gap-2">
                      <MessageStatusBadge message={message} />
                      {message.category ? <Badge variant="outline">{message.category}</Badge> : null}
                      <span className="text-xs text-muted-foreground">{formatDateTime(message.receivedAt)}</span>
                    </div>

                    <div>
                      <h3 className="text-base font-semibold font-display text-foreground">{message.subject}</h3>
                      <p className="mt-1 text-sm text-muted-foreground">
                        {message.companyName}
                        {message.companyDocument ? ` • ${message.companyDocument}` : ""}
                      </p>
                    </div>

                    <div className="grid grid-cols-1 gap-2 text-xs text-muted-foreground md:grid-cols-3">
                      <div>
                        <span className="font-medium text-foreground">Remetente:</span>{" "}
                        {message.senderName || "Receita Federal"}
                      </div>
                      <div>
                        <span className="font-medium text-foreground">Protocolo:</span>{" "}
                        {message.externalMessageId}
                      </div>
                      <div>
                        <span className="font-medium text-foreground">Capturada em:</span>{" "}
                        {formatDateTime(message.fetchedAt)}
                      </div>
                    </div>

                    {preview ? (
                      <div className="rounded-xl border border-border bg-background/50 px-4 py-3 text-sm text-muted-foreground">
                        {preview}
                      </div>
                    ) : null}
                  </div>

                  <div className="flex flex-wrap items-center gap-2 xl:justify-end">
                    {!message.isRead ? (
                      <Button
                        variant="outline"
                        size="sm"
                        className="gap-2"
                        onClick={() => markOneAsReadMutation.mutate(message.id)}
                        disabled={markOneAsReadMutation.isPending}
                      >
                        <CheckCheck className="h-4 w-4" />
                        Marcar como lida
                      </Button>
                    ) : null}
                    <Button
                      variant="outline"
                      size="sm"
                      className="gap-2"
                      onClick={() => archiveMessageMutation.mutate(message.id)}
                      disabled={archiveMessageMutation.isPending || message.status === "arquivado"}
                    >
                      <Archive className="h-4 w-4" />
                      Arquivar
                    </Button>
                  </div>
                </div>
              </GlassCard>
            );
          })}
        </div>
      )}
    </div>
  );
}
