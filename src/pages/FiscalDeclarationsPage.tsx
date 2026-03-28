import { useEffect, useMemo, useState } from "react";
import { keepPreviousData, useQueries, useQuery } from "@tanstack/react-query";
import {
  BadgeAlert,
  FileArchive,
  Loader2,
  ReceiptText,
  ShieldCheck,
} from "lucide-react";
import { toast } from "sonner";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useCompanies } from "@/hooks/useCompanies";
import { useProfile } from "@/hooks/useProfile";
import { useSelectedCompanyIds } from "@/hooks/useSelectedCompanies";
import { cancelExecutionRequest } from "@/services/executionRequestsService";
import { downloadServerFileByPath } from "@/services/serverFileService";
import { cn } from "@/utils";
import {
  getDefaultDeclarationCompetence,
  isValidCompetence,
  sanitizeDeclarationError,
} from "@/features/fiscal-declaracoes/helpers";
import {
  buildDeclarationRunHistoryEntries,
  downloadDeclarationRunHistoryZip,
  downloadDeclarationArtifact,
  deleteAllDeclarationRunHistory,
  hydrateDeclarationRunArtifacts,
  getDeclarationRunState,
  getFiscalDeclarationsBootstrap,
  listAllDeclarationRunHistoryRuns,
  listDeclarationArtifacts,
  listDeclarationRunHistory,
  persistDeclarationRunState,
  startDeclarationRun,
  stopDeclarationRobotRuntime,
} from "@/features/fiscal-declaracoes/service";
import type {
  DeclarationActionKind,
  DeclarationArtifactListItem,
  DeclarationGuideModalState,
  DeclarationRunHistoryEntry,
  DeclarationRunItem,
  DeclarationRunState,
  DeclarationStoredDocumentsModalState,
} from "@/features/fiscal-declaracoes/types";
import { DeclarationActionCard } from "@/features/fiscal-declaracoes/components/DeclarationActionCard";
import { DeclarationArtifactsCard } from "@/features/fiscal-declaracoes/components/DeclarationArtifactsCard";
import { DeclarationExecutionModal } from "@/features/fiscal-declaracoes/components/DeclarationExecutionModal";
import { DeclarationRunHistoryTable } from "@/features/fiscal-declaracoes/components/DeclarationRunHistoryTable";
import { DeclarationStoredDocumentsModal } from "@/features/fiscal-declaracoes/components/DeclarationStoredDocumentsModal";
import { OverdueGuidesCard } from "@/features/fiscal-declaracoes/components/OverdueGuidesCard";

function upsertRunHistory(
  current: DeclarationRunState[],
  incoming: DeclarationRunState,
): DeclarationRunState[] {
  const next = [incoming, ...current.filter((run) => run.runId !== incoming.runId)];
  return next.sort((left, right) => Date.parse(right.startedAt) - Date.parse(left.startedAt));
}

function syncVisibleRunHistoryEntries(
  runs: DeclarationRunState[],
  currentEntries: DeclarationRunHistoryEntry[],
): DeclarationRunHistoryEntry[] {
  const safeRuns = Array.isArray(runs) ? runs : [];
  const safeEntries = Array.isArray(currentEntries) ? currentEntries : [];
  const entriesById = new Map(
    buildDeclarationRunHistoryEntries(safeRuns).map((entry) => [entry.entryId, entry] as const),
  );
  return safeEntries.map((entry) => entriesById.get(entry.entryId) ?? entry);
}

function normalizeRunHistoryPayload(
  payload: unknown,
): {
  runs: DeclarationRunState[];
  entries: DeclarationRunHistoryEntry[];
  totalEntries: number;
} {
  const data = payload && typeof payload === "object" ? (payload as Record<string, unknown>) : null;
  const runs = Array.isArray(data?.runs)
    ? (data.runs as DeclarationRunState[])
    : Array.isArray(data?.items)
      ? (data.items as DeclarationRunState[])
      : [];
  const entries = Array.isArray(data?.entries)
    ? (data.entries as DeclarationRunHistoryEntry[])
    : buildDeclarationRunHistoryEntries(runs);
  const totalEntries =
    typeof data?.totalEntries === "number" && Number.isFinite(data.totalEntries)
      ? data.totalEntries
      : entries.length;
  return { runs, entries, totalEntries };
}

function hasEntryArtifact(entry: DeclarationRunHistoryEntry | DeclarationRunItem) {
  return Boolean(entry.artifact?.filePath || entry.artifact?.url || entry.artifact?.artifactKey);
}

function runNeedsArtifactResolution(run: DeclarationRunState) {
  return run.items.some((item) => item.status === "sucesso" && !hasEntryArtifact(item));
}

const initialGuideModalState: DeclarationGuideModalState = {
  open: false,
  action: "simples_emitir_guia",
  source: "card",
  presetCompanyId: null,
  presetCompetence: null,
  presetDueDate: null,
  recalculateByDefault: false,
};

const initialStoredDocumentsModalState: DeclarationStoredDocumentsModalState = {
  open: false,
  action: "simples_extrato",
  presetCompanyId: null,
  presetYear: null,
};

const SIMPLES_DOCUMENT_ACTIONS: Array<{
  action: DeclarationActionKind;
  title: string;
  description: string;
}> = [
  {
    action: "simples_emitir_guia",
    title: "Emitir Guia",
    description: "Abre o fluxo de emissao ou recalculo do DAS conforme os dados informados no modal.",
  },
  {
    action: "simples_extrato",
    title: "Extrato do Simples Nacional",
    description: "Abre o modal com os extratos salvos por ano e por competencia, no mesmo padrao visual do e-CAC.",
  },
  {
    action: "simples_defis",
    title: "DEFIS",
    description: "Abre o modal com recibos e declaracoes anuais ja salvos para a empresa selecionada.",
  },
];

const MEI_DOCUMENT_ACTIONS: Array<{
  action: DeclarationActionKind;
  title: string;
  description: string;
}> = [
  {
    action: "mei_declaracao_anual",
    title: "Documentos da Declaracao Anual",
    description: "Prepara a leitura dos arquivos anuais do MEI pela mesma regra central de base path e estrutura do SaaS.",
  },
  {
    action: "mei_guias_mensais",
    title: "Documentos de Guias Mensais",
    description: "Lista guias e comprovantes do MEI sem expor caminhos arbitrarios para o frontend.",
  },
];

export default function FiscalDeclarationsPage() {
  const { data: companies = [] } = useCompanies();
  const { selectedCompanyIds } = useSelectedCompanyIds();
  const { isSuperAdmin, officeId, officeRole } = useProfile();
  const [selectedTab, setSelectedTab] = useState<"simples-nacional" | "mei">("simples-nacional");
  const [guideModalState, setGuideModalState] = useState<DeclarationGuideModalState>(initialGuideModalState);
  const [storedDocumentsModalState, setStoredDocumentsModalState] = useState<DeclarationStoredDocumentsModalState>(
    initialStoredDocumentsModalState,
  );
  const defaultCompetence = useMemo(() => getDefaultDeclarationCompetence(), []);
  const [runHistory, setRunHistory] = useState<DeclarationRunState[]>([]);
  const [runHistoryEntries, setRunHistoryEntries] = useState<DeclarationRunHistoryEntry[]>([]);
  const [runHistoryTotal, setRunHistoryTotal] = useState(0);
  const [runHistoryPage, setRunHistoryPage] = useState(1);
  const [runHistoryPageSize, setRunHistoryPageSize] = useState(10);
  const safeRunHistory = Array.isArray(runHistory) ? runHistory : [];
  const [dispatching, setDispatching] = useState(false);
  const [zipDownloading, setZipDownloading] = useState(false);
  const [downloadingArtifactKey, setDownloadingArtifactKey] = useState<string | null>(null);

  const canOperate = isSuperAdmin || officeRole === "owner" || officeRole === "admin" || officeRole === "operator";
  const visibleCompanies = useMemo(() => {
    if (selectedCompanyIds.length === 0) return companies;
    const ids = new Set(selectedCompanyIds);
    return companies.filter((company) => ids.has(company.id));
  }, [companies, selectedCompanyIds]);
  const declarationCompanies = useMemo(
    () =>
      visibleCompanies.map((company) => ({
        id: company.id,
        name: company.name,
        document: company.document,
        active: company.active,
      })),
    [visibleCompanies],
  );
  const selectionLabel =
    selectedCompanyIds.length === 0
      ? "Todas as empresas selecionadas"
      : selectedCompanyIds.length === 1
        ? visibleCompanies[0]?.name ?? "1 empresa selecionada"
        : `${selectedCompanyIds.length} empresas selecionadas`;

  const bootstrapQuery = useQuery({
    queryKey: ["fiscal-declaracoes-bootstrap", declarationCompanies.map((company) => company.id).join(",")],
    queryFn: () => getFiscalDeclarationsBootstrap({ companies: declarationCompanies }),
    placeholderData: keepPreviousData,
  });

  const runHistoryQuery = useQuery({
    queryKey: ["fiscal-declaracoes-run-history", officeId, runHistoryPage, runHistoryPageSize],
    queryFn: () =>
      listDeclarationRunHistory({
        page: runHistoryPage,
        pageSize: runHistoryPageSize,
        actions: ["simples_emitir_guia"],
      }),
    enabled: Boolean(officeId),
    placeholderData: keepPreviousData,
    refetchInterval: safeRunHistory.some((run) => !run.terminal || runNeedsArtifactResolution(run)) ? 5_000 : false,
    refetchIntervalInBackground: true,
  });

  useEffect(() => {
    if (!runHistoryQuery.data) return;
    const normalizedHistory = normalizeRunHistoryPayload(runHistoryQuery.data);
    setRunHistory(normalizedHistory.runs);
    setRunHistoryEntries(normalizedHistory.entries);
    setRunHistoryTotal(normalizedHistory.totalEntries);
  }, [runHistoryQuery.data]);

  const openRuns = useMemo(
    () =>
      safeRunHistory.filter(
        (run) => run.requestIds.length > 0 && (!run.terminal || runNeedsArtifactResolution(run)),
      ),
    [safeRunHistory],
  );
  const runStatusQueries = useQueries({
    queries: openRuns.map((run) => ({
      queryKey: ["fiscal-declaracoes-run", run.runId, run.requestIds.join(",")],
      queryFn: () => getDeclarationRunState(run),
      enabled: true,
      refetchInterval: (query: { state: { data?: DeclarationRunState } }) => {
        const data = query.state.data as DeclarationRunState | undefined;
        return data?.terminal ? false : 2500;
      },
      refetchIntervalInBackground: true,
    })),
  });
  const runQuerySignature = runStatusQueries
    .map((query) => `${query.dataUpdatedAt}:${query.data?.runId ?? ""}:${query.isFetching ? "1" : "0"}`)
    .join("|");

  useEffect(() => {
    const updates = runStatusQueries
      .map((query) => query.data)
      .filter((entry): entry is DeclarationRunState => Boolean(entry));
    if (updates.length === 0) return;
    setRunHistory((current) => {
      const baseRuns = Array.isArray(current) ? current : [];
      const nextRuns = updates.reduce((runs, update) => upsertRunHistory(runs, update), baseRuns);
      setRunHistoryEntries((currentEntries) => syncVisibleRunHistoryEntries(nextRuns, currentEntries));
      return nextRuns;
    });
    for (const update of updates) {
      void persistDeclarationRunState(update).catch(() => null);
    }
  }, [runQuerySignature]);

  const isBusy = dispatching;
  const availableCompanies = bootstrapQuery.data?.availableCompanies ?? declarationCompanies;
  const overdueGuides = bootstrapQuery.data?.overdueGuides ?? [];
  const actionAvailability = bootstrapQuery.data?.actionAvailability;
  const documentCompanyIds = availableCompanies.map((company) => company.id);
  const documentQueries = useQueries({
    queries: MEI_DOCUMENT_ACTIONS.map(({ action }) => ({
      queryKey: ["fiscal-declaracoes-documents", action, documentCompanyIds.join(",")],
      queryFn: () =>
        listDeclarationArtifacts({
          action,
          companyIds: documentCompanyIds,
          limit: 150,
        }),
      enabled: documentCompanyIds.length > 0,
      placeholderData: keepPreviousData,
      staleTime: 10_000,
    })),
  });
  const documentResponseByAction = useMemo(
    () =>
      new Map(
        MEI_DOCUMENT_ACTIONS.map((item, index) => [
          item.action,
          documentQueries[index]?.data,
        ]),
      ),
    [documentQueries],
  );
  const documentLoadingByAction = useMemo(
    () =>
      new Map(
        MEI_DOCUMENT_ACTIONS.map((item, index) => [
          item.action,
          Boolean(documentQueries[index]?.isLoading || documentQueries[index]?.isFetching),
        ]),
      ),
    [documentQueries],
  );

  const applyRunUpdate = (run: DeclarationRunState, options?: { incrementTotal?: boolean }) => {
    setRunHistory((current) => {
      const baseRuns = Array.isArray(current) ? current : [];
      const nextRuns = upsertRunHistory(baseRuns, run);
      setRunHistoryEntries(buildDeclarationRunHistoryEntries(nextRuns).slice(0, runHistoryPageSize));
      return nextRuns.slice(0, runHistoryPageSize);
    });
    if (options?.incrementTotal) {
      setRunHistoryTotal((current) => current + run.items.length);
    }
    void persistDeclarationRunState(run).catch(() => null);
  };

  const resolveActionDisabledReason = (action: DeclarationActionKind) => {
    if (!canOperate) return "Seu perfil tem acesso somente para consulta nesta area.";
    return actionAvailability?.[action]?.reason ?? null;
  };

  const isActionEnabled = (action: DeclarationActionKind) =>
    canOperate && Boolean(actionAvailability?.[action]?.enabled);

  const executeBatch = async (params: {
    action: DeclarationActionKind;
    mode: "emitir" | "recalcular";
    companyIds: string[];
    competence?: string | null;
    recalculateDueDate?: string | null;
  }) => {
    if (!canOperate) {
      toast.error("Seu perfil nao pode executar rotinas fiscais nesta area.");
      return;
    }

    setDispatching(true);
    try {
      setRunHistoryPage(1);
      const run = await startDeclarationRun({
        action: params.action,
        mode: params.mode,
        companies: availableCompanies,
        input: {
          companyIds: params.companyIds,
          competence: params.competence ?? null,
          recalculate: params.mode === "recalcular",
          recalculateDueDate: params.mode === "recalcular" ? params.recalculateDueDate ?? null : null,
        },
        onProgress: (nextRun) => applyRunUpdate(nextRun),
      });
      applyRunUpdate(run, { incrementTotal: true });
      if (run.requestIds.length > 0) {
        toast.success("Solicitacoes enviadas. O painel abaixo acompanhara o andamento da emissao.");
      } else {
        toast.error("Nenhuma solicitacao pode ser enviada. Revise os motivos exibidos no acompanhamento.");
      }
    } catch (error) {
      toast.error(
        sanitizeDeclarationError(error, "Não foi possível iniciar o processamento da rotina."),
      );
    } finally {
      setDispatching(false);
    }
  };

  const handleGuideModalSubmit = async (input: {
    companyIds: string[];
    competence: string;
    recalculate: boolean;
    recalculateDueDate?: string | null;
  }) => {
    await executeBatch({
      action: "simples_emitir_guia",
      mode: input.recalculate ? "recalcular" : "emitir",
      companyIds: input.companyIds,
      competence: input.competence,
      recalculateDueDate: input.recalculate ? input.recalculateDueDate ?? null : null,
    });
    setGuideModalState((current) => ({ ...current, open: false }));
  };

  const handleDownloadRunArtifact = async (run: DeclarationRunState, item: DeclarationRunItem) => {
    if (item.artifact?.filePath) {
      try {
        await downloadServerFileByPath(item.artifact.filePath, item.artifact.label);
        toast.success("Download iniciado.");
      } catch (error) {
        toast.error(
          sanitizeDeclarationError(error, "Não foi possível baixar o documento gerado."),
        );
      }
      return;
    }

    if (item.artifact?.artifactKey) {
      const rawReference = String(
        item.meta && typeof item.meta === "object"
          ? (item.meta as Record<string, unknown>).competencia ?? (item.meta as Record<string, unknown>).competence ?? ""
          : "",
      ).trim();
      try {
        await downloadDeclarationArtifact({
          action: run.action,
          companyId: item.companyId,
          competence: isValidCompetence(rawReference) ? rawReference : null,
          artifactKey: item.artifact.artifactKey,
          suggestedName: item.artifact.label,
        });
        toast.success("Download iniciado.");
      } catch (error) {
        toast.error(
          sanitizeDeclarationError(error, "Não foi possível baixar o documento gerado."),
        );
      }
      return;
    }

    if (item.artifact?.url) {
      window.open(item.artifact.url, "_blank", "noopener,noreferrer");
      return;
    }

    toast.error("Nenhum artefato disponivel para esta solicitacao.");
  };

  const handleDownloadHistoryEntryArtifact = async (entry: DeclarationRunHistoryEntry) => {
    const run = safeRunHistory.find((candidate) => candidate.runId === entry.runId) ?? null;
    if (run) {
      const hydratedRun = await hydrateDeclarationRunArtifacts(run).catch(() => run);
      if (hydratedRun !== run) {
        setRunHistory((current) => {
          const baseRuns = Array.isArray(current) ? current : [];
          const nextRuns = baseRuns.map((candidate) =>
            candidate.runId === hydratedRun.runId ? hydratedRun : candidate,
          );
          setRunHistoryEntries((currentEntries) => syncVisibleRunHistoryEntries(nextRuns, currentEntries));
          return nextRuns;
        });
        void persistDeclarationRunState(hydratedRun).catch(() => null);
      }

      const matchedItem = hydratedRun.items.find((item, index) => `${hydratedRun.runId}:${index}` === entry.entryId) ?? null;
      if (matchedItem) {
        await handleDownloadRunArtifact(hydratedRun, matchedItem);
        return;
      }
    }

    if (entry.artifact?.filePath) {
      try {
        await downloadServerFileByPath(entry.artifact.filePath, entry.artifact.label);
        toast.success("Download iniciado.");
      } catch (error) {
        toast.error(
          sanitizeDeclarationError(error, "Não foi possível baixar o documento gerado."),
        );
      }
      return;
    }

    if (entry.artifact?.artifactKey) {
      const rawReference = String(
        entry.meta && typeof entry.meta === "object"
          ? (entry.meta as Record<string, unknown>).competencia ?? (entry.meta as Record<string, unknown>).competence ?? ""
          : "",
      ).trim();
      try {
        await downloadDeclarationArtifact({
          action: entry.action,
          companyId: entry.companyId,
          competence: isValidCompetence(rawReference) ? rawReference : null,
          artifactKey: entry.artifact.artifactKey,
          suggestedName: entry.artifact.label,
        });
        toast.success("Download iniciado.");
      } catch (error) {
        toast.error(
          sanitizeDeclarationError(error, "Não foi possível baixar o documento gerado."),
        );
      }
      return;
    }

    toast.error("Nenhum PDF disponível para esta empresa.");
  };

  const handleDownloadAllRunHistoryZip = async () => {
    setZipDownloading(true);
    try {
      const allRuns = await listAllDeclarationRunHistoryRuns({
        actions: ["simples_emitir_guia"],
      });

      const addedFiles = await downloadDeclarationRunHistoryZip({
        runs: allRuns,
        suggestedName: "guias-simples-nacional",
      });
      toast.success(`${addedFiles} PDF(s) adicionados ao ZIP.`);
    } catch (error) {
      toast.error(
        sanitizeDeclarationError(error, "Não foi possível gerar o ZIP com os PDFs da lista."),
      );
    } finally {
      setZipDownloading(false);
    }
  };

  const handleDownloadDeclarationDocument = async (
    action: DeclarationActionKind,
    item: DeclarationArtifactListItem,
  ) => {
    setDownloadingArtifactKey(item.artifact_key);
    try {
      await downloadDeclarationArtifact({
        action,
        companyId: item.company_id,
        artifactKey: item.artifact_key,
        suggestedName: item.file_name,
      });
      toast.success("Download iniciado.");
    } catch (error) {
      toast.error(
        sanitizeDeclarationError(error, "Não foi possível baixar o documento localizado no servidor."),
      );
    } finally {
      setDownloadingArtifactKey(null);
    }
  };

  const handleClearAllRunHistory = async () => {
    setDispatching(true);
    try {
      const allRuns = await listAllDeclarationRunHistoryRuns({
        actions: ["simples_emitir_guia"],
      });
      const requestIds = Array.from(
        new Set(
          allRuns
            .filter((run) => !run.terminal)
            .flatMap((run) =>
            (run.requestIds ?? []).map((value) => String(value ?? "").trim()).filter(Boolean),
            ),
        ),
      );

      await stopDeclarationRobotRuntime({
        robotTechnicalIds: ["ecac_simples_emitir_guia"],
        reason: "Solicitacao cancelada ao limpar o historico de emissao no SaaS.",
      });

      if (requestIds.length > 0) {
        const cancellationResults = await Promise.allSettled(
          requestIds.map((requestId) =>
            cancelExecutionRequest(
              requestId,
              "Cancelado ao limpar o historico de emissao no SaaS.",
            ),
          ),
        );
        const failedCancellation = cancellationResults.find((result) => result.status === "rejected");
        if (failedCancellation?.status === "rejected") {
          throw failedCancellation.reason;
        }
      }

      await deleteAllDeclarationRunHistory({ actions: ["simples_emitir_guia"] });
      setRunHistory([]);
      setRunHistoryEntries([]);
      setRunHistoryTotal(0);
      setRunHistoryPage(1);
      await runHistoryQuery.refetch();
      toast.success("Histórico limpo e robô de emissão sinalizado para parar.");
    } catch (error) {
      toast.error(
        sanitizeDeclarationError(error, "Não foi possível limpar o histórico do escritório."),
      );
    } finally {
      setDispatching(false);
    }
  };

  return (
    <div className="space-y-6">
      <div className="relative overflow-hidden rounded-3xl border border-border/70 bg-gradient-to-br from-background via-background to-primary/5 p-6">
        <div className="absolute -right-10 top-0 h-36 w-36 rounded-full bg-primary/10 blur-3xl" />
        <div className="relative flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div className="space-y-3">
            <div className="inline-flex items-center gap-2 rounded-full border border-primary/15 bg-primary/5 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.14em] text-primary-icon">
              <ShieldCheck className="h-3.5 w-3.5" />
              Fiscal • Declaracoes
            </div>
            <div>
              <h1 className="text-3xl font-bold font-display tracking-tight">Declaracoes</h1>
              <p className="mt-2 max-w-3xl text-sm text-muted-foreground">
                Area operacional para Simples Nacional e MEI com foco em controle por empresa, consulta de documentos e emissao segura de guias.
              </p>
            </div>
          </div>

          <div className="grid gap-3 sm:grid-cols-1">
            <div className="rounded-2xl border border-border bg-background/70 px-4 py-3">
              <p className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Escopo atual</p>
              <p className="mt-1 text-sm font-semibold">{selectionLabel}</p>
            </div>
          </div>
        </div>
      </div>

      {!canOperate ? (
        <Alert>
          <BadgeAlert className="h-4 w-4" />
          <AlertTitle>Modo de consulta</AlertTitle>
          <AlertDescription>
            Seu perfil pode visualizar esta area, mas nao pode disparar emissoes ou reprocessamentos.
          </AlertDescription>
        </Alert>
      ) : null}

      {bootstrapQuery.isError ? (
        <Alert variant="destructive">
          <BadgeAlert className="h-4 w-4" />
          <AlertTitle>Não foi possível carregar a área de declarações</AlertTitle>
          <AlertDescription>
            {sanitizeDeclarationError(bootstrapQuery.error, "Tente novamente em instantes.")}
          </AlertDescription>
        </Alert>
      ) : null}

      <Tabs value={selectedTab} onValueChange={(value) => setSelectedTab(value as "simples-nacional" | "mei")} className="space-y-6">
        <TabsList className="h-auto rounded-2xl bg-muted/70 p-1">
          <TabsTrigger value="simples-nacional" className="rounded-xl px-5 py-2.5">
            Simples Nacional
          </TabsTrigger>
          <TabsTrigger value="mei" className="rounded-xl px-5 py-2.5">
            MEI
          </TabsTrigger>
        </TabsList>

        <TabsContent value="simples-nacional" className="space-y-6">
          <GlassCard className="border border-border/70 p-6">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
              <div className="space-y-2">
                <h2 className="text-xl font-semibold font-display tracking-tight">Operacoes do Simples Nacional</h2>
                <p className="text-sm text-muted-foreground">
                  A emissao usa o modal operacional do SaaS. Extratos e DEFIS abrem um modal proprio com os PDFs salvos no servidor, no formato parecido com o e-CAC.
                </p>
              </div>
              <div className="rounded-2xl border border-border bg-background/70 px-4 py-3 text-sm">
                <p className="text-xs uppercase tracking-[0.12em] text-muted-foreground">Empresas no escopo</p>
                <p className="mt-1 text-lg font-semibold font-display">{availableCompanies.length}</p>
              </div>
            </div>
          </GlassCard>

          <OverdueGuidesCard
            guides={overdueGuides}
            busy={isBusy}
            onRecalculate={(guide) =>
              setGuideModalState({
                open: true,
                action: "simples_emitir_guia",
                source: "overdue-guide",
                presetCompanyId: guide.companyId,
                presetCompetence: guide.competence,
                presetDueDate: guide.dueDate,
                recalculateByDefault: true,
              })
            }
          />

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
            {SIMPLES_DOCUMENT_ACTIONS.map((item) => (
              <DeclarationActionCard
                key={item.action}
                eyebrow={
                  item.action === "simples_emitir_guia"
                    ? "DAS"
                    : item.action === "simples_extrato"
                      ? "Extrato"
                      : "Anual"
                }
                title={item.title}
                description={item.description}
                icon={
                  item.action === "simples_emitir_guia" ? (
                    <ReceiptText className="h-5 w-5" />
                  ) : item.action === "simples_extrato" ? (
                    <FileArchive className="h-5 w-5" />
                  ) : (
                    <ShieldCheck className="h-5 w-5" />
                  )
                }
                ctaLabel={
                  item.action === "simples_emitir_guia"
                    ? "Abrir emissao"
                    : item.action === "simples_extrato"
                      ? "Abrir extratos"
                      : "Abrir DEFIS"
                }
                busy={dispatching}
                disabled={!isActionEnabled(item.action)}
                disabledReason={resolveActionDisabledReason(item.action)}
                onClick={() => {
                  if (item.action === "simples_emitir_guia") {
                    setGuideModalState({
                      open: true,
                      action: "simples_emitir_guia",
                      source: "card",
                      presetCompanyId: null,
                      presetCompetence: defaultCompetence,
                      presetDueDate: null,
                      recalculateByDefault: false,
                    });
                    return;
                  }

                  setStoredDocumentsModalState({
                    open: true,
                    action: item.action,
                    presetCompanyId: availableCompanies[0]?.id ?? null,
                    presetYear: defaultCompetence.slice(0, 4),
                  });
                }}
                toneClassName={
                  item.action === "simples_emitir_guia"
                    ? "from-background to-primary/5"
                    : item.action === "simples_extrato"
                      ? "from-background to-sky-500/5"
                      : "from-background to-emerald-500/5"
                }
              />
            ))}
          </div>

          <DeclarationRunHistoryTable
            entries={runHistoryEntries}
            loading={runHistoryQuery.isLoading || runHistoryQuery.isFetching}
            totalItems={runHistoryTotal}
            currentPage={runHistoryPage}
            pageSize={runHistoryPageSize}
            onPageChange={setRunHistoryPage}
            onPageSizeChange={(pageSize) => {
              setRunHistoryPageSize(pageSize);
              setRunHistoryPage(1);
            }}
            onDownloadArtifact={(entry) => void handleDownloadHistoryEntryArtifact(entry)}
            onDownloadAllZip={() => void handleDownloadAllRunHistoryZip()}
            zipBusy={zipDownloading}
            onClearAll={() => void handleClearAllRunHistory()}
          />
        </TabsContent>

        <TabsContent value="mei" className="space-y-6">
          <GlassCard className="border border-border/70 p-6">
            <div className="space-y-2">
              <h2 className="text-xl font-semibold font-display tracking-tight">Operacoes do MEI</h2>
              <p className="text-sm text-muted-foreground">
                A aba do MEI mantem o mesmo padrao de cards, feedback operacional e bloqueios de concorrencia usados no restante do SaaS.
              </p>
            </div>
          </GlassCard>

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            <DeclarationActionCard
              eyebrow="Anual"
              title="Declaracao Anual do MEI"
              description="Enfileira a declaracao anual das empresas MEI dentro do escopo atual, com acompanhamento de status por empresa."
              icon={<FileArchive className="h-5 w-5" />}
              ctaLabel="Executar declaracao"
              busy={dispatching}
              disabled={!isActionEnabled("mei_declaracao_anual")}
              disabledReason={resolveActionDisabledReason("mei_declaracao_anual")}
              onClick={() => {
                void executeBatch({
                  action: "mei_declaracao_anual",
                  mode: "emitir",
                  companyIds: availableCompanies.map((company) => company.id),
                  competence: null,
                });
              }}
              toneClassName="from-background to-amber-500/5"
            />
            <DeclarationActionCard
              eyebrow="Mensal"
              title="Guias Mensais do MEI"
              description="Dispara a emissao mensal das guias MEI com o mesmo padrao visual, mensagens seguras e acompanhamento consolidado."
              icon={<ReceiptText className="h-5 w-5" />}
              ctaLabel="Emitir guias"
              busy={dispatching}
              disabled={!isActionEnabled("mei_guias_mensais")}
              disabledReason={resolveActionDisabledReason("mei_guias_mensais")}
              onClick={() => {
                void executeBatch({
                  action: "mei_guias_mensais",
                  mode: "emitir",
                  companyIds: availableCompanies.map((company) => company.id),
                  competence: defaultCompetence,
                });
              }}
              toneClassName="from-background to-primary/5"
            />
          </div>

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            {MEI_DOCUMENT_ACTIONS.map((item) => (
              <DeclarationArtifactsCard
                key={item.action}
                action={item.action}
                title={item.title}
                description={item.description}
                loading={documentLoadingByAction.get(item.action) ?? false}
                response={documentResponseByAction.get(item.action)}
                busyArtifactKey={downloadingArtifactKey}
                onDownload={(artifact) => void handleDownloadDeclarationDocument(item.action, artifact)}
              />
            ))}
          </div>
        </TabsContent>
      </Tabs>

      <DeclarationExecutionModal
        open={guideModalState.open}
        state={guideModalState}
        companies={availableCompanies}
        defaultCompetence={defaultCompetence}
        busy={dispatching}
        onOpenChange={(open) => setGuideModalState((current) => ({ ...current, open }))}
        onSubmit={handleGuideModalSubmit}
      />

      <DeclarationStoredDocumentsModal
        open={storedDocumentsModalState.open}
        state={storedDocumentsModalState}
        companies={availableCompanies}
        onOpenChange={(open) => setStoredDocumentsModalState((current) => ({ ...current, open }))}
      />

      {bootstrapQuery.isLoading ? (
        <div className={cn("fixed bottom-4 right-4 rounded-full border border-border bg-card/95 px-4 py-2 shadow-lg")}>
          <span className="inline-flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Carregando declaracoes...
          </span>
        </div>
      ) : null}
    </div>
  );
}
