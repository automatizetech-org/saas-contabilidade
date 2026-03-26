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
import { downloadServerFileByPath } from "@/services/serverFileService";
import { cn } from "@/utils";
import {
  getDefaultDeclarationCompetence,
  sanitizeDeclarationError,
} from "@/features/fiscal-declaracoes/helpers";
import {
  downloadDeclarationArtifact,
  getDeclarationRunState,
  getFiscalDeclarationsBootstrap,
  listDeclarationArtifacts,
  startDeclarationRun,
} from "@/features/fiscal-declaracoes/service";
import type {
  DeclarationActionKind,
  DeclarationArtifactListItem,
  DeclarationGuideModalState,
  DeclarationRunItem,
  DeclarationRunState,
} from "@/features/fiscal-declaracoes/types";
import { DeclarationActionCard } from "@/features/fiscal-declaracoes/components/DeclarationActionCard";
import { DeclarationArtifactsCard } from "@/features/fiscal-declaracoes/components/DeclarationArtifactsCard";
import { DeclarationExecutionModal } from "@/features/fiscal-declaracoes/components/DeclarationExecutionModal";
import { DeclarationProcessingPanel } from "@/features/fiscal-declaracoes/components/DeclarationProcessingPanel";
import { OverdueGuidesCard } from "@/features/fiscal-declaracoes/components/OverdueGuidesCard";

const initialGuideModalState: DeclarationGuideModalState = {
  open: false,
  action: "simples_emitir_guia",
  source: "card",
  presetCompanyId: null,
  presetCompetence: null,
  presetDueDate: null,
  recalculateByDefault: false,
};

const SIMPLES_DOCUMENT_ACTIONS: Array<{
  action: DeclarationActionKind;
  title: string;
  description: string;
}> = [
  {
    action: "simples_emitir_guia",
    title: "Emitir Guia",
    description: "Lista os arquivos ja salvos no segmento configurado e permite abrir o fluxo de emissao/recalculo.",
  },
  {
    action: "simples_extrato",
    title: "Extrato do Simples Nacional",
    description: "Lista os extratos ja salvos no segmento configurado e permite solicitar a coleta completa.",
  },
  {
    action: "simples_defis",
    title: "DEFIS",
    description: "Lista declaracoes e recibos disponiveis no segmento configurado e permite solicitar nova coleta.",
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
  const { isSuperAdmin, officeRole } = useProfile();
  const [selectedTab, setSelectedTab] = useState<"simples-nacional" | "mei">("simples-nacional");
  const [guideModalState, setGuideModalState] = useState<DeclarationGuideModalState>(initialGuideModalState);
  const defaultCompetence = useMemo(() => getDefaultDeclarationCompetence(), []);
  const [activeRun, setActiveRun] = useState<DeclarationRunState | null>(null);
  const [dispatching, setDispatching] = useState(false);
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

  const runStatusQuery = useQuery({
    queryKey: ["fiscal-declaracoes-run", activeRun?.runId, activeRun?.requestIds.join(",")],
    queryFn: () => getDeclarationRunState(activeRun!),
    enabled: Boolean(activeRun && activeRun.requestIds.length > 0 && !activeRun.terminal),
    refetchInterval: (query) => {
      const data = query.state.data as DeclarationRunState | undefined;
      return data?.terminal ? false : 2500;
    },
    refetchIntervalInBackground: true,
  });

  useEffect(() => {
    if (runStatusQuery.data) setActiveRun(runStatusQuery.data);
  }, [runStatusQuery.data]);

  const hasRunningBatch = Boolean(activeRun && !activeRun.terminal);
  const isBusy = dispatching || hasRunningBatch;
  const availableCompanies = bootstrapQuery.data?.availableCompanies ?? declarationCompanies;
  const overdueGuides = bootstrapQuery.data?.overdueGuides ?? [];
  const actionAvailability = bootstrapQuery.data?.actionAvailability;
  const documentCompanyIds = availableCompanies.map((company) => company.id);
  const documentQueries = useQueries({
    queries: [...SIMPLES_DOCUMENT_ACTIONS, ...MEI_DOCUMENT_ACTIONS].map(({ action }) => ({
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
        [...SIMPLES_DOCUMENT_ACTIONS, ...MEI_DOCUMENT_ACTIONS].map((item, index) => [
          item.action,
          documentQueries[index]?.data,
        ]),
      ),
    [documentQueries],
  );
  const documentLoadingByAction = useMemo(
    () =>
      new Map(
        [...SIMPLES_DOCUMENT_ACTIONS, ...MEI_DOCUMENT_ACTIONS].map((item, index) => [
          item.action,
          Boolean(documentQueries[index]?.isLoading || documentQueries[index]?.isFetching),
        ]),
      ),
    [documentQueries],
  );

  const resolveActionDisabledReason = (action: DeclarationActionKind) => {
    if (!canOperate) return "Seu perfil tem acesso somente para consulta nesta área.";
    if (hasRunningBatch) return "Aguarde o processamento atual terminar para iniciar uma nova rotina.";
    return actionAvailability?.[action]?.reason ?? null;
  };

  const isActionEnabled = (action: DeclarationActionKind) =>
    canOperate && !hasRunningBatch && Boolean(actionAvailability?.[action]?.enabled);

  const executeBatch = async (params: {
    action: DeclarationActionKind;
    mode: "emitir" | "recalcular";
    companyIds: string[];
    competence?: string | null;
    recalculateDueDate?: string | null;
  }) => {
    if (!canOperate) {
      toast.error("Seu perfil não pode executar rotinas fiscais nesta área.");
      return;
    }
    if (hasRunningBatch) {
      toast.error("Já existe um processamento em andamento. Aguarde a conclusão para iniciar outro.");
      return;
    }

    setDispatching(true);
    try {
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
        onProgress: setActiveRun,
      });
      setActiveRun(run);
      if (run.requestIds.length > 0) {
        toast.success("Solicitações enviadas. O painel abaixo acompanhará o andamento por empresa.");
      } else {
        toast.error("Nenhuma solicitação pôde ser enviada. Revise os motivos exibidos no acompanhamento.");
      }
    } catch (error) {
      toast.error(
        sanitizeDeclarationError(error, "Não foi possível iniciar o processamento da rotina."),
      );
    } finally {
      setDispatching(false);
    }
  };

  const handleSimpleModalSubmit = async (input: {
    companyIds: string[];
    competence: string;
    recalculate: boolean;
    recalculateDueDate?: string | null;
  }) => {
    await executeBatch({
      action: guideModalState.action,
      mode: guideModalState.action === "simples_emitir_guia" && input.recalculate ? "recalcular" : "emitir",
      companyIds: input.companyIds,
      competence: input.competence,
      recalculateDueDate: guideModalState.action === "simples_emitir_guia" ? input.recalculateDueDate ?? null : null,
    });
    setGuideModalState((current) => ({ ...current, open: false }));
  };

  const handleDownloadArtifact = async (item: DeclarationRunItem) => {
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

    if (item.artifact?.url) {
      window.open(item.artifact.url, "_blank", "noopener,noreferrer");
      return;
    }

    toast.error("Nenhum artefato disponível para esta empresa.");
  };

  const handleOpenArtifact = async (item: DeclarationRunItem) => {
    if (item.artifact?.url) {
      window.open(item.artifact.url, "_blank", "noopener,noreferrer");
      return;
    }
    await handleDownloadArtifact(item);
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
        sanitizeDeclarationError(error, "Nao foi possivel baixar o documento localizado no servidor."),
      );
    } finally {
      setDownloadingArtifactKey(null);
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
              Fiscal • Declarações
            </div>
            <div>
              <h1 className="text-3xl font-bold font-display tracking-tight">Declarações</h1>
              <p className="mt-2 max-w-3xl text-sm text-muted-foreground">
                Área operacional para Simples Nacional e MEI com foco em usabilidade, controle por empresa e acompanhamento seguro do processamento.
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
            Seu perfil pode visualizar esta área, mas não pode disparar emissões ou reprocessamentos.
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
                <h2 className="text-xl font-semibold font-display tracking-tight">Operações do Simples Nacional</h2>
                <p className="text-sm text-muted-foreground">
                  A emissão usa os dados informados no modal do SaaS, enquanto extratos e DEFIS fazem a coleta completa do que estiver disponível no portal.
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
              <DeclarationArtifactsCard
                key={item.action}
                action={item.action}
                title={item.title}
                description={item.description}
                loading={documentLoadingByAction.get(item.action) ?? false}
                response={documentResponseByAction.get(item.action)}
                busyArtifactKey={downloadingArtifactKey}
                ctaLabel={
                  item.action === "simples_emitir_guia"
                    ? "Abrir emissão"
                    : item.action === "simples_extrato"
                      ? "Solicitar extrato"
                      : "Solicitar DEFIS"
                }
                actionBusy={dispatching}
                actionDisabled={!canOperate}
                onPrimaryAction={() => {
                  setGuideModalState({
                    open: true,
                    action: item.action,
                    source: "card",
                    presetCompanyId: availableCompanies[0]?.id ?? null,
                    presetCompetence: defaultCompetence,
                    presetDueDate: null,
                    recalculateByDefault: false,
                  });
                }}
                onDownload={(artifact) => handleDownloadDeclarationDocument(item.action, artifact)}
              />
            ))}
          </div>
        </TabsContent>

        <TabsContent value="mei" className="space-y-6">
          <GlassCard className="border border-border/70 p-6">
            <div className="space-y-2">
              <h2 className="text-xl font-semibold font-display tracking-tight">Operações do MEI</h2>
              <p className="text-sm text-muted-foreground">
                A aba do MEI mantém o mesmo padrão de cards, feedback operacional e bloqueios de concorrência usados no Simples Nacional.
              </p>
            </div>
          </GlassCard>

          <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
            <DeclarationActionCard
              eyebrow="Anual"
              title="Declaração Anual do MEI"
              description="Enfileira a declaração anual das empresas MEI dentro do escopo atual, com acompanhamento de status por empresa."
              icon={<FileArchive className="h-5 w-5" />}
              ctaLabel="Executar declaração"
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
              description="Dispara a emissão mensal das guias MEI com o mesmo padrão visual, mensagens seguras e acompanhamento consolidado."
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
                onDownload={(artifact) => handleDownloadDeclarationDocument(item.action, artifact)}
              />
            ))}
          </div>
        </TabsContent>
      </Tabs>

      <DeclarationProcessingPanel
        run={activeRun}
        loading={runStatusQuery.isFetching}
        onOpenArtifact={handleOpenArtifact}
        onDownloadArtifact={handleDownloadArtifact}
      />

      <DeclarationExecutionModal
        open={guideModalState.open}
        state={guideModalState}
        companies={availableCompanies}
        defaultCompetence={defaultCompetence}
        busy={dispatching}
        onOpenChange={(open) => setGuideModalState((current) => ({ ...current, open }))}
        onSubmit={handleSimpleModalSubmit}
      />

      {bootstrapQuery.isLoading ? (
        <div className={cn("fixed bottom-4 right-4 rounded-full border border-border bg-card/95 px-4 py-2 shadow-lg")}>
          <span className="inline-flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Carregando declarações...
          </span>
        </div>
      ) : null}
    </div>
  );
}
