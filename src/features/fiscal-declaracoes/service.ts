import { getRobotEligibilityReport, indexCompanyRobotConfigs } from "@/lib/robotEligibility";
import {
  getCompanyRobotConfigsForSelection,
} from "@/services/companiesService";
import { createExecutionRequest } from "@/services/executionRequestsService";
import { getRobots, type Robot } from "@/services/robotsService";
import {
  downloadOfficeServerAction,
  postOfficeServerJson,
} from "@/services/serverFileService";
import { supabase } from "@/services/supabaseClient";
import type { Json } from "@/types/database";
import {
  asArray,
  asObject,
  createRunId,
  isValidCompetence,
  isValidIsoDate,
  sanitizeDeclarationError,
  toPeriodRange,
} from "./helpers";
import type {
  DeclarationActionAvailability,
  DeclarationActionKind,
  DeclarationActionMode,
  DeclarationBootstrapData,
  DeclarationArtifactListResponse,
  DeclarationCompany,
  DeclarationGuideSubmitInput,
  DeclarationRunItem,
  DeclarationRunState,
  OverdueGuide,
} from "./types";

const ACTION_TITLES: Record<DeclarationActionKind, string> = {
  simples_emitir_guia: "Emissão de guia do Simples Nacional",
  simples_extrato: "Solicitação de extrato do Simples Nacional",
  simples_defis: "Solicitação de DEFIS",
  mei_declaracao_anual: "Declaração anual do MEI",
  mei_guias_mensais: "Guias mensais do MEI",
};

const ACTION_ROBOT_CANDIDATES: Record<DeclarationActionKind, string[]> = {
  simples_emitir_guia: [
    "ecac_simples_emitir_guia",
    "simples_nacional_emitir_guia",
    "simples_nacional_guia",
    "simples_nacional_das",
    "simples_nacional",
  ],
  simples_extrato: [
    "ecac_simples_consulta_extratos_defis",
    "simples_nacional_extrato",
    "simples_nacional_consulta_extratos_defis",
    "simples_extrato",
    "simples_nacional",
  ],
  simples_defis: [
    "ecac_simples_consulta_extratos_defis",
    "simples_nacional_consulta_extratos_defis",
    "simples_nacional_defis",
    "defis",
  ],
  mei_declaracao_anual: ["mei_declaracao_anual", "mei_anual", "mei"],
  mei_guias_mensais: ["mei_guias_mensais", "mei_das", "mei"],
};

const ACTION_TEXT_MATCHERS: Record<DeclarationActionKind, string[]> = {
  simples_emitir_guia: ["simples", "guia", "das"],
  simples_extrato: ["simples", "extrato"],
  simples_defis: ["defis"],
  mei_declaracao_anual: ["mei", "declaracao", "anual"],
  mei_guias_mensais: ["mei", "guia"],
};

function normalizeToken(value: string) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .trim()
    .toLowerCase();
}

function uniqueCompanyIds(companyIds: string[]) {
  return Array.from(new Set(companyIds.map((item) => String(item ?? "").trim()).filter(Boolean)));
}

function mapRunItems(items: DeclarationRunItem[]) {
  return new Map(items.map((item) => [item.companyId, item]));
}

function buildRunState(params: {
  action: DeclarationActionKind;
  mode: DeclarationActionMode;
  requestIds: string[];
  items: DeclarationRunItem[];
  startedAt: string;
  finishedAt?: string | null;
  runId?: string;
}): DeclarationRunState {
  const terminal = params.items.every((item) => item.status === "sucesso" || item.status === "erro");
  return {
    runId: params.runId ?? createRunId(),
    action: params.action,
    mode: params.mode,
    title: ACTION_TITLES[params.action],
    requestIds: params.requestIds,
    items: params.items,
    startedAt: params.startedAt,
    finishedAt: terminal ? (params.finishedAt ?? new Date().toISOString()) : null,
    terminal,
  };
}

function findRobotForAction(action: DeclarationActionKind, robots: Robot[]): Robot | null {
  const candidates = ACTION_ROBOT_CANDIDATES[action];
  const exact = robots.find((robot) => candidates.includes(robot.technical_id));
  if (exact) return exact;

  const matchers = ACTION_TEXT_MATCHERS[action];
  return (
    robots.find((robot) => {
      const haystack = normalizeToken(`${robot.technical_id} ${robot.display_name}`);
      return matchers.every((token) => haystack.includes(normalizeToken(token)));
    }) ?? null
  );
}

function buildActionUnavailable(reason: string): DeclarationActionAvailability {
  return { enabled: false, reason, robotTechnicalId: null };
}

function buildActionAvailability(params: {
  action: DeclarationActionKind;
  robot: Robot | null;
  eligibleCount: number;
}): DeclarationActionAvailability {
  if (!params.robot) {
    return buildActionUnavailable("Rotina ainda não configurada para este escritório.");
  }
  if (params.eligibleCount <= 0) {
    return {
      enabled: false,
      reason: "Nenhuma empresa visível atende os requisitos operacionais desta rotina.",
      robotTechnicalId: params.robot.technical_id,
    };
  }
  return {
    enabled: true,
    reason: null,
    robotTechnicalId: params.robot.technical_id,
  };
}

async function getOverdueGuides(_companyIds: string[]): Promise<OverdueGuide[]> {
  // A base atual ainda não expõe uma fonte dedicada para DAS vencidas.
  // A UI já fica preparada para consumir a lista assim que o backend for disponibilizado.
  return [];
}

export async function getFiscalDeclarationsBootstrap(params: {
  companies: DeclarationCompany[];
}): Promise<DeclarationBootstrapData> {
  const availableCompanies = params.companies
    .map((company) => ({
      id: company.id,
      name: company.name,
      document: company.document,
      active: Boolean(company.active),
    }))
    .sort((left, right) => left.name.localeCompare(right.name, "pt-BR"));

  if (availableCompanies.length === 0) {
    return {
      availableCompanies,
      overdueGuides: [],
      actionAvailability: {
        simples_emitir_guia: buildActionUnavailable("Nenhuma empresa selecionada no escopo atual."),
        simples_extrato: buildActionUnavailable("Nenhuma empresa selecionada no escopo atual."),
        simples_defis: buildActionUnavailable("Nenhuma empresa selecionada no escopo atual."),
        mei_declaracao_anual: buildActionUnavailable("Nenhuma empresa selecionada no escopo atual."),
        mei_guias_mensais: buildActionUnavailable("Nenhuma empresa selecionada no escopo atual."),
      },
    };
  }

  const robots = await getRobots().catch(() => []);
  const companyIds = availableCompanies.map((company) => company.id);
  const matchedRobots = Object.fromEntries(
    (Object.keys(ACTION_TITLES) as DeclarationActionKind[]).map((action) => [action, findRobotForAction(action, robots)]),
  ) as Record<DeclarationActionKind, Robot | null>;

  const robotTechnicalIds = uniqueCompanyIds(
    (Object.values(matchedRobots).map((robot) => robot?.technical_id ?? "")).filter(Boolean),
  );
  const companyConfigs =
    robotTechnicalIds.length > 0
      ? await getCompanyRobotConfigsForSelection({
          companyIds,
          robotTechnicalIds,
        }).catch(() => [])
      : [];
  const configIndex = indexCompanyRobotConfigs(companyConfigs);

  const actionAvailability = Object.fromEntries(
    (Object.keys(ACTION_TITLES) as DeclarationActionKind[]).map((action) => {
      const robot = matchedRobots[action];
      const eligibleCount = robot
        ? getRobotEligibilityReport({
            robot,
            selectedCompanyIds: companyIds,
            companies: availableCompanies,
            companyConfigsByRobot: configIndex,
          }).eligibleCompanyIds.length
        : 0;
      return [action, buildActionAvailability({ action, robot, eligibleCount })];
    }),
  ) as Record<DeclarationActionKind, DeclarationActionAvailability>;

  return {
    availableCompanies,
    overdueGuides: await getOverdueGuides(companyIds),
    actionAvailability,
  };
}

export function validateDeclarationGuideSubmitInput(params: {
  action: DeclarationActionKind;
  input: DeclarationGuideSubmitInput;
  availableCompanies: DeclarationCompany[];
}) {
  const availableCompanyIds = new Set(params.availableCompanies.map((company) => company.id));
  const companyIds = uniqueCompanyIds(params.input.companyIds).filter((companyId) =>
    availableCompanyIds.has(companyId),
  );

  if (companyIds.length === 0) {
    throw new Error("Selecione ao menos uma empresa disponível no escopo atual.");
  }
  const rawCompetence = String(params.input.competence ?? "").trim();
  const requiresCompetence =
    params.action === "simples_emitir_guia"
    || params.action === "simples_extrato"
    || params.action === "simples_defis"
    || params.action === "mei_guias_mensais";
  if (requiresCompetence && !isValidCompetence(rawCompetence)) {
    throw new Error("Informe uma competência válida no formato MM/AAAA.");
  }
  if (params.action === "simples_emitir_guia" && params.input.recalculate) {
    if (!params.input.recalculateDueDate || !isValidIsoDate(params.input.recalculateDueDate)) {
      throw new Error("Informe uma nova data de vencimento válida para o recálculo.");
    }
  }

  return {
    companyIds,
    competence: rawCompetence || null,
    recalculate: params.action === "simples_emitir_guia" ? params.input.recalculate : false,
    recalculateDueDate: params.action === "simples_emitir_guia" && params.input.recalculate
      ? params.input.recalculateDueDate ?? null
      : null,
  };
}

type StartDeclarationRunParams = {
  action: DeclarationActionKind;
  mode: DeclarationActionMode;
  companies: DeclarationCompany[];
  input: DeclarationGuideSubmitInput;
  onProgress?: (state: DeclarationRunState) => void;
};

function createQueuedItems(companies: DeclarationCompany[]): DeclarationRunItem[] {
  return companies.map((company) => ({
    companyId: company.id,
    companyName: company.name,
    companyDocument: company.document,
    status: "pendente",
    message: "Aguardando envio da solicitação.",
    executionRequestId: null,
    artifact: null,
  }));
}

function normalizeSummaryMessage(summary: Record<string, Json>, fallback: string) {
  const candidates = [
    summary.message,
    summary.status_message,
    summary.detail,
    summary.description,
    summary.result,
  ];
  for (const candidate of candidates) {
    const value = String(candidate ?? "").trim();
    if (value) return value;
  }
  return fallback;
}

function extractArtifact(summary: Record<string, Json>) {
  const filePath = String(summary.file_path ?? summary.document_path ?? "").trim();
  const url = String(summary.download_url ?? summary.file_url ?? summary.document_url ?? "").trim();
  const label =
    String(summary.document_name ?? summary.file_name ?? summary.filename ?? "").trim() || "Baixar documento";
  if (!filePath && !url) return null;
  return {
    label,
    filePath: filePath || null,
    url: url || null,
  };
}

export async function startDeclarationRun(
  params: StartDeclarationRunParams,
): Promise<DeclarationRunState> {
  const validatedInput = validateDeclarationGuideSubmitInput({
    action: params.action,
    input: params.input,
    availableCompanies: params.companies,
  });
  const runId = createRunId();
  const startedAt = new Date().toISOString();
  const selectedCompanies = params.companies.filter((company) =>
    validatedInput.companyIds.includes(company.id),
  );
  const runItems = createQueuedItems(selectedCompanies);
  const runItemsMap = mapRunItems(runItems);
  const requestIds: string[] = [];

  const updateCompanyItem = (companyId: string, updates: Partial<DeclarationRunItem>) => {
    const current = runItemsMap.get(companyId);
    if (!current) return;
    const next = { ...current, ...updates };
    runItemsMap.set(companyId, next);
    const snapshot = buildRunState({
      action: params.action,
      mode: params.mode,
      requestIds,
      items: selectedCompanies.map((company) => runItemsMap.get(company.id) ?? current),
      startedAt,
      runId,
    });
    params.onProgress?.(snapshot);
    return snapshot;
  };

  const robots = await getRobots().catch(() => []);
  const robot = findRobotForAction(params.action, robots);
  if (!robot) {
    const finalState = buildRunState({
      action: params.action,
      mode: params.mode,
      requestIds,
      items: selectedCompanies.map((company) => ({
        companyId: company.id,
        companyName: company.name,
        companyDocument: company.document,
        status: "erro",
        message: "Esta rotina ainda não foi configurada para o seu escritório.",
        executionRequestId: null,
        artifact: null,
      })),
      startedAt,
      finishedAt: new Date().toISOString(),
      runId,
    });
    params.onProgress?.(finalState);
    return finalState;
  }

  const companyConfigs = await getCompanyRobotConfigsForSelection({
    companyIds: validatedInput.companyIds,
    robotTechnicalIds: [robot.technical_id],
  }).catch(() => []);
  const configIndex = indexCompanyRobotConfigs(companyConfigs);
  const eligibility = getRobotEligibilityReport({
    robot,
    selectedCompanyIds: validatedInput.companyIds,
    companies: selectedCompanies,
    companyConfigsByRobot: configIndex,
  });
  const skippedByCompanyId = new Map(
    eligibility.skipped.map((item) => [item.companyId, item.reason]),
  );

  for (const company of selectedCompanies) {
    const skipReason = skippedByCompanyId.get(company.id);
    if (skipReason) {
      updateCompanyItem(company.id, {
        status: "erro",
        message: `Empresa sem elegibilidade operacional: ${skipReason}.`,
      });
      continue;
    }

    const shouldUsePeriod = Boolean(validatedInput.competence && isValidCompetence(validatedInput.competence));
    const { periodStart, periodEnd } = shouldUsePeriod
      ? toPeriodRange(validatedInput.competence!)
      : { periodStart: null, periodEnd: null };
    try {
      const request = await createExecutionRequest({
        companyIds: [company.id],
        robotTechnicalIds: [robot.technical_id],
        periodStart,
        periodEnd,
        executionMode: "sequential",
        source: "fiscal_declaracoes",
        jobPayload: {
          action: params.action,
          mode: params.mode,
          ui_origin: "fiscal_declaracoes",
          competence: validatedInput.competence,
          recalculate: validatedInput.recalculate,
          recalculate_due_date: validatedInput.recalculateDueDate,
          company_id: company.id,
          company_document: company.document,
        },
      });
      requestIds.push(request.id);
      updateCompanyItem(company.id, {
        status: "processando",
        executionRequestId: request.id,
        message: "Solicitação enviada para processamento.",
      });
    } catch (error) {
      updateCompanyItem(company.id, {
        status: "erro",
        message: sanitizeDeclarationError(
          error,
          "Não foi possível iniciar o processamento desta empresa.",
        ),
      });
    }
  }

  return buildRunState({
    action: params.action,
    mode: params.mode,
    requestIds,
    items: selectedCompanies.map((company) => runItemsMap.get(company.id)!),
    startedAt,
    runId,
  });
}

export async function getDeclarationRunState(current: DeclarationRunState): Promise<DeclarationRunState> {
  if (current.requestIds.length === 0 || current.terminal) return current;

  const [{ data: requests, error: requestsError }, { data: resultEvents, error: eventsError }] =
    await Promise.all([
      supabase
        .from("execution_requests")
        .select("id, company_ids, status, error_message, result_summary, completed_at")
        .in("id", current.requestIds),
      supabase
        .from("robot_result_events")
        .select("execution_request_id, status, summary, company_results, error_message")
        .in("execution_request_id", current.requestIds),
    ]);

  if (requestsError) throw requestsError;
  if (eventsError) throw eventsError;

  const requestById = new Map(
    (requests ?? []).map((row) => [row.id, row]),
  );
  const resultByExecutionId = new Map(
    (resultEvents ?? []).map((row) => [String(row.execution_request_id ?? ""), row]),
  );

  const nextItems = current.items.map((item) => {
    if (!item.executionRequestId) return item;
    const request = requestById.get(item.executionRequestId);
    if (!request) return item;

    const resultEvent = resultByExecutionId.get(item.executionRequestId);
    const requestSummary = asObject((request as { result_summary?: Json | null }).result_summary);
    const eventSummary = asObject((resultEvent as { summary?: Json | null } | undefined)?.summary);
    const companyResult = asObject(
      asArray((resultEvent as { company_results?: Json | null } | undefined)?.company_results)[0] ?? null,
    );
    const mergedSummary = {
      ...requestSummary,
      ...eventSummary,
      ...companyResult,
    };

    if (request.status === "completed") {
      return {
        ...item,
        status: "sucesso",
        message: normalizeSummaryMessage(
          mergedSummary,
          "Processamento concluído com sucesso.",
        ),
        artifact: extractArtifact(mergedSummary),
        meta: mergedSummary,
      };
    }

    if (request.status === "failed") {
      return {
        ...item,
        status: "erro",
        message: sanitizeDeclarationError(
          (resultEvent as { error_message?: string | null } | undefined)?.error_message ??
            (request as { error_message?: string | null }).error_message ??
            "Falha ao processar a rotina.",
          "Falha ao processar a rotina.",
        ),
        meta: mergedSummary,
      };
    }

    return {
      ...item,
      status: "processando",
      message: normalizeSummaryMessage(
        mergedSummary,
        request.status === "running"
          ? "Processamento em andamento."
          : "Solicitação aguardando execução.",
      ),
      meta: mergedSummary,
    };
  });

  return buildRunState({
    action: current.action,
    mode: current.mode,
    requestIds: current.requestIds,
    items: nextItems,
    startedAt: current.startedAt,
    finishedAt: nextItems.every((item) => item.status === "sucesso" || item.status === "erro")
      ? new Date().toISOString()
      : null,
    runId: current.runId,
  });
}

export async function listDeclarationArtifacts(params: {
  action: DeclarationActionKind;
  companyIds: string[];
  competence?: string | null;
  limit?: number;
}): Promise<DeclarationArtifactListResponse> {
  return postOfficeServerJson<DeclarationArtifactListResponse>(
    "list-declaration-artifacts",
    {
      action: params.action,
      company_ids: params.companyIds,
      competence: params.competence,
      limit: params.limit ?? 200,
    },
  );
}

export async function downloadDeclarationArtifact(params: {
  action: DeclarationActionKind;
  companyId: string;
  competence?: string | null;
  artifactKey: string;
  suggestedName?: string;
}): Promise<void> {
  await downloadOfficeServerAction(
    "download-declaration-artifact",
    {
      action: params.action,
      company_id: params.companyId,
      competence: params.competence,
      artifact_key: params.artifactKey,
    },
    params.suggestedName,
  );
}
