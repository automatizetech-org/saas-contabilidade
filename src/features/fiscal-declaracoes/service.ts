import JSZip from "jszip";
import { getRobotEligibilityReport, indexCompanyRobotConfigs } from "@/lib/robotEligibility";
import {
  getCompanyRobotConfigsForSelection,
} from "@/services/companiesService";
import { getCurrentOfficeContext } from "@/services/officeContextService";
import { createExecutionRequest } from "@/services/executionRequestsService";
import { getRobots, type Robot } from "@/services/robotsService";
import {
  downloadOfficeServerAction,
  fetchOfficeServerActionBlob,
  fetchServerFileByPath,
  openOfficeServerAction,
  postOfficeServerJson,
  probeServerFileByPath,
} from "@/services/serverFileService";
import { supabase } from "@/services/supabaseClient";
import type { Database, Json } from "@/types/database";
import {
  asArray,
  asObject,
  createRunId,
  formatCompetenceLabel,
  formatCurrencyFromCents,
  formatYearLabel,
  isValidCompetence,
  isValidIsoDate,
  isValidYear,
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
  DeclarationGuideDocumentListItem,
  DeclarationGuideDocumentListResponse,
  DeclarationGuideDocumentSortKey,
  DeclarationGuideSubmitInput,
  DeclarationRunHistoryEntry,
  DeclarationRunHistoryPage,
  DeclarationRunItem,
  DeclarationRunState,
  OverdueGuide,
} from "./types";

type DeclarationRunHistoryRow = Database["public"]["Tables"]["declaration_run_history"]["Row"];

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

const OVERDUE_GUIDES_ROBOT_CANDIDATES = [
  "ecac_simples_debitos",
  "simples_nacional_debitos",
  "simples_debitos",
  "debitos_simples_nacional",
];

const SIMPLES_GUIDE_DOCUMENT_TYPE = "GUIA_SIMPLES_DAS";

function normalizeToken(value: string) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .trim()
    .toLowerCase();
}

function sanitizeDiskSegment(value: string) {
  const cleaned = String(value ?? "")
    .replace(/[<>:"/\\|?*\u0000-\u001F]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return cleaned;
}

function splitLogicalSegments(value: string | null | undefined) {
  return String(value ?? "")
    .split(/[\\/]/)
    .map((segment) => sanitizeDiskSegment(segment))
    .filter(Boolean);
}

type FolderStructureLookupRow = {
  id: string;
  parent_id: string | null;
  name: string | null;
  slug: string | null;
};

let officeFolderRowsPromise: Promise<FolderStructureLookupRow[]> | null = null;

async function loadOfficeFolderRows(): Promise<FolderStructureLookupRow[]> {
  if (!officeFolderRowsPromise) {
    officeFolderRowsPromise = (async () => {
      const context = await getCurrentOfficeContext().catch(() => null);
      if (!context?.officeId) return [];
      const { data, error } = await supabase
        .from("folder_structure_nodes")
        .select("id, parent_id, name, slug")
        .eq("office_id", context.officeId)
        .order("parent_id", { nullsFirst: true })
        .order("position", { ascending: true });
      if (error) throw error;
      return (data ?? []) as FolderStructureLookupRow[];
    })();
  }
  return officeFolderRowsPromise;
}

async function resolvePhysicalFolderSegments(
  logicalPath: string | null | undefined,
): Promise<string[]> {
  const segments = splitLogicalSegments(logicalPath);
  if (segments.length === 0) return [];

  const rows = await loadOfficeFolderRows().catch(() => []);
  if (rows.length === 0) return [];

  const byParentAndSlug = new Map<string, FolderStructureLookupRow>();
  for (const row of rows) {
    const slug = normalizeToken(String(row.slug ?? row.name ?? ""));
    const key = `${row.parent_id ?? "root"}:${slug}`;
    byParentAndSlug.set(key, row);
  }

  const names: string[] = [];
  let parentId: string | null = null;
  for (const segment of segments) {
    const key = `${parentId ?? "root"}:${normalizeToken(segment)}`;
    const current = byParentAndSlug.get(key) ?? null;
    if (!current) return [];
    names.push(sanitizeDiskSegment(String(current.name ?? segment)));
    parentId = current.id;
  }

  return names;
}

function buildDateSegments(
  dateRule: string | null | undefined,
  reference: string | null | undefined,
) {
  const rawReference = String(reference ?? "").trim();
  if (!dateRule || !rawReference) return [] as string[];

  if (/^\d{4}-\d{2}$/.test(rawReference)) {
    const year = rawReference.slice(0, 4);
    const month = rawReference.slice(5, 7);
    if (dateRule === "year") return [year];
    if (dateRule === "year_month" || dateRule === "year_month_day") return [year, month];
    return [];
  }

  if (/^\d{4}$/.test(rawReference) && dateRule === "year") {
    return [rawReference];
  }

  return [];
}

function buildExpectedArtifactFileNames(
  action: DeclarationActionKind,
  reference: string | null | undefined,
) {
  const rawReference = String(reference ?? "").trim();
  if (!rawReference) return [] as string[];

  if (action === "simples_extrato") {
    return [`Extrato do Simples - ${rawReference}.pdf`];
  }

  if (action === "simples_defis") {
    return [
      `DEFIS - ${rawReference} - Declaracao.pdf`,
      `DEFIS - ${rawReference} - Recibo.pdf`,
    ];
  }

  if (action === "simples_emitir_guia") {
    return [`DAS - ${rawReference}.pdf`];
  }

  return [];
}

function matchesDeclarationArtifactByAction(action: DeclarationActionKind, fileName: string) {
  const normalizedName = normalizeToken(fileName);
  if (!normalizedName) return false;

  const isPdf = normalizedName.endsWith(".pdf");
  switch (action) {
    case "simples_extrato":
      return isPdf && normalizedName.startsWith("extrato do simples");
    case "simples_defis":
      return (
        isPdf &&
        normalizedName.startsWith("defis") &&
        (normalizedName.includes("recibo") || normalizedName.includes("declaracao"))
      );
    case "simples_emitir_guia":
      return isPdf && normalizedName.startsWith("das ");
    default:
      return true;
  }
}

function filterDeclarationArtifactsByAction(
  action: DeclarationActionKind,
  response: DeclarationArtifactListResponse,
): DeclarationArtifactListResponse {
  return {
    ...response,
    items: (response.items ?? []).filter((item) =>
      matchesDeclarationArtifactByAction(action, String(item.file_name ?? ""))),
  };
}

function extractYearFromArtifactName(fileName: string) {
  const match = normalizeToken(fileName).match(/\b(20\d{2})\b/);
  return match ? match[1] : null;
}

function matchesDeclarationArtifactReference(
  action: DeclarationActionKind,
  fileName: string,
  reference: string | null | undefined,
) {
  const rawReference = String(reference ?? "").trim();
  if (!rawReference) return true;

  if (action === "simples_extrato") {
    return normalizeToken(fileName).includes(normalizeToken(rawReference));
  }

  if (action === "simples_defis") {
    return extractYearFromArtifactName(fileName) === rawReference;
  }

  if (action === "simples_emitir_guia") {
    const aliases = new Set<string>([rawReference]);
    const competenceMatch = rawReference.match(/^(\d{4})-(\d{2})$/);
    if (competenceMatch) {
      aliases.add(`${competenceMatch[2]}-${competenceMatch[1]}`);
      aliases.add(`${competenceMatch[2]}/${competenceMatch[1]}`);
      aliases.add(`${competenceMatch[1]}/${competenceMatch[2]}`);
    }
    const normalizedFileName = normalizeToken(fileName);
    return Array.from(aliases).some((alias) => normalizedFileName.includes(normalizeToken(alias)));
  }

  return true;
}

function pickArtifactForAction(
  action: DeclarationActionKind,
  items: DeclarationArtifactListResponse["items"],
  reference: string | null | undefined,
) {
  const filtered = items.filter((item) =>
    matchesDeclarationArtifactReference(action, String(item.file_name ?? ""), reference));
  if (filtered.length === 0) return null;

  if (action === "simples_defis") {
    return (
      filtered.find((item) => normalizeToken(item.file_name).includes("declaracao")) ??
      filtered.find((item) => normalizeToken(item.file_name).includes("recibo")) ??
      filtered[0]
    );
  }

  return filtered[0];
}

function uniqueCompanyIds(companyIds: string[]) {
  return Array.from(new Set(companyIds.map((item) => String(item ?? "").trim()).filter(Boolean)));
}

function normalizeRobotCandidates(values: string[]) {
  return values.map((value) => normalizeToken(value));
}

function parsePortalDateToIso(value: unknown): string | null {
  const raw = String(value ?? "").trim();
  const match = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!match) return null;
  return `${match[3]}-${match[2]}-${match[1]}`;
}

function parsePortalCompetence(value: unknown): string | null {
  const raw = String(value ?? "").trim();
  const match = raw.match(/^(\d{2})\/(\d{4})$/);
  if (!match) return null;
  return `${match[2]}-${match[1]}`;
}

function parseCurrencyToCents(value: unknown): number | null {
  const raw = String(value ?? "").trim();
  if (!raw) return null;
  const normalized = raw.replace(/\./g, "").replace(",", ".");
  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? Math.round(parsed * 100) : null;
}

function basenameFromPath(value: string | null | undefined) {
  const raw = String(value ?? "").trim();
  if (!raw) return "";
  const parts = raw.split(/[\\/]/);
  return parts[parts.length - 1] ?? "";
}

function toRelativeOfficePath(filePath: string | null | undefined, summary?: Record<string, Json>) {
  const rawPath = String(filePath ?? "").trim();
  if (!rawPath) return "";
  if (!/^[a-z]:[\\/]/i.test(rawPath) && !rawPath.startsWith("\\\\")) {
    return rawPath.replace(/^[/\\]+/, "");
  }

  const connector = asObject(summary?.connector ?? null);
  const basePath = String(connector.base_path ?? "").trim();
  if (!basePath) return rawPath;

  const normalizedBase = basePath.replace(/[\\/]+/g, "\\").replace(/[\\]+$/, "").toLowerCase();
  const normalizedRaw = rawPath.replace(/[\\/]+/g, "\\");
  if (!normalizedRaw.toLowerCase().startsWith(`${normalizedBase}\\`)) {
    return rawPath;
  }

  return normalizedRaw.slice(normalizedBase.length).replace(/^[/\\]+/, "");
}

function asRecordArray(value: Json | null | undefined): Record<string, Json>[] {
  return asArray(value).map((item) => asObject(item));
}

function findRobotByCandidates(robots: Robot[], candidates: string[], matchers: string[]) {
  const normalizedCandidates = normalizeRobotCandidates(candidates);
  const exact = robots.find((robot) => normalizedCandidates.includes(normalizeToken(robot.technical_id)));
  if (exact) return exact;

  return (
    robots.find((robot) => {
      const haystack = normalizeToken(`${robot.technical_id} ${robot.display_name}`);
      return matchers.every((token) => haystack.includes(normalizeToken(token)));
    }) ?? null
  );
}

function mapRunItems(items: DeclarationRunItem[]) {
  return new Map(items.map((item) => [item.companyId, item]));
}

function hasArtifact(item: DeclarationRunItem | DeclarationRunHistoryEntry) {
  return Boolean(item.artifact?.filePath || item.artifact?.url || item.artifact?.artifactKey);
}

function stabilizeRunItem(previous: DeclarationRunItem | undefined, incoming: DeclarationRunItem): DeclarationRunItem {
  if (!previous || previous.status !== "sucesso" || !hasArtifact(previous)) {
    return incoming;
  }
  return {
    ...incoming,
    status: "sucesso",
    message: previous.message || incoming.message || "PDF gerado e disponível para download.",
    artifact: previous.artifact,
    meta: incoming.meta ?? previous.meta ?? null,
  };
}

function stabilizeRunItems(previousItems: DeclarationRunItem[], incomingItems: DeclarationRunItem[]) {
  const previousByKey = new Map(
    previousItems.map((item) => [`${item.companyId}:${item.executionRequestId ?? ""}`, item] as const),
  );
  return incomingItems.map((item) =>
    stabilizeRunItem(previousByKey.get(`${item.companyId}:${item.executionRequestId ?? ""}`), item),
  );
}

function runNeedsArtifactResolution(run: DeclarationRunState) {
  return run.items.some((item) => item.status === "sucesso" && !hasArtifact(item));
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

function getRunCounters(items: DeclarationRunItem[]) {
  const successCount = items.filter((item) => item.status === "sucesso").length;
  const errorCount = items.filter((item) => item.status === "erro").length;
  const processingCount = items.filter((item) => item.status === "processando").length;
  return {
    itemsTotal: items.length,
    itemsSuccess: successCount,
    itemsError: errorCount,
    itemsProcessing: processingCount,
  };
}

function getRunHistoryStatus(run: DeclarationRunState): "processando" | "sucesso" | "divergente" {
  if (!run.terminal) return "processando";
  return run.items.some((item) => item.status === "erro") ? "divergente" : "sucesso";
}

function sanitizeRunItems(value: Json | null | undefined): DeclarationRunItem[] {
  return asArray(value).map((entry) => {
    const row = asObject(entry);
    const artifactRaw = asObject(row.artifact ?? null);
    const artifact =
      Object.keys(artifactRaw).length > 0
        ? {
            label: String(artifactRaw.label ?? "").trim(),
            filePath: artifactRaw.filePath == null ? null : String(artifactRaw.filePath ?? ""),
            url: artifactRaw.url == null ? null : String(artifactRaw.url ?? ""),
            artifactKey: artifactRaw.artifactKey == null ? null : String(artifactRaw.artifactKey ?? ""),
          }
        : null;
    return {
      companyId: String(row.companyId ?? ""),
      companyName: String(row.companyName ?? ""),
      companyDocument: row.companyDocument == null ? null : String(row.companyDocument ?? ""),
      status: (["pendente", "processando", "sucesso", "erro"].includes(String(row.status ?? ""))
        ? String(row.status)
        : "pendente") as DeclarationRunItem["status"],
      message: String(row.message ?? ""),
      executionRequestId: row.executionRequestId == null ? null : String(row.executionRequestId ?? ""),
      artifact,
      meta: (row.meta as Json | undefined) ?? null,
    };
  });
}

function mapHistoryRowToRun(row: DeclarationRunHistoryRow): DeclarationRunState {
  const payload = asObject(row.payload);
  const action = String(payload.action ?? row.action) as DeclarationRunState["action"];
  const mode = String(payload.mode ?? row.mode) as DeclarationRunState["mode"];
  const requestIds = Array.isArray(row.request_ids) ? row.request_ids.map((value) => String(value)) : [];
  return {
    runId: String(row.run_id),
    action,
    mode,
    title: String(payload.title ?? row.title ?? ACTION_TITLES[action]),
    requestIds,
    items: sanitizeRunItems(payload.items),
    startedAt: String(payload.startedAt ?? row.started_at ?? row.created_at),
    finishedAt: payload.finishedAt == null ? row.finished_at : String(payload.finishedAt ?? ""),
    terminal: Boolean(payload.terminal ?? row.status !== "processando"),
  };
}

function formatReferenceLabel(action: DeclarationActionKind, reference: string | null) {
  if (!reference) return "-";
  return action === "simples_defis" ? formatYearLabel(reference) : formatCompetenceLabel(reference);
}

function formatAmountLabel(amountCents: number | null | undefined) {
  return formatCurrencyFromCents(amountCents);
}

export function buildDeclarationRunHistoryEntries(runs: DeclarationRunState[]): DeclarationRunHistoryEntry[] {
  return runs.flatMap((run) =>
    run.items.map((item, index) => {
      const summary = asObject(item.meta ?? null);
      const amountCents = extractAmountCentsFromSummary(run.action, summary);
      return {
        entryId: `${run.runId}:${index}`,
        runId: run.runId,
        action: run.action,
        mode: run.mode,
        title: run.title,
        requestIds: run.requestIds,
        startedAt: run.startedAt,
        finishedAt: run.finishedAt,
        terminal: run.terminal,
        referenceLabel: formatReferenceLabel(
          run.action,
          extractReferenceFromSummary(run.action, summary),
        ),
        dueDateLabel: extractDueDateFromSummary(run.action, summary),
        amountLabel: formatAmountLabel(amountCents),
        amountCents,
        companyId: item.companyId,
        companyName: item.companyName,
        companyDocument: item.companyDocument,
        status: item.status,
        message: item.message,
        executionRequestId: item.executionRequestId,
        artifact: item.artifact ?? null,
        meta: item.meta ?? null,
      };
    }),
  );
}

async function listAllDeclarationRunHistoryRows(params?: {
  actions?: DeclarationActionKind[];
}): Promise<DeclarationRunHistoryRow[]> {
  const chunkSize = 200;
  let from = 0;
  const rows: DeclarationRunHistoryRow[] = [];
  const actions = Array.from(new Set((params?.actions ?? []).map((value) => String(value).trim()).filter(Boolean)));

  while (true) {
    let query = supabase
      .from("declaration_run_history")
      .select("*")
      .order("started_at", { ascending: false })
      .order("created_at", { ascending: false })
      .range(from, from + chunkSize - 1);

    if (actions.length > 0) {
      query = query.in("action", actions);
    }

    const { data, error } = await query;
    if (error) throw error;

    const chunk = (data ?? []) as DeclarationRunHistoryRow[];
    rows.push(...chunk);
    if (chunk.length < chunkSize) {
      break;
    }
    from += chunkSize;
  }

  return rows;
}

export async function listAllDeclarationRunHistoryRuns(params?: {
  actions?: DeclarationActionKind[];
}): Promise<DeclarationRunState[]> {
  const rows = await listAllDeclarationRunHistoryRows(params);
  return Promise.all(
    rows.map(async (row) => hydrateDeclarationRunArtifacts(await hydrateGuideMetadataForRun(mapHistoryRowToRun(row)))),
  );
}

function findRobotForAction(action: DeclarationActionKind, robots: Robot[]): Robot | null {
  return findRobotByCandidates(robots, ACTION_ROBOT_CANDIDATES[action], ACTION_TEXT_MATCHERS[action]);
}

function buildActionUnavailable(reason: string): DeclarationActionAvailability {
  return { enabled: false, reason, robotTechnicalId: null };
}

function canDispatchToRobotNow(robot: Robot | null) {
  return Boolean(robot && (robot.status === "active" || robot.status === "processing"));
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
  if (params.action === "simples_emitir_guia" && !canDispatchToRobotNow(params.robot)) {
    return {
      enabled: false,
      reason: "O robô de emitir guia está inativo no servidor do escritório. Abra o robô antes de solicitar a emissão.",
      robotTechnicalId: params.robot.technical_id,
    };
  }
  return {
    enabled: true,
    reason: null,
    robotTechnicalId: params.robot.technical_id,
  };
}

async function getOverdueGuides(companyIds: string[]): Promise<OverdueGuide[]> {
  // A base atual ainda não expõe uma fonte dedicada para DAS vencidas.
  // A UI já fica preparada para consumir a lista assim que o backend for disponibilizado.
  const normalizedCompanyIds = uniqueCompanyIds(companyIds);
  if (normalizedCompanyIds.length === 0) return [];

  const robots = await getRobots().catch(() => []);
  const debitRobot = findRobotByCandidates(
    robots,
    OVERDUE_GUIDES_ROBOT_CANDIDATES,
    ["simples", "debito"],
  );
  if (!debitRobot) return [];

  const { data, error } = await supabase
    .from("robot_result_events")
    .select("created_at, company_results")
    .eq("robot_technical_id", debitRobot.technical_id)
    .eq("status", "completed")
    .order("created_at", { ascending: false })
    .limit(200);
  if (error) throw error;

  const todayIso = new Date().toISOString().slice(0, 10);
  const latestByCompany = new Map<string, Record<string, Json>>();
  for (const row of data ?? []) {
    const companyResults = asRecordArray((row as { company_results?: Json | null }).company_results);
    for (const companyResult of companyResults) {
      const companyId = String(companyResult.company_id ?? "").trim();
      if (!companyId || latestByCompany.has(companyId) || !normalizedCompanyIds.includes(companyId)) continue;
      latestByCompany.set(companyId, companyResult);
    }
    if (latestByCompany.size >= normalizedCompanyIds.length) break;
  }

  const guides: OverdueGuide[] = [];
  for (const companyId of normalizedCompanyIds) {
    const companyResult = latestByCompany.get(companyId);
    if (!companyResult) continue;

    const companyName = String(companyResult.company_name ?? "").trim();
    const companyDocument = String(companyResult.company_document ?? "").trim() || null;
    const records = asRecordArray(companyResult.records);

    for (const record of records) {
      const competence = parsePortalCompetence(record.periodo_apuracao ?? record.competencia);
      const dueDate = parsePortalDateToIso(record.data_vencimento);
      if (!competence || !dueDate || dueDate > todayIso) continue;

      const parcelamento = Number(record.numero_parcelamento ?? 0);
      if (Number.isFinite(parcelamento) && parcelamento !== 0) continue;

      guides.push({
        id: `${companyId}:${competence}`,
        companyId,
        companyName,
        companyDocument,
        competence,
        dueDate,
        status: "vencido",
        amountCents: parseCurrencyToCents(record.total ?? record.saldo_devedor ?? record.debito_declarado),
        referenceLabel: null,
      });
    }
  }

  guides.sort((left, right) => {
    const dueCompare = String(left.dueDate).localeCompare(String(right.dueDate));
    if (dueCompare !== 0) return dueCompare;
    return left.companyName.localeCompare(right.companyName, "pt-BR");
  });
  return guides;
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
  const requiresYear = params.action === "simples_defis";
  const requiresCompetence =
    params.action === "simples_emitir_guia"
    || params.action === "simples_extrato"
    || params.action === "mei_guias_mensais";
  if (requiresCompetence && !isValidCompetence(rawCompetence)) {
    throw new Error("Informe uma competência válida no formato MM/AAAA.");
  }
  if (requiresYear && !isValidYear(rawCompetence)) {
    throw new Error("Informe um ano valido para a DEFIS.");
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

function resolveCompanyMessage(summary: Record<string, Json>, fallback: string) {
  const errors = asArray(summary.errors)
    .map((item) => String(item ?? "").trim())
    .filter(Boolean);
  if (errors.length > 0) return errors[0];

  const portalMessage = String(
    asObject(asArray(summary.records)[0] ?? null).portal_message ?? "",
  ).trim();
  if (portalMessage) return portalMessage;

  const warnings = asArray(summary.warnings)
    .map((item) => String(item ?? "").trim())
    .filter(Boolean);
  if (warnings.length > 0) return warnings[0];

  return normalizeSummaryMessage(summary, fallback);
}

function mapCompanyResultStatus(value: string): DeclarationRunItem["status"] {
  const normalized = normalizeToken(value);
  if (["error", "erro", "failed", "falha", "blocked", "bloqueado", "interrupted", "interrompido", "cancelled", "cancelado"].includes(normalized)) {
    return "erro";
  }
  return "sucesso";
}

function extractReferenceFromSummary(action: DeclarationActionKind, summary: Record<string, Json>) {
  if (action === "simples_defis") {
    const directYear = String(summary.year ?? summary.ano ?? "").trim();
    if (/^\d{4}$/.test(directYear)) return directYear;

    const record = asObject(asArray(summary.records)[0] ?? null);
    const recordYear = String(record.year ?? record.ano ?? "").trim();
    if (/^\d{4}$/.test(recordYear)) return recordYear;
    return null;
  }

  const direct = String(summary.competencia ?? summary.competence ?? "").trim();
  if (/^\d{4}-\d{2}$/.test(direct)) return direct;
  const normalizedDirect = parsePortalCompetence(direct);
  if (normalizedDirect) return normalizedDirect;

  const record = asObject(asArray(summary.records)[0] ?? null);
  const recordCompetence = String(record.competencia ?? record.competence ?? record.periodo_apuracao ?? "").trim();
  if (/^\d{4}-\d{2}$/.test(recordCompetence)) return recordCompetence;
  return parsePortalCompetence(recordCompetence);
}

function formatHistoryDateLabel(value: string | null | undefined) {
  const raw = String(value ?? "").trim();
  if (!raw) return "-";

  if (/^\d{2}\/\d{2}\/\d{4}$/.test(raw)) {
    return raw;
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    const [year, month, day] = raw.split("-");
    return `${day}/${month}/${year}`;
  }

  const normalized = parsePortalDateToIso(raw);
  if (normalized) {
    return formatHistoryDateLabel(normalized);
  }

  return "-";
}

function extractDueDateFromSummary(
  action: DeclarationActionKind,
  summary: Record<string, Json>,
) {
  if (action !== "simples_emitir_guia") return "-";

  const directCandidates = [
    summary.data_vencimento_guia,
    summary.guia_data_vencimento,
    summary.data_vencimento_pdf,
    summary.data_vencimento,
  ];
  for (const candidate of directCandidates) {
    const label = formatHistoryDateLabel(candidate == null ? null : String(candidate));
    if (label !== "-") return label;
  }

  const record = asObject(asArray(summary.records)[0] ?? null);
  const recordCandidates = [
    record.data_vencimento_guia,
    record.guia_data_vencimento,
    record.data_vencimento_pdf,
    record.data_vencimento,
  ];
  for (const candidate of recordCandidates) {
    const label = formatHistoryDateLabel(candidate == null ? null : String(candidate));
    if (label !== "-") return label;
  }

  return "-";
}

function extractAmountCentsFromSummary(
  action: DeclarationActionKind,
  summary: Record<string, Json>,
): number | null {
  if (action !== "simples_emitir_guia") return null;

  const directCandidates = [
    summary.amount_cents,
    summary.valor_total_centavos,
    summary.valor_total_cents,
    summary.valor_total,
    summary.saldo_devedor,
  ];
  for (const candidate of directCandidates) {
    const parsed =
      typeof candidate === "number"
        ? Math.trunc(candidate)
        : parseCurrencyToCents(candidate == null ? null : String(candidate));
    if (Number.isFinite(parsed)) return Number(parsed);
  }

  const record = asObject(asArray(summary.records)[0] ?? null);
  const recordCandidates = [
    record.amount_cents,
    record.valor_total_centavos,
    record.valor_total_cents,
    record.valor_total,
    record.total,
    record.saldo_devedor,
    record.debito_declarado,
  ];
  for (const candidate of recordCandidates) {
    const parsed =
      typeof candidate === "number"
        ? Math.trunc(candidate)
        : parseCurrencyToCents(candidate == null ? null : String(candidate));
    if (Number.isFinite(parsed)) return Number(parsed);
  }

  return null;
}

function extractArtifact(summary: Record<string, Json>) {
  const filePath = toRelativeOfficePath(String(summary.file_path ?? summary.document_path ?? "").trim(), summary);
  const url = String(summary.download_url ?? summary.file_url ?? summary.document_url ?? "").trim();
  const label =
    String(summary.document_name ?? summary.file_name ?? summary.filename ?? "").trim() || "Baixar documento";
  if (filePath || url) {
    return {
      label,
      filePath: filePath || null,
      url: url || null,
    };
  }

  const files = asRecordArray(summary.files);
  const firstFile = files[0];
  if (!firstFile) return null;

  const firstPath = toRelativeOfficePath(
    String(firstFile.file_path ?? firstFile.relative_path ?? firstFile.path ?? "").trim(),
    summary,
  );
  const firstUrl = String(firstFile.url ?? firstFile.download_url ?? "").trim();
  const firstLabel = String(firstFile.label ?? firstFile.filename ?? firstFile.file_name ?? "").trim() || "Baixar documento";
  if (!firstPath && !firstUrl) return null;

  return {
    label: firstLabel,
    filePath: firstPath || null,
    url: firstUrl || null,
  };
}

function buildGuideLookupKey(companyId: string, competence: string) {
  return `${String(companyId ?? "").trim()}:${String(competence ?? "").trim()}`;
}

function mergeGuideMetadataIntoSummary(
  summary: Record<string, Json>,
  document: DeclarationGuideDocumentListItem,
): Record<string, Json> {
  return {
    ...summary,
    document_id: summary.document_id ?? document.documentId,
    document_type: summary.document_type ?? SIMPLES_GUIDE_DOCUMENT_TYPE,
    competencia: summary.competencia ?? document.competence,
    competence: summary.competence ?? document.competence,
    data_vencimento: summary.data_vencimento ?? document.dueDate,
    data_vencimento_pdf: summary.data_vencimento_pdf ?? document.dueDate,
    amount_cents: summary.amount_cents ?? document.amountCents,
    valor_total: summary.valor_total ?? (document.amountCents != null ? formatAmountLabel(document.amountCents) : null),
    checksum: summary.checksum ?? document.checksum,
    parser_version: summary.parser_version ?? document.parserVersion,
    parsed_at: summary.parsed_at ?? document.parsedAt,
    file_path: summary.file_path ?? document.filePath,
    document_path: summary.document_path ?? document.filePath,
    document_name: summary.document_name ?? document.fileName,
    file_name: summary.file_name ?? document.fileName,
  };
}

async function hydrateGuideMetadataForItems(
  action: DeclarationActionKind,
  items: DeclarationRunItem[],
): Promise<DeclarationRunItem[]> {
  if (action !== "simples_emitir_guia" || items.length === 0) {
    return items;
  }

  const lookupRequests = new Map<string, { companyIds: Set<string>; items: DeclarationRunItem[] }>();
  for (const item of items) {
    const summary = asObject(item.meta ?? null);
    const needsMetadata =
      extractDueDateFromSummary(action, summary) === "-"
      || extractAmountCentsFromSummary(action, summary) == null
      || !item.artifact;
    if (!needsMetadata) continue;
    const competence = extractReferenceFromSummary(action, summary);
    if (!competence) continue;
    const year = competence.slice(0, 4);
    if (!/^\d{4}$/.test(year)) continue;
    const bucket = lookupRequests.get(year) ?? { companyIds: new Set<string>(), items: [] };
    bucket.companyIds.add(item.companyId);
    bucket.items.push(item);
    lookupRequests.set(year, bucket);
  }

  if (lookupRequests.size === 0) {
    return items;
  }

  const guideLookup = new Map<string, DeclarationGuideDocumentListItem>();
  await Promise.all(
    Array.from(lookupRequests.entries()).map(async ([year, bucket]) => {
      const firstPage = await listSimplesGuideDocuments({
        companyIds: Array.from(bucket.companyIds),
        year,
        page: 1,
        pageSize: 100,
        sortKey: "competencia",
        sortDirection: "desc",
        autoScan: false,
      }).catch(() => null);
      if (!firstPage) return;
      const responses = [firstPage];
      const totalPages = Math.max(1, Math.ceil(firstPage.total / firstPage.pageSize));
      for (let page = 2; page <= totalPages; page += 1) {
        const nextPage = await listSimplesGuideDocuments({
          companyIds: Array.from(bucket.companyIds),
          year,
          page,
          pageSize: firstPage.pageSize,
          sortKey: "competencia",
          sortDirection: "desc",
          autoScan: false,
        }).catch(() => null);
        if (!nextPage) break;
        responses.push(nextPage);
      }
      for (const document of responses.flatMap((response) => response.items)) {
        if (!document.competence) continue;
        guideLookup.set(buildGuideLookupKey(document.companyId, document.competence), document);
      }
    }),
  );

  if (guideLookup.size === 0) {
    return items;
  }

  return items.map((item) => {
    const summary = asObject(item.meta ?? null);
    const competence = extractReferenceFromSummary(action, summary);
    if (!competence) return item;
    const document = guideLookup.get(buildGuideLookupKey(item.companyId, competence));
    if (!document) return item;
    const mergedMeta = mergeGuideMetadataIntoSummary(summary, document);
    const artifact = item.artifact ?? {
      label: document.fileName || "Baixar documento",
      filePath: document.filePath || null,
      url: null,
      artifactKey: null,
    };
    return {
      ...item,
      artifact,
      meta: mergedMeta,
    };
  });
}

async function hydrateGuideMetadataForRun(run: DeclarationRunState): Promise<DeclarationRunState> {
  if (run.action !== "simples_emitir_guia" || run.items.length === 0) {
    return run;
  }
  const items = await hydrateGuideMetadataForItems(run.action, run.items);
  if (items === run.items) return run;
  return { ...run, items };
}

async function resolveArtifactsFromStorage(params: {
  action: DeclarationActionKind;
  items: DeclarationRunItem[];
}): Promise<Map<string, DeclarationRunItem["artifact"]>> {
  const successItems = params.items.filter((item) => item.status === "sucesso" && !item.artifact);
  if (successItems.length === 0) return new Map();

  const byCompanyId = new Map<string, DeclarationRunItem["artifact"]>();
  const grouped = new Map<string, DeclarationRunItem[]>();
  for (const item of successItems) {
    const reference = extractReferenceFromSummary(params.action, asObject(item.meta ?? null)) ?? "";
    const key = reference || "__all__";
    const bucket = grouped.get(key) ?? [];
    bucket.push(item);
    grouped.set(key, bucket);
  }

  for (const [reference, items] of grouped.entries()) {
    const response = await listDeclarationArtifacts({
      action: params.action,
      companyIds: items.map((item) => item.companyId),
      competence: null,
      limit: 500,
    }).catch(() => null);
    if (!response) continue;

    const itemsByCompany = new Map<string, DeclarationArtifactListResponse["items"]>();
    for (const artifact of response.items ?? []) {
      const bucket = itemsByCompany.get(artifact.company_id) ?? [];
      bucket.push(artifact);
      itemsByCompany.set(artifact.company_id, bucket);
    }

    for (const item of items) {
      const meta = asObject(item.meta ?? null);
      const targetNames = asRecordArray(meta.files)
        .map((entry) => basenameFromPath(String(entry.filename ?? entry.file_name ?? entry.path ?? entry.file_path ?? "")))
        .filter(Boolean);
      const artifactItems = itemsByCompany.get(item.companyId) ?? [];
      const matchedArtifact =
        artifactItems.find((artifact) => targetNames.includes(basenameFromPath(artifact.file_name))) ??
        pickArtifactForAction(params.action, artifactItems, reference === "__all__" ? null : reference);
      if (!matchedArtifact) continue;

      byCompanyId.set(item.companyId, {
        label: matchedArtifact.file_name || "Baixar documento",
        filePath: null,
        url: null,
        artifactKey: matchedArtifact.artifact_key,
      });
    }
  }

  return byCompanyId;
}

type ExecutionRuntimeProgress = {
  current: number;
  companyId: string | null;
  companyName: string | null;
  status: string;
  companyResults: Record<string, Json>[];
};

async function getRuntimeProgressByRequestIds(
  requestIds: string[],
): Promise<Map<string, ExecutionRuntimeProgress>> {
  const normalizedRequestIds = Array.from(new Set(requestIds.map((value) => String(value ?? "").trim()).filter(Boolean)));
  if (normalizedRequestIds.length === 0) return new Map();

  const { data, error } = await supabase
    .from("office_robot_runtime")
    .select("current_execution_request_id, status, heartbeat_payload, updated_at")
    .in("current_execution_request_id", normalizedRequestIds);
  if (error) throw error;

  const progressByRequestId = new Map<string, ExecutionRuntimeProgress>();
  for (const row of (data ?? []) as Array<Record<string, unknown>>) {
    const requestId = String(row.current_execution_request_id ?? "").trim();
    if (!requestId) continue;

    const heartbeat = asObject((row.heartbeat_payload as Json | undefined) ?? null);
    const progress = asObject(heartbeat.progress ?? null);
    const currentRaw = Number(progress.current ?? 0);
    const current = Number.isFinite(currentRaw) && currentRaw > 0 ? Math.trunc(currentRaw) : 0;
    const companyId = String(progress.company_id ?? "").trim() || null;
    const companyName = String(progress.company_name ?? "").trim() || null;
    const status = String(row.status ?? heartbeat.status ?? "").trim().toLowerCase() || "inactive";
    const companyResults = asRecordArray(progress.company_results ?? heartbeat.company_results ?? null);
    const updatedAt = Date.parse(String(row.updated_at ?? ""));
    const existing = progressByRequestId.get(requestId);
    const existingUpdatedAt = existing ? Date.parse(String((existing as { updatedAt?: string }).updatedAt ?? "")) : Number.NaN;

    if (!existing || (!Number.isNaN(updatedAt) && (Number.isNaN(existingUpdatedAt) || updatedAt >= existingUpdatedAt))) {
      progressByRequestId.set(
        requestId,
        {
          current,
          companyId,
          companyName,
          status,
          companyResults,
          updatedAt: String(row.updated_at ?? ""),
        } as ExecutionRuntimeProgress & { updatedAt: string },
      );
    }
  }

  return new Map(
    Array.from(progressByRequestId.entries()).map(([requestId, progress]) => [
      requestId,
      {
        current: progress.current,
        companyId: progress.companyId,
        companyName: progress.companyName,
        status: progress.status,
        companyResults: progress.companyResults,
      },
    ]),
  );
}

async function promoteSequentialRuntimeArtifacts(params: {
  action: DeclarationActionKind;
  items: DeclarationRunItem[];
  requestIds: string[];
}): Promise<DeclarationRunItem[]> {
  const progressByRequestId = await getRuntimeProgressByRequestIds(params.requestIds).catch(() => new Map());
  if (progressByRequestId.size === 0) return params.items;

  const candidateCompanyIds = new Set<string>();
  const candidateItems = new Map<string, DeclarationRunItem>();

  for (const requestId of params.requestIds) {
    const progress = progressByRequestId.get(requestId);
    if (!progress || progress.status !== "processing") continue;

    const requestItems = params.items.filter((item) => item.executionRequestId === requestId);
    if (requestItems.length === 0) continue;

    let completedCount = progress.current > 0 ? progress.current - 1 : 0;
    if (progress.companyId) {
      const currentIndex = requestItems.findIndex((item) => item.companyId === progress.companyId);
      if (currentIndex >= 0) {
        completedCount = Math.max(completedCount, currentIndex);
      }
    }

    for (const item of requestItems.slice(0, Math.min(completedCount, requestItems.length))) {
      if (item.status === "sucesso" || item.status === "erro") continue;
      candidateCompanyIds.add(item.companyId);
      candidateItems.set(item.companyId, item);
    }
  }

  if (candidateItems.size === 0) return params.items;

  const provisionalItems = Array.from(candidateItems.values()).map((item) => ({
    ...item,
    status: "sucesso" as const,
    artifact: item.artifact ?? extractArtifact(asObject(item.meta ?? null)),
  }));
  const provisionalArtifactMap = new Map(
    provisionalItems
      .filter((item) => hasArtifact(item))
      .map((item) => [item.companyId, item.artifact ?? null] as const),
  );
  const unresolvedItems = provisionalItems.filter((item) => !provisionalArtifactMap.has(item.companyId));
  const storageArtifactMap =
    unresolvedItems.length > 0
      ? await resolveArtifactsFromStorage({
          action: params.action,
          items: unresolvedItems,
        }).catch(() => new Map<string, DeclarationRunItem["artifact"]>())
      : new Map<string, DeclarationRunItem["artifact"]>();

  if (provisionalArtifactMap.size === 0 && storageArtifactMap.size === 0) {
    return params.items;
  }

  return params.items.map((item) => {
    if (!candidateCompanyIds.has(item.companyId) || item.status === "erro") return item;
    const artifact = item.artifact ?? provisionalArtifactMap.get(item.companyId) ?? storageArtifactMap.get(item.companyId) ?? null;
    if (!artifact) return item;
    return {
      ...item,
      status: "sucesso",
      message: "PDF gerado e disponível para download.",
      artifact,
    };
  });
}

export async function hydrateDeclarationRunArtifacts(run: DeclarationRunState): Promise<DeclarationRunState> {
  const runWithGuideMetadata = await hydrateGuideMetadataForRun(run);

  if (!runNeedsArtifactResolution(runWithGuideMetadata)) {
    return runWithGuideMetadata;
  }

  const extractedItems = runWithGuideMetadata.items.map((item) => ({
    ...item,
    artifact: item.artifact ?? extractArtifact(asObject(item.meta ?? null)),
  }));

  if (!extractedItems.some((item) => item.status === "sucesso" && !item.artifact)) {
    return {
      ...runWithGuideMetadata,
      items: extractedItems,
    };
  }

  const artifactMap = await resolveArtifactsFromStorage({
    action: run.action,
    items: extractedItems,
  }).catch(() => new Map<string, DeclarationRunItem["artifact"]>());

  if (artifactMap.size === 0) {
    return {
      ...runWithGuideMetadata,
      items: extractedItems,
    };
  }

  return {
    ...runWithGuideMetadata,
    items: extractedItems.map((item) => ({
      ...item,
      artifact: item.artifact ?? artifactMap.get(item.companyId) ?? null,
    })),
  };
}

async function resolveStoredArtifactsBeforeDispatch(params: {
  action: DeclarationActionKind;
  companyIds: string[];
  reference: string | null;
  companies: DeclarationCompany[];
  robot: Robot | null;
}): Promise<Map<string, DeclarationRunItem["artifact"]>> {
  if (!["simples_extrato", "simples_defis"].includes(params.action)) {
    return new Map();
  }

  const response = await listDeclarationArtifacts({
    action: params.action,
    companyIds: params.companyIds,
    competence: null,
    limit: 500,
  }).catch(() => null);
  if (!response) return new Map();

  const byCompany = new Map<string, DeclarationArtifactListResponse["items"]>();
  for (const item of response.items ?? []) {
    const bucket = byCompany.get(item.company_id) ?? [];
    bucket.push(item);
    byCompany.set(item.company_id, bucket);
  }

  const matches = new Map<string, DeclarationRunItem["artifact"]>();
  const rawLogicalPath =
    response.source.logical_folder_path
    || response.source.segment_path
    || params.robot?.segment_path
    || "";
  const logicalSegments = splitLogicalSegments(rawLogicalPath);
  const physicalSegments = await resolvePhysicalFolderSegments(rawLogicalPath).catch(() => []);
  const dateSegments = buildDateSegments(response.source.date_rule, params.reference);
  const expectedFileNames = buildExpectedArtifactFileNames(params.action, params.reference);

  for (const companyId of params.companyIds) {
    const artifact = pickArtifactForAction(params.action, byCompany.get(companyId) ?? [], params.reference);
    if (artifact) {
      matches.set(companyId, {
        label: artifact.file_name || "Baixar documento",
        filePath: null,
        url: null,
        artifactKey: artifact.artifact_key,
      });
      continue;
    }

    const company = params.companies.find((entry) => entry.id === companyId);
    if (!company || expectedFileNames.length === 0) continue;

    const companySegment = sanitizeDiskSegment(company.name);
    const folderVariants = [physicalSegments, logicalSegments]
      .filter((segments) => segments.length > 0)
      .map((segments) => segments.join("/"))
      .filter((value, index, array) => array.indexOf(value) === index)
      .map((value) => value.split("/").filter(Boolean));
    const candidateDirectories = folderVariants
      .flatMap((segments) => [
        [companySegment, ...segments, ...dateSegments],
        [companySegment, ...segments],
      ])
      .filter((segments) => segments.length > 1)
      .map((segments) => segments.join("/"))
      .filter((value, index, array) => array.indexOf(value) === index);

    for (const directory of candidateDirectories) {
      let matchedFileName: string | null = null;
      for (const expectedFileName of expectedFileNames) {
        const relativePath = `${directory}/${expectedFileName}`.replace(/\\/g, "/");
        const probe = await probeServerFileByPath(relativePath).catch(() => ({ exists: false, filename: null }));
        if (!probe.exists) continue;
        matchedFileName = probe.filename ?? expectedFileName;
        matches.set(companyId, {
          label: matchedFileName,
          filePath: relativePath,
          url: null,
          artifactKey: null,
        });
        break;
      }
      if (matchedFileName) break;
    }
  }
  return matches;
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
  const robots = await getRobots().catch(() => []);
  const robot = findRobotForAction(params.action, robots);
  const storedArtifacts =
    await resolveStoredArtifactsBeforeDispatch({
      action: params.action,
      companyIds: selectedCompanies.map((company) => company.id),
      reference: validatedInput.competence ?? null,
      companies: selectedCompanies,
      robot,
    }).catch(() => new Map<string, DeclarationRunItem["artifact"]>());

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
  const emitGuideDispatchCompanies: DeclarationCompany[] = [];

  for (const company of selectedCompanies) {
    const skipReason = skippedByCompanyId.get(company.id);
    if (skipReason) {
      updateCompanyItem(company.id, {
        status: "erro",
        message: `Empresa sem elegibilidade operacional: ${skipReason}.`,
      });
      continue;
    }

    const storedArtifact = storedArtifacts.get(company.id);
    if (storedArtifact) {
      updateCompanyItem(company.id, {
        status: "sucesso",
        message:
          params.action === "simples_defis"
            ? "Documentos da DEFIS localizados na pasta da empresa."
            : "Documento localizado na pasta da empresa.",
        artifact: storedArtifact,
        meta: {
          competence: validatedInput.competence ?? null,
          competencia: validatedInput.competence ?? null,
          source: "stored_artifact",
        },
      });
      continue;
    }

    if (!canDispatchToRobotNow(robot)) {
      updateCompanyItem(company.id, {
        status: "erro",
        message:
          params.action === "simples_emitir_guia"
            ? "O robô de emitir guia está inativo no servidor do escritório. Abra o robô antes de solicitar a emissão."
            : "O robô desta rotina está inativo no servidor do escritório. Abra o robô antes de solicitar uma nova coleta.",
        meta: {
          robot_status: robot.status,
          robot_technical_id: robot.technical_id,
          blocked_by_robot_runtime: true,
        },
      });
      continue;
    }

    if (params.action === "simples_emitir_guia") {
      emitGuideDispatchCompanies.push(company);
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
        meta: {
          competence: validatedInput.competence ?? null,
          competencia: validatedInput.competence ?? null,
          recalculate: validatedInput.recalculate,
          recalculate_due_date: validatedInput.recalculateDueDate ?? null,
          robot_technical_id: robot.technical_id,
        },
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

  if (params.action === "simples_emitir_guia" && emitGuideDispatchCompanies.length > 0) {
    const shouldUsePeriod = Boolean(validatedInput.competence && isValidCompetence(validatedInput.competence));
    const { periodStart, periodEnd } = shouldUsePeriod
      ? toPeriodRange(validatedInput.competence!)
      : { periodStart: null, periodEnd: null };
    try {
      const request = await createExecutionRequest({
        companyIds: emitGuideDispatchCompanies.map((company) => company.id),
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
          company_ids: emitGuideDispatchCompanies.map((company) => company.id),
          company_documents: emitGuideDispatchCompanies.map((company) => company.document),
          company_names: emitGuideDispatchCompanies.map((company) => company.name),
        },
      });
      requestIds.push(request.id);
      for (const company of emitGuideDispatchCompanies) {
        updateCompanyItem(company.id, {
          status: "processando",
          executionRequestId: request.id,
          message: "Solicitação enviada para processamento.",
          meta: {
            competence: validatedInput.competence ?? null,
            competencia: validatedInput.competence ?? null,
            recalculate: validatedInput.recalculate,
            recalculate_due_date: validatedInput.recalculateDueDate ?? null,
            robot_technical_id: robot.technical_id,
          },
        });
      }
    } catch (error) {
      const message = sanitizeDeclarationError(
        error,
        "Não foi possível iniciar o processamento desta solicitação.",
      );
      for (const company of emitGuideDispatchCompanies) {
        updateCompanyItem(company.id, {
          status: "erro",
          message,
        });
      }
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
  if (current.requestIds.length === 0) {
    return hydrateDeclarationRunArtifacts(current);
  }
  if (current.terminal) {
    return hydrateDeclarationRunArtifacts(current);
  }

  const [{ data: requests, error: requestsError }, { data: resultEvents, error: eventsError }] =
    await Promise.all([
      supabase
        .from("execution_requests")
        .select("id, company_ids, status, error_message, result_summary, job_payload, completed_at, created_at")
        .in("id", current.requestIds),
      supabase
        .from("robot_result_events")
        .select("execution_request_id, status, summary, company_results, error_message, created_at")
        .in("execution_request_id", current.requestIds),
    ]);

  if (requestsError) throw requestsError;
  if (eventsError) throw eventsError;

  const runtimeProgressByRequestId = await getRuntimeProgressByRequestIds(current.requestIds).catch(
    () => new Map<string, ExecutionRuntimeProgress>(),
  );

  const requestById = new Map(
    (requests ?? []).map((row) => [row.id, row]),
  );
  const resultByExecutionId = new Map<string, Record<string, unknown>>();
  for (const row of (resultEvents ?? []) as Array<Record<string, unknown>>) {
    const requestId = String(row.execution_request_id ?? "").trim();
    if (!requestId) continue;
    const existing = resultByExecutionId.get(requestId);
    const existingCreatedAt = existing ? Date.parse(String(existing.created_at ?? "")) : Number.NaN;
    const nextCreatedAt = Date.parse(String(row.created_at ?? ""));
    if (!existing || (!Number.isNaN(nextCreatedAt) && (Number.isNaN(existingCreatedAt) || nextCreatedAt >= existingCreatedAt))) {
      resultByExecutionId.set(requestId, row);
    }
  }

  let nextItems = current.items.map((item) => {
    if (!item.executionRequestId) return item;
    const request = requestById.get(item.executionRequestId);
    if (!request) {
      const missingForMs = Date.now() - Date.parse(current.startedAt);
      if (Number.isFinite(missingForMs) && missingForMs >= 15_000) {
        return {
          ...item,
          status: "erro",
          message: "A solicitacao nao foi localizada na fila de execucao. Limpe o acompanhamento e solicite novamente.",
          meta: {
            ...(asObject(item.meta ?? null)),
            missing_execution_request: true,
            execution_request_id: item.executionRequestId,
          },
        };
      }
      return {
        ...item,
        status: "processando",
        message: "Solicitacao aguardando sincronizacao da fila.",
      };
    }

    const resultEvent = resultByExecutionId.get(item.executionRequestId);
    const runtimeProgress = runtimeProgressByRequestId.get(item.executionRequestId);
    const requestSummary = asObject((request as { result_summary?: Json | null }).result_summary);
    const requestJobPayload = asObject((request as { job_payload?: Json | null }).job_payload);
    const eventSummary = asObject((resultEvent as { summary?: Json | null } | undefined)?.summary);
    const eventCompanyResults = asRecordArray(
      (resultEvent as { company_results?: Json | null } | undefined)?.company_results,
    );
    const runtimeCompanyResults = runtimeProgress?.companyResults ?? [];
    const runtimeCompanyResult = asObject(
      runtimeCompanyResults.find((row) => String(row.company_id ?? "").trim() === item.companyId)
      ?? null,
    );
    const companyResult = asObject(
      eventCompanyResults.find((row) => String(row.company_id ?? "").trim() === item.companyId)
      ?? eventCompanyResults[0]
      ?? null,
    );
    const mergedSummary = {
      ...requestJobPayload,
      ...requestSummary,
      ...eventSummary,
      ...runtimeCompanyResult,
      ...companyResult,
    };

    const runtimeCompanyStatus = String(runtimeCompanyResult.status ?? "").trim();
    if (runtimeCompanyStatus && request.status !== "completed" && request.status !== "failed") {
      const status = mapCompanyResultStatus(runtimeCompanyStatus);
      return {
        ...item,
        status,
        message: resolveCompanyMessage(
          mergedSummary,
          status === "erro"
            ? "Falha ao processar a rotina."
            : "PDF gerado e disponível para download.",
        ),
        artifact: extractArtifact(mergedSummary),
        meta: mergedSummary,
      };
    }

    if (request.status === "completed") {
      return {
        ...item,
        status: mapCompanyResultStatus(String(companyResult.status ?? request.status ?? "")),
        message: resolveCompanyMessage(
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

    const createdAt = String((request as { created_at?: string | null }).created_at ?? "").trim();
    const pendingForMs = createdAt ? Date.now() - Date.parse(createdAt) : 0;
    if (request.status === "pending" && Number.isFinite(pendingForMs) && pendingForMs >= 120_000) {
      return {
        ...item,
        status: "erro",
        message: "A solicitacao nao foi assumida pelo robo. Verifique se o bot do escritorio esta ativo.",
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

  nextItems = await promoteSequentialRuntimeArtifacts({
    action: current.action,
    items: nextItems,
    requestIds: current.requestIds,
  });

  if (nextItems.some((item) => item.status === "sucesso" && !item.artifact)) {
    const artifactMap = await resolveArtifactsFromStorage({
      action: current.action,
      items: nextItems,
    }).catch(() => new Map<string, DeclarationRunItem["artifact"]>());
    if (artifactMap.size > 0) {
      nextItems = nextItems.map((item) => ({
        ...item,
        artifact: item.artifact ?? artifactMap.get(item.companyId) ?? null,
      }));
    }
  }

  nextItems = await hydrateGuideMetadataForItems(current.action, nextItems);

  nextItems = stabilizeRunItems(current.items, nextItems);

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

export async function persistDeclarationRunState(run: DeclarationRunState): Promise<void> {
  const counters = getRunCounters(run.items);
  const companyIds = run.items.map((item) => item.companyId).filter(Boolean);
  const payload: Json = {
    runId: run.runId,
    action: run.action,
    mode: run.mode,
    title: run.title,
    requestIds: run.requestIds,
    items: run.items as unknown as Json,
    startedAt: run.startedAt,
    finishedAt: run.finishedAt,
    terminal: run.terminal,
  };

  const { error } = await supabase.from("declaration_run_history").upsert(
    {
      run_id: run.runId,
      action: run.action,
      mode: run.mode,
      title: run.title,
      status: getRunHistoryStatus(run),
      company_ids: companyIds,
      request_ids: run.requestIds,
      items_total: counters.itemsTotal,
      items_success: counters.itemsSuccess,
      items_error: counters.itemsError,
      items_processing: counters.itemsProcessing,
      payload,
      started_at: run.startedAt,
      finished_at: run.finishedAt,
      last_event_at: run.finishedAt ?? new Date().toISOString(),
    },
    { onConflict: "office_id,run_id" },
  );

  if (error) throw error;
}

export async function listDeclarationRunHistory(params: {
  page: number;
  pageSize: number;
  actions?: DeclarationActionKind[];
}): Promise<DeclarationRunHistoryPage> {
  const page = Math.max(1, Number(params.page) || 1);
  const pageSize = Math.min(50, Math.max(1, Number(params.pageSize) || 10));
  const startIndex = (page - 1) * pageSize;
  const endIndex = startIndex + pageSize;
  const rows = await listAllDeclarationRunHistoryRows({ actions: params.actions });
  const allRuns = rows.map((row) => mapHistoryRowToRun(row));
  const allEntries = buildDeclarationRunHistoryEntries(allRuns);
  const pageDescriptors = allEntries.slice(startIndex, endIndex);
  const visibleRunIds = new Set(pageDescriptors.map((entry) => entry.runId));
  const visibleRuns = await Promise.all(
    allRuns
      .filter((run) => visibleRunIds.has(run.runId))
      .map((run) => hydrateDeclarationRunArtifacts(run)),
  );
  const visibleEntriesById = new Map(
    buildDeclarationRunHistoryEntries(visibleRuns).map((entry) => [entry.entryId, entry] as const),
  );

  return {
    runs: visibleRuns,
    entries: pageDescriptors
      .map((entry) => visibleEntriesById.get(entry.entryId) ?? entry),
    totalEntries: allEntries.length,
  };
}

export async function deleteDeclarationRunHistory(runId: string): Promise<void> {
  const normalizedRunId = String(runId ?? "").trim();
  if (!normalizedRunId) return;
  const { error } = await supabase.from("declaration_run_history").delete().eq("run_id", normalizedRunId);
  if (error) throw error;
}

export async function deleteAllDeclarationRunHistory(params?: {
  actions?: DeclarationActionKind[];
}): Promise<void> {
  let query = supabase
    .from("declaration_run_history")
    .delete()
    .not("run_id", "is", null);

  const actions = Array.from(new Set((params?.actions ?? []).map((value) => String(value).trim()).filter(Boolean)));
  if (actions.length > 0) {
    query = query.in("action", actions);
  }

  const { error } = await query;
  if (error) throw error;
}

export async function stopDeclarationRobotRuntime(params: {
  robotTechnicalIds: string[];
  reason?: string | null;
}): Promise<void> {
  const robotTechnicalIds = Array.from(
    new Set(
      (params.robotTechnicalIds ?? [])
        .map((value) => String(value ?? "").trim())
        .filter(Boolean),
    ),
  );
  if (robotTechnicalIds.length === 0) return;

  await postOfficeServerJson("stop-robot-runtime", {
    robot_technical_ids: robotTechnicalIds,
    reason: String(params.reason ?? "").trim() || null,
  });
}

export async function listDeclarationArtifacts(params: {
  action: DeclarationActionKind;
  companyIds: string[];
  competence?: string | null;
  limit?: number;
}): Promise<DeclarationArtifactListResponse> {
  const response = await postOfficeServerJson<DeclarationArtifactListResponse>(
    "list-declaration-artifacts",
    {
      action: params.action,
      company_ids: params.companyIds,
      competence: params.competence,
      limit: params.limit ?? 200,
    },
  );
  return filterDeclarationArtifactsByAction(params.action, response);
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

export async function fetchDeclarationArtifactBlob(params: {
  action: DeclarationActionKind;
  companyId: string;
  competence?: string | null;
  artifactKey: string;
  suggestedName?: string;
}): Promise<{ blob: Blob; filename: string }> {
  return fetchOfficeServerActionBlob(
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

export async function openDeclarationArtifact(params: {
  action: DeclarationActionKind;
  companyId: string;
  competence?: string | null;
  artifactKey: string;
  suggestedName?: string;
}): Promise<void> {
  await openOfficeServerAction(
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

type RawDeclarationGuideDocumentListItem = {
  document_id?: string | null;
  company_id?: string | null;
  company_name?: string | null;
  company_document?: string | null;
  competencia?: string | null;
  data_vencimento?: string | null;
  amount_cents?: number | null;
  file_name?: string | null;
  file_path?: string | null;
  checksum?: string | null;
  parsed_at?: string | null;
  parser_version?: string | null;
  status?: string | null;
  meta?: Json;
  updated_at?: string | null;
};

type RawDeclarationGuideDocumentListResponse = {
  items?: RawDeclarationGuideDocumentListItem[];
  total?: number;
  page?: number;
  page_size?: number;
};

function mapGuideDocumentListItem(
  row: RawDeclarationGuideDocumentListItem,
): DeclarationGuideDocumentListItem {
  return {
    documentId: String(row.document_id ?? "").trim(),
    companyId: String(row.company_id ?? "").trim(),
    companyName: String(row.company_name ?? "").trim(),
    companyDocument: row.company_document == null ? null : String(row.company_document ?? "").trim(),
    competence: row.competencia == null ? null : String(row.competencia ?? "").trim(),
    dueDate: row.data_vencimento == null ? null : String(row.data_vencimento ?? "").trim(),
    amountCents: Number.isFinite(Number(row.amount_cents)) ? Number(row.amount_cents) : null,
    fileName: String(row.file_name ?? "").trim(),
    filePath: String(row.file_path ?? "").trim(),
    checksum: row.checksum == null ? null : String(row.checksum ?? "").trim(),
    parsedAt: row.parsed_at == null ? null : String(row.parsed_at ?? "").trim(),
    parserVersion: row.parser_version == null ? null : String(row.parser_version ?? "").trim(),
    status: String(row.status ?? "").trim() || "novo",
    meta: (row.meta as Json | undefined) ?? null,
    updatedAt: row.updated_at == null ? null : String(row.updated_at ?? "").trim(),
  };
}

export async function listSimplesGuideDocuments(params: {
  companyIds: string[];
  year?: string | null;
  page?: number;
  pageSize?: number;
  sortKey?: DeclarationGuideDocumentSortKey;
  sortDirection?: "asc" | "desc";
  autoScan?: boolean;
}): Promise<DeclarationGuideDocumentListResponse> {
  const response = await postOfficeServerJson<RawDeclarationGuideDocumentListResponse>(
    "list-simples-guide-documents",
    {
      company_ids: uniqueCompanyIds(params.companyIds ?? []),
      year: /^\d{4}$/.test(String(params.year ?? "").trim()) ? String(params.year).trim() : null,
      page: Math.max(1, Number(params.page) || 1),
      page_size: Math.min(100, Math.max(1, Number(params.pageSize) || 12)),
      sort_key: params.sortKey ?? "competencia",
      sort_direction: params.sortDirection ?? "desc",
      auto_scan: params.autoScan ?? true,
    },
  );
  return {
    items: (response.items ?? []).map(mapGuideDocumentListItem),
    total: Number.isFinite(Number(response.total)) ? Number(response.total) : 0,
    page: Math.max(1, Number(response.page) || 1),
    pageSize: Math.min(100, Math.max(1, Number(response.page_size) || 12)),
  };
}

export async function downloadSimplesGuideDocument(params: {
  documentId: string;
  suggestedName?: string;
}): Promise<void> {
  await downloadOfficeServerAction(
    "download-simples-guide-document",
    { document_id: params.documentId },
    params.suggestedName,
  );
}

export async function scanSimplesGuideDocuments(params: {
  companyIds?: string[];
  year?: string | null;
  force?: boolean;
}): Promise<{
  scanned: number;
  parsed: number;
  inserted: number;
  updated: number;
  skipped: number;
  errors: Array<{ file: string; error: string }>;
}> {
  return postOfficeServerJson<{
    scanned: number;
    parsed: number;
    inserted: number;
    updated: number;
    skipped: number;
    errors: Array<{ file: string; error: string }>;
  }>("scan-simples-guide-documents", {
    company_ids: uniqueCompanyIds(params.companyIds ?? []),
    year: /^\d{4}$/.test(String(params.year ?? "").trim()) ? String(params.year).trim() : null,
    force: Boolean(params.force),
  });
}

export async function scanSingleSimplesGuideDocument(params: {
  documentId: string;
  force?: boolean;
}): Promise<{ ok: boolean; force: boolean; file_path: string }> {
  return postOfficeServerJson<{ ok: boolean; force: boolean; file_path: string }>("scan-single-simples-guide-document", {
    document_id: String(params.documentId ?? "").trim(),
    force: params.force ?? true,
  });
}

function sanitizeZipSegment(value: string): string {
  return (
    String(value ?? "")
      .replace(/[<>:"/\\|?*\u0000-\u001F]+/g, " ")
      .replace(/\s+/g, " ")
      .trim() || "Empresa"
  );
}

function triggerZipDownload(blob: Blob, filename: string) {
  const anchor = document.createElement("a");
  const blobUrl = URL.createObjectURL(blob);
  anchor.href = blobUrl;
  anchor.download = String(filename ?? "").trim() || "guias-simples-nacional.zip";
  anchor.style.display = "none";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  window.setTimeout(() => URL.revokeObjectURL(blobUrl), 30_000);
}

export async function downloadDeclarationRunHistoryZip(params: {
  runs: DeclarationRunState[];
  suggestedName?: string;
}): Promise<number> {
  const hydratedRuns = await Promise.all(params.runs.map((run) => hydrateDeclarationRunArtifacts(run)));
  const zip = new JSZip();
  const usedZipPaths = new Set<string>();
  let addedFiles = 0;

  const makeUniqueZipPath = (zipPath: string) => {
    let candidate = zipPath;
    let index = 0;
    while (usedZipPaths.has(candidate)) {
      index += 1;
      const dotIndex = zipPath.lastIndexOf(".");
      const base = dotIndex >= 0 ? zipPath.slice(0, dotIndex) : zipPath;
      const ext = dotIndex >= 0 ? zipPath.slice(dotIndex) : "";
      candidate = `${base} (${index})${ext}`;
    }
    usedZipPaths.add(candidate);
    return candidate;
  };

  for (const run of hydratedRuns) {
    for (const item of run.items) {
      if (item.status !== "sucesso" || !item.artifact) continue;

      const companyFolder = sanitizeZipSegment(item.companyName);

      if (item.artifact.filePath) {
        const { blob, filename } = await fetchServerFileByPath(item.artifact.filePath);
        zip.file(makeUniqueZipPath(`${companyFolder}/${filename}`), blob);
        addedFiles += 1;
        continue;
      }

      if (item.artifact.artifactKey) {
        const meta = asObject(item.meta ?? null);
        const rawReference = String(meta.competencia ?? meta.competence ?? "").trim();
        const { blob, filename } = await fetchDeclarationArtifactBlob({
          action: run.action,
          companyId: item.companyId,
          competence: isValidCompetence(rawReference) ? rawReference : null,
          artifactKey: item.artifact.artifactKey,
          suggestedName: item.artifact.label,
        });
        zip.file(makeUniqueZipPath(`${companyFolder}/${filename}`), blob);
        addedFiles += 1;
      }
    }
  }

  if (addedFiles === 0) {
    throw new Error("Nenhum PDF disponivel para gerar o ZIP.");
  }

  const zipBlob = await zip.generateAsync({ type: "blob", compression: "STORE" });
  triggerZipDownload(
    zipBlob,
    `${String(params.suggestedName ?? "guias-simples-nacional").trim() || "guias-simples-nacional"}.zip`,
  );
  return addedFiles;
}


