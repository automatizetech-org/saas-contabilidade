/**
 * API unificada — Fleury Insights Hub
 * Roda na VM na porta 3001. Atende rotas de arquivos e repassa o restante ao backend WhatsApp.
 * BASE_PATH: lido do Supabase (admin_settings.base_path) na inicialização; fallback para .env BASE_PATH.
 * Configure WHATSAPP_BACKEND_URL, SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY no .env
 */

import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import { createHash, randomUUID, timingSafeEqual } from "crypto";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// Único .env: pasta Servidor (um nível acima). Path absoluto para PM2/Windows.
dotenv.config({ path: path.resolve(__dirname, "..", ".env") });

import express from "express";
import cors from "cors";
import helmet from "helmet";
import rateLimit from "express-rate-limit";
import fs from "fs";
import os from "os";
import archiver from "archiver";
import { createProxyMiddleware } from "http-proxy-middleware";
import { createClient } from "@supabase/supabase-js";


const robotEligibility = (() => {
function onlyDigits(value) {
  return String(value ?? "").replace(/\D/g, "");
}

function coerceJsonObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function coerceJsonArray(value) {
  return Array.isArray(value) ? value : [];
}

function normalizeCityName(value) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .trim()
    .toLowerCase();
}

function normalizeLoginRows(value) {
  const seen = new Set();
  return coerceJsonArray(value)
    .map((item) => {
      if (!item || typeof item !== "object" || Array.isArray(item)) return null;
      const cpf = onlyDigits(item.cpf);
      const password = String(item.password ?? "").trim();
      if (!cpf || !password) return null;
      return {
        cpf,
        password,
        is_default: Boolean(item.is_default),
      };
    })
    .filter(Boolean)
    .filter((item) => {
      if (seen.has(item.cpf)) return false;
      seen.add(item.cpf);
      return true;
    });
}

function getCompanySettings(configRow) {
  const settings = coerceJsonObject(configRow?.settings);
  const authMode = configRow?.auth_mode || settings.auth_mode || "password";
  return {
    ...settings,
    auth_mode: authMode,
    nfs_password: authMode === "password" ? (configRow?.nfs_password ?? settings.nfs_password ?? null) : null,
    selected_login_cpf: configRow?.selected_login_cpf ?? settings.selected_login_cpf ?? null,
  };
}

function hasActiveCertificate(company) {
  if (!String(company?.cert_blob_b64 ?? "").trim()) return false;
  if (!String(company?.cert_password ?? "").trim()) return false;
  const validUntil = String(company?.cert_valid_until ?? "").trim();
  if (!validUntil) return true;
  const expiresAt = new Date(validUntil);
  if (Number.isNaN(expiresAt.getTime())) return true;
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return expiresAt.getTime() >= today.getTime();
}

function getCapabilityBoolean(robotRow, key) {
  const capabilities = coerceJsonObject(robotRow?.capabilities);
  return typeof capabilities[key] === "boolean" ? capabilities[key] : null;
}

function getCapabilityStringList(robotRow, key) {
  const capabilities = coerceJsonObject(robotRow?.capabilities);
  return coerceJsonArray(capabilities[key])
    .map((item) => String(item ?? "").trim())
    .filter(Boolean);
}

function getEligibilityPolicy(robotRow) {
  const capabilities = coerceJsonObject(robotRow?.capabilities);
  const authBehaviorRaw = String(capabilities.auth_behavior ?? "").trim();
  const hasExplicitCapabilities = Object.keys(capabilities).length > 0;
  const fallbackAuthBehavior =
    robotRow?.technical_id === "nfs_padrao"
      ? "choice"
      : robotRow?.technical_id === "sefaz_xml"
        ? "login_only"
        : "cnpj_only";

  const policy = {
    requireEnabledConfig: getCapabilityBoolean(robotRow, "require_enabled_config") ?? true,
    requireDocument: getCapabilityBoolean(robotRow, "require_document") ?? false,
    requireStateRegistration: getCapabilityBoolean(robotRow, "require_state_registration") ?? false,
    requireCae: getCapabilityBoolean(robotRow, "require_cae") ?? false,
    requireAnyLoginSource: getCapabilityBoolean(robotRow, "require_any_login_source") ?? false,
    loginRouting:
      String(capabilities.login_routing ?? "").trim().toLowerCase() === "any_available"
        ? "any_available"
        : "match_selected_or_accountant",
    authBehavior:
      authBehaviorRaw === "choice" || authBehaviorRaw === "login_only" || authBehaviorRaw === "cnpj_only"
        ? authBehaviorRaw
        : fallbackAuthBehavior,
  };

  for (const field of getCapabilityStringList(robotRow, "required_company_fields")) {
    if (field === "document") policy.requireDocument = true;
    if (field === "state_registration") policy.requireStateRegistration = true;
    if (field === "cae") policy.requireCae = true;
  }

  if (!hasExplicitCapabilities) {
    if (robotRow?.technical_id === "nfs_padrao") {
      policy.requireEnabledConfig = true;
      policy.authBehavior = "choice";
    } else if (robotRow?.technical_id === "sefaz_xml") {
      policy.requireEnabledConfig = false;
      policy.requireStateRegistration = true;
      policy.requireAnyLoginSource = true;
      policy.authBehavior = "login_only";
    } else if (robotRow?.technical_id === "goiania_taxas_impostos") {
      policy.requireEnabledConfig = false;
      policy.requireCae = true;
      policy.requireAnyLoginSource = true;
      policy.loginRouting = "any_available";
    } else if (robotRow?.technical_id === "certidoes" || robotRow?.technical_id === "certidoes_fiscal") {
      policy.requireEnabledConfig = false;
      policy.requireDocument = true;
    }
  }

  return policy;
}

function hasEligiblePortalLogin(robotRow, company, configRow, routing = "match_selected_or_accountant") {
  const settings = getCompanySettings(configRow);
  const selectedLoginCpf = onlyDigits(settings.selected_login_cpf);
  const contadorCpf = onlyDigits(company?.contador_cpf);
  const availableLogins = [
    ...normalizeLoginRows(robotRow?.global_logins),
    ...normalizeLoginRows(company?.sefaz_go_logins),
  ];

  if (availableLogins.length === 0) return false;
  if (routing === "any_available") return true;
  const hasCpf = (cpf) => availableLogins.some((item) => item.cpf === cpf);
  if (selectedLoginCpf) return hasCpf(selectedLoginCpf);
  if (contadorCpf) return hasCpf(contadorCpf);
  return true;
}

function filterEligibleCompaniesForRobot({
  robotRow,
  companies,
  configByCompanyId,
  cityName = null,
}) {
  const policy = getEligibilityPolicy(robotRow);
  const normalizedCity = normalizeCityName(cityName);

  return (companies ?? []).filter((company) => {
    if (!company?.active) return false;
    const config = configByCompanyId?.get(company.company_id || company.id) ?? null;
    const settings = getCompanySettings(config);

    if (policy.requireEnabledConfig && !config?.enabled) return false;
    if (normalizedCity && normalizeCityName(company.city_name) !== normalizedCity) return false;
    if (policy.requireDocument && !onlyDigits(company.document).trim()) return false;
    if (policy.requireStateRegistration && !onlyDigits(company.state_registration).trim()) return false;
    if (policy.requireCae && !String(company.cae ?? "").trim()) return false;
    if (policy.requireAnyLoginSource && !hasEligiblePortalLogin(robotRow, company, config, policy.loginRouting)) return false;

    if (policy.authBehavior === "choice") {
      const authMode = String(settings.auth_mode ?? config?.auth_mode ?? company.auth_mode ?? "password").trim().toLowerCase();
      if (authMode === "certificate") return hasActiveCertificate(company);
      return Boolean(String(settings.nfs_password ?? config?.nfs_password ?? "").trim());
    }

    return true;
  });
}

function getRequestedCityNameFromJob(jobPayload, robotRow) {
  const settings = coerceJsonObject(jobPayload);
  return String(
    settings.city_name ??
    coerceJsonObject(settings.execution_defaults).city_name ??
    coerceJsonObject(robotRow?.execution_defaults).city_name ??
    "",
  ).trim();
}

  return {
    normalizeCityName,
    filterEligibleCompaniesForRobot,
    getRequestedCityNameFromJob,
  };
})();

const robotJsonRuntimeModule = (() => {
  const { filterEligibleCompaniesForRobot, getRequestedCityNameFromJob } = robotEligibility;
function normalizedResolvedPath(inputPath) {
  const resolved = path.resolve(String(inputPath || ""));
  return process.platform === "win32" ? resolved.toLowerCase() : resolved;
}

function ensureDirectory(dirPath) {
  fs.mkdirSync(dirPath, { recursive: true });
}

function resolveWithin(rootPath, relativePath) {
  const base = path.resolve(rootPath);
  const target = path.resolve(base, String(relativePath || ""));
  const normalizedBase = normalizedResolvedPath(base);
  const normalizedTarget = normalizedResolvedPath(target);
  if (!normalizedTarget.startsWith(normalizedBase)) {
    throw new Error(`Path fora do diretório permitido: ${relativePath}`);
  }
  return target;
}

function normalizeRuntimeRelPath(value, fallback) {
  const trimmed = String(value || "").trim();
  if (!trimmed) return fallback;
  return trimmed.replace(/\//g, path.sep);
}

function safeReadJson(filePath) {
  if (!filePath || !fs.existsSync(filePath)) return null;
  const raw = fs.readFileSync(filePath, "utf8").replace(/^\uFEFF/, "");
  if (!raw.trim()) return null;
  return JSON.parse(raw);
}

function writeJsonAtomic(filePath, payload) {
  ensureDirectory(path.dirname(filePath));
  const tempPath = `${filePath}.tmp`;
  fs.writeFileSync(tempPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  fs.renameSync(tempPath, filePath);
}

function removeFileIfExists(filePath) {
  if (filePath && fs.existsSync(filePath)) {
    fs.unlinkSync(filePath);
  }
}

function writeProcessedJson(filePath, payload, extra = {}) {
  if (!filePath) return;
  writeJsonAtomic(filePath, {
    ...coerceJsonObject(payload),
    ...coerceJsonObject(extra),
  });
}

function executionIdFromPayload(payload) {
  if (!payload || typeof payload !== "object") return null;
  return String(payload.execution_request_id || payload.job_id || payload.id || "").trim() || null;
}

function isResultForJob(resultPayload, jobPayload) {
  const resultId = executionIdFromPayload(resultPayload);
  const jobId = executionIdFromPayload(jobPayload);
  return Boolean(resultId && jobId && resultId === jobId);
}

function coerceJsonObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function sanitizeCompanySettings(configRow) {
  const settings = coerceJsonObject(configRow?.settings);
  const authMode = configRow?.auth_mode || settings.auth_mode || "password";
  const nfsPassword =
    authMode === "password"
      ? (configRow?.nfs_password ?? settings.nfs_password ?? null)
      : null;
  const selectedLoginCpf =
    String(configRow?.selected_login_cpf ?? settings.selected_login_cpf ?? "").trim() || null;

  return {
    ...settings,
    auth_mode: authMode,
    nfs_password: nfsPassword,
    selected_login_cpf: selectedLoginCpf,
  };
}

function buildRuntimePaths(robotsRootPath, robotRow) {
  const technicalId = String(robotRow.technical_id || "").trim();
  const configuredRuntimeFolder = String(robotRow.runtime_folder || "").trim();
  const folderCandidates = [technicalId, configuredRuntimeFolder].filter(Boolean);
  if (!folderCandidates.length) return null;

  let runtimeRoot = null;
  for (const folder of folderCandidates) {
    const candidateRoot = resolveWithin(robotsRootPath, folder);
    if (fs.existsSync(candidateRoot)) {
      runtimeRoot = candidateRoot;
      break;
    }
  }
  if (!runtimeRoot) {
    runtimeRoot = resolveWithin(robotsRootPath, folderCandidates[0]);
  }
  return {
    runtimeRoot,
    entrypointPath: resolveWithin(runtimeRoot, normalizeRuntimeRelPath(robotRow.entrypoint_relpath, "bot.py")),
    jobFilePath: resolveWithin(runtimeRoot, normalizeRuntimeRelPath(robotRow.job_file_relpath, path.join("data", "json", "job.json"))),
    resultFilePath: resolveWithin(runtimeRoot, normalizeRuntimeRelPath(robotRow.result_file_relpath, path.join("data", "json", "result.json"))),
    heartbeatFilePath: resolveWithin(runtimeRoot, normalizeRuntimeRelPath(robotRow.heartbeat_file_relpath, path.join("data", "json", "heartbeat.json"))),
    stopFilePath: resolveWithin(runtimeRoot, path.join("data", "json", "stop.json")),
  };
}

/** Lista robôs mesclados com office_robot_configs + office_robot_runtime (usado pelo dispatcher e pelo agendador). */
async function listRobotRuntimeRows(supabase, officeId, officeServerId) {
  const [{ data: robots, error: robotsError }, { data: configs, error: configsError }, { data: runtimeRows, error: runtimeError }] =
    await Promise.all([
      supabase
        .from("robots")
        .select([
          "id",
          "technical_id",
          "display_name",
          "segment_path",
          "notes_mode",
          "date_execution_mode",
          "initial_period_start",
          "initial_period_end",
          "last_period_end",
          "is_fiscal_notes_robot",
          "fiscal_notes_kind",
          "global_logins",
          "runtime_folder",
          "entrypoint_relpath",
          "job_file_relpath",
          "result_file_relpath",
          "heartbeat_file_relpath",
          "capabilities",
          "runtime_defaults",
        ].join(",")),
      supabase
        .from("office_robot_configs")
        .select("*")
        .eq("office_id", officeId),
      supabase
        .from("office_robot_runtime")
        .select("*")
        .eq("office_server_id", officeServerId),
    ]);

  if (robotsError) throw robotsError;
  if (configsError) throw configsError;
  if (runtimeError) throw runtimeError;

  const configByTechnicalId = new Map(
    (configs ?? []).map((row) => [row.robot_technical_id, row]),
  );
  const runtimeByTechnicalId = new Map(
    (runtimeRows ?? []).map((row) => [row.robot_technical_id, row]),
  );

  return (robots ?? []).map((robot) => {
    const config = configByTechnicalId.get(robot.technical_id) ?? {};
    const runtime = runtimeByTechnicalId.get(robot.technical_id) ?? {};
    return {
      ...robot,
      display_name: config.display_name || robot.display_name,
      segment_path: config.segment_path ?? robot.segment_path ?? null,
      notes_mode: config.notes_mode ?? robot.notes_mode ?? null,
      date_execution_mode: config.date_execution_mode ?? robot.date_execution_mode ?? null,
      initial_period_start: config.initial_period_start ?? robot.initial_period_start ?? null,
      initial_period_end: config.initial_period_end ?? robot.initial_period_end ?? null,
      last_period_end: config.last_period_end ?? robot.last_period_end ?? null,
      global_logins: config.global_logins ?? robot.global_logins ?? [],
      admin_settings: config.admin_settings ?? {},
      execution_defaults: config.execution_defaults ?? {},
      runtime_status: runtime.status ?? robot.status ?? "inactive",
      runtime_last_heartbeat_at: runtime.last_heartbeat_at ?? robot.last_heartbeat_at ?? null,
    };
  });
}

async function claimNextExecutionRequest(supabase, { officeId, officeServerId, robotRow }) {
  const { data: runningRows, error: runningError } = await supabase
    .from("execution_requests")
    .select("id,robot_technical_ids")
    .eq("office_id", officeId)
    .eq("status", "running")
    .order("started_at", { ascending: true, nullsFirst: true })
    .order("created_at", { ascending: true })
    .limit(1);
  if (runningError) throw runningError;
  if ((runningRows ?? []).length > 0) return null;

  const { data: nextRow, error } = await supabase
    .from("execution_requests")
    .select("*")
    .eq("office_id", officeId)
    .eq("status", "pending")
    .order("created_at", { ascending: true })
    .order("execution_order", { ascending: true, nullsFirst: true })
    .limit(1)
    .maybeSingle();
  if (error) throw error;
  if (!nextRow) return null;
  if (!Array.isArray(nextRow.robot_technical_ids) || !nextRow.robot_technical_ids.includes(robotRow.technical_id)) {
    return null;
  }

  const nowIso = new Date().toISOString();
  const { data: claimed, error: claimError } = await supabase
    .from("execution_requests")
    .update({
      status: "running",
      robot_id: robotRow.id,
      claimed_at: nowIso,
      started_at: nowIso,
      claimed_by_server_id: officeServerId,
    })
    .eq("id", nextRow.id)
    .eq("status", "pending")
    .select("*")
    .maybeSingle();

  if (!claimError && claimed?.id) return claimed;

  return null;
}

async function loadCompaniesForExecution(supabase, executionRequest, robotRow) {
  const requestSettings = coerceJsonObject(executionRequest.job_payload);
  const explicitCompanyIdsFromJob = Array.isArray(requestSettings.company_ids)
    ? requestSettings.company_ids.map((value) => String(value || "").trim()).filter(Boolean)
    : [];
  const companyIds = (
    explicitCompanyIdsFromJob.length > 0
      ? explicitCompanyIdsFromJob
      : (Array.isArray(executionRequest.company_ids) ? executionRequest.company_ids : [])
  ).map((value) => String(value || "").trim()).filter(Boolean);
  if (companyIds.length === 0) return [];
  const requestedCityName = getRequestedCityNameFromJob(requestSettings, robotRow);

  const [{ data: companies, error: companiesError }, { data: configRows, error: configError }] =
    await Promise.all([
      supabase
        .from("companies")
        .select([
          "id",
          "office_id",
          "name",
          "document",
          "active",
          "auth_mode",
          "cert_blob_b64",
          "cert_password",
          "cert_valid_until",
          "contador_nome",
          "contador_cpf",
          "state_registration",
          "state_code",
          "city_name",
          "cae",
          "sefaz_go_logins",
        ].join(","))
        .in("id", companyIds),
      supabase
        .from("company_robot_config")
        .select("*")
        .eq("robot_technical_id", robotRow.technical_id)
        .in("company_id", companyIds),
    ]);

  if (companiesError) throw companiesError;
  if (configError) throw configError;

  const orderMap = new Map(companyIds.map((companyId, index) => [companyId, index]));
  const configByCompanyId = new Map((configRows ?? []).map((row) => [row.company_id, row]));

  const rows = (companies ?? [])
    .map((company) => {
      const config = configByCompanyId.get(company.id) ?? null;
      const settings = sanitizeCompanySettings(config);
      return {
        company_id: company.id,
        id: company.id,
        office_id: company.office_id,
        name: String(company.name || "").trim(),
        document: company.document,
        doc: company.document,
        cnpj: company.document,
        active: Boolean(company.active),
        auth_mode: settings.auth_mode || company.auth_mode || "password",
        cert_blob_b64: company.cert_blob_b64,
        cert_password: company.cert_password,
        cert_valid_until: company.cert_valid_until,
        contador_nome: company.contador_nome,
        contador_cpf: company.contador_cpf,
        state_registration: company.state_registration,
        state_code: company.state_code,
        city_name: company.city_name,
        cae: company.cae,
        sefaz_go_logins: company.sefaz_go_logins ?? [],
        enabled_for_robot: Boolean(config?.enabled),
        selected_login_cpf: settings.selected_login_cpf,
        password: settings.nfs_password,
        nfs_password: settings.nfs_password,
        robot_config: config,
        settings,
      };
    })
  const orderedRows = rows
    .sort((left, right) => (orderMap.get(left.company_id) ?? 0) - (orderMap.get(right.company_id) ?? 0));

  if (explicitCompanyIdsFromJob.length > 0) {
    return orderedRows;
  }

  return filterEligibleCompaniesForRobot({
    robotRow,
    companies: orderedRows,
    configByCompanyId,
    cityName: requestedCityName,
  });
}

function buildJobPayload({ officeId, officeServerId, basePath, robotsRootPath, robotRow, executionRequest, companies }) {
  return {
    id: executionRequest.id,
    job_id: executionRequest.id,
    execution_request_id: executionRequest.id,
    robot_technical_id: robotRow.technical_id,
    robot_display_name: robotRow.display_name,
    office_id: officeId,
    office_server_id: officeServerId,
    source: executionRequest.source || (executionRequest.schedule_rule_id ? "scheduler" : "manual"),
    schedule_rule_id: executionRequest.schedule_rule_id ?? null,
    period_start: executionRequest.period_start ?? null,
    period_end: executionRequest.period_end ?? null,
    notes_mode: executionRequest.notes_mode ?? robotRow.notes_mode ?? null,
    requested_at: executionRequest.created_at,
    created_at: executionRequest.created_at,
    company_ids: companies.map((company) => company.company_id || company.id),
    companies_total: companies.length,
    settings: coerceJsonObject(executionRequest.job_payload),
    connector: {
      office_id: officeId,
      office_server_id: officeServerId,
      base_path: basePath,
      robots_root_path: robotsRootPath,
    },
    robot: {
      id: robotRow.id,
      technical_id: robotRow.technical_id,
      display_name: robotRow.display_name,
      segment_path: robotRow.segment_path ?? null,
      notes_mode: robotRow.notes_mode ?? null,
      date_execution_mode: robotRow.date_execution_mode ?? null,
      initial_period_start: robotRow.initial_period_start ?? null,
      initial_period_end: robotRow.initial_period_end ?? null,
      last_period_end: robotRow.last_period_end ?? null,
      is_fiscal_notes_robot: Boolean(robotRow.is_fiscal_notes_robot),
      fiscal_notes_kind: robotRow.fiscal_notes_kind ?? null,
      global_logins: robotRow.global_logins ?? [],
      runtime_folder: robotRow.runtime_folder ?? robotRow.technical_id,
      entrypoint_relpath: robotRow.entrypoint_relpath,
      job_file_relpath: robotRow.job_file_relpath,
      result_file_relpath: robotRow.result_file_relpath,
      heartbeat_file_relpath: robotRow.heartbeat_file_relpath,
      capabilities: robotRow.capabilities ?? {},
      runtime_defaults: robotRow.runtime_defaults ?? {},
      admin_settings: robotRow.admin_settings ?? {},
      execution_defaults: robotRow.execution_defaults ?? {},
    },
    companies,
  };
}

async function upsertOfficeRobotRuntime(supabase, payload) {
  const nowIso = new Date().toISOString();
  const row = {
    office_id: payload.office_id,
    office_server_id: payload.office_server_id,
    robot_technical_id: payload.robot_technical_id,
    status: payload.status ?? "inactive",
    last_heartbeat_at: payload.last_heartbeat_at ?? null,
    current_execution_request_id: payload.current_execution_request_id ?? null,
    current_job_id: payload.current_job_id ?? null,
    runtime_version: payload.runtime_version ?? null,
    host_name: payload.host_name ?? null,
    heartbeat_payload: payload.heartbeat_payload ?? {},
    updated_at: nowIso,
  };

  const { error } = await supabase
    .from("office_robot_runtime")
    .upsert(row, { onConflict: "office_server_id,robot_technical_id" });
  if (error) throw error;
}

async function finalizeExecutionRequest(supabase, executionRequestId, success, errorMessage, resultSummary) {
  const nowIso = new Date().toISOString();

  const { data: requestRow, error: requestError } = await supabase
    .from("execution_requests")
    .select("id,office_id,execution_mode,execution_group_id,execution_order")
    .eq("id", executionRequestId)
    .maybeSingle();
  if (requestError) throw requestError;

  const { error: updateError } = await supabase
    .from("execution_requests")
    .update({
      status: success ? "completed" : "failed",
      completed_at: nowIso,
      error_message: errorMessage ?? null,
      result_summary: resultSummary ?? {},
    })
    .eq("id", executionRequestId);
  if (updateError) throw updateError;

  if (!success && requestRow?.execution_mode === "sequential" && requestRow.execution_group_id) {
    const { data: pendingRows, error: pendingError } = await supabase
      .from("execution_requests")
      .select("id,execution_order")
      .eq("office_id", requestRow.office_id)
      .eq("execution_group_id", requestRow.execution_group_id)
      .eq("status", "pending")
      .order("execution_order", { ascending: true, nullsFirst: true })
      .order("created_at", { ascending: true });
    if (pendingError) throw pendingError;

    const cancelMessage = errorMessage
      ? `Cancelado porque um robô anterior da fila falhou. ${errorMessage}`
      : "Cancelado porque um robô anterior da fila falhou.";

    for (const pendingRow of pendingRows ?? []) {
      if (
        requestRow.execution_order != null &&
        pendingRow.execution_order != null &&
        pendingRow.execution_order <= requestRow.execution_order
      ) {
        continue;
      }

      await supabase
        .from("execution_requests")
        .update({
          status: "failed",
          completed_at: nowIso,
          error_message: cancelMessage,
          result_summary: {},
        })
        .eq("id", pendingRow.id)
        .eq("status", "pending");
    }
  }
}

async function clearOfflineRobotQueue(
  supabase,
  {
    officeId,
    robotTechnicalId,
    skipExecutionRequestId = null,
    reason = "Robô indisponível",
  },
) {
  let pendingQuery = supabase
    .from("execution_requests")
    .delete()
    .eq("office_id", officeId)
    .eq("status", "pending")
    .contains("robot_technical_ids", [robotTechnicalId]);
  if (skipExecutionRequestId) {
    pendingQuery = pendingQuery.neq("id", skipExecutionRequestId);
  }
  const { error: pendingError } = await pendingQuery;
  if (pendingError) throw pendingError;

  let runningQuery = supabase
    .from("execution_requests")
    .update({
      status: "failed",
      completed_at: new Date().toISOString(),
      error_message: reason,
      result_summary: {
        cancelled: true,
        cancelled_reason: reason,
      },
    })
    .eq("office_id", officeId)
    .eq("status", "running")
    .contains("robot_technical_ids", [robotTechnicalId]);
  if (skipExecutionRequestId) {
    runningQuery = runningQuery.neq("id", skipExecutionRequestId);
  }
  const { error: runningError } = await runningQuery;
  if (runningError) throw runningError;
}

async function pauseOfflineRobotSchedule(
  supabase,
  {
    officeId,
    robotTechnicalId,
  },
) {
  const { error } = await supabase
    .from("schedule_rules")
    .update({
      status: "paused",
      last_run_at: null,
    })
    .eq("office_id", officeId)
    .eq("status", "active")
    .contains("robot_technical_ids", [robotTechnicalId]);
  if (error) throw error;
}

async function applyResultOperations(supabase, officeId, payload) {
  const operations = Array.isArray(payload?.operations) ? payload.operations : [];
  for (const operation of operations) {
    const kind = String(operation?.kind || "").trim();
    if (!kind) continue;

    if (kind === "insert_rows" || kind === "upsert_rows") {
      const table = String(operation.table || "").trim();
      if (!table) continue;
      const rows = Array.isArray(operation.rows) ? operation.rows : [];
      if (rows.length === 0) continue;
      const normalizedRows = rows.map((row) => ({
        office_id: row?.office_id ?? officeId,
        ...coerceJsonObject(row),
      }));
      const query = supabase.from(table);
      const { error } =
        kind === "upsert_rows"
          ? await query.upsert(normalizedRows, {
              onConflict: String(operation.on_conflict || "").trim() || undefined,
            })
          : await query.insert(normalizedRows);
      if (error) throw error;
      continue;
    }

    if (kind === "replace_company_rows") {
      const table = String(operation.table || "").trim();
      const companyId = String(operation.company_id || "").trim();
      if (!table || !companyId) continue;
      const rows = Array.isArray(operation.rows) ? operation.rows : [];
      const deleteQuery = await supabase
        .from(table)
        .delete()
        .eq("office_id", officeId)
        .eq("company_id", companyId);
      if (deleteQuery.error) throw deleteQuery.error;

      if (rows.length > 0) {
        const normalizedRows = rows.map((row) => ({
          office_id: officeId,
          company_id: companyId,
          ...coerceJsonObject(row),
        }));
        const { error } = await supabase.from(table).insert(normalizedRows);
        if (error) throw error;
      }
      continue;
    }

    if (kind === "replace_rows") {
      const table = String(operation.table || "").trim();
      const filters = coerceJsonObject(operation.filters);
      if (!table || Object.keys(filters).length === 0) continue;
      let deleteQuery = supabase.from(table).delete().eq("office_id", officeId);
      for (const [filterKey, filterValue] of Object.entries(filters)) {
        if (Array.isArray(filterValue)) {
          deleteQuery = deleteQuery.in(filterKey, filterValue);
        } else if (filterValue === null) {
          deleteQuery = deleteQuery.is(filterKey, null);
        } else {
          deleteQuery = deleteQuery.eq(filterKey, filterValue);
        }
      }
      const deleteResult = await deleteQuery;
      if (deleteResult.error) throw deleteResult.error;

      const rows = Array.isArray(operation.rows) ? operation.rows : [];
      if (rows.length > 0) {
        const normalizedRows = rows.map((row) => ({
          office_id: officeId,
          ...coerceJsonObject(row),
        }));
        const { error } = await supabase.from(table).insert(normalizedRows);
        if (error) throw error;
      }
      continue;
    }

    if (kind === "rpc") {
      const fn = String(operation.fn || "").trim();
      if (!fn) continue;
      const args = coerceJsonObject(operation.args);
      const { error } = await supabase.rpc(fn, args);
      if (error) throw error;
    }
  }
}

function resultEventId(resultPayload) {
  const rawEventId = String(resultPayload?.event_id || resultPayload?.execution_request_id || "").trim();
  if (rawEventId) return rawEventId;
  return null;
}

async function ingestResultFile(supabase, officeId, robotRow, runtimePaths, logger) {
  const resultPayload = safeReadJson(runtimePaths.resultFilePath);
  if (!resultPayload) return false;
  if (resultPayload._ingested_at) return false;
  if (!executionIdFromPayload(resultPayload) && !resultEventId(resultPayload)) {
    return false;
  }

  const eventId = resultEventId(resultPayload);
  if (!eventId) {
    logger?.warn?.(`[robot-json-runtime] ${robotRow.technical_id}: result.json sem event_id/execution_request_id.`);
    writeProcessedJson(runtimePaths.resultFilePath, resultPayload, {
      _ingested_at: new Date().toISOString(),
      _ingested_status: "invalid",
      _ingest_error: "missing_event_id",
    });
    return true;
  }

  const executionRequestId = String(resultPayload.execution_request_id || "").trim() || null;
  const eventHash = createHash("sha256")
    .update(JSON.stringify(resultPayload))
    .digest("hex");

  const { data: existingEvent, error: existingEventError } = await supabase
    .from("robot_result_events")
    .select("id")
    .eq("event_id", eventId)
    .maybeSingle();
  if (existingEventError) throw existingEventError;
  if (existingEvent?.id) {
    writeProcessedJson(runtimePaths.resultFilePath, resultPayload, {
      _ingested_at: new Date().toISOString(),
      _ingested_status: "duplicate",
    });
    return true;
  }

  await applyResultOperations(supabase, officeId, resultPayload.payload ?? {});

  const status = String(resultPayload.status || "completed").trim().toLowerCase() === "failed" ? "failed" : "completed";
  if (executionRequestId) {
    await finalizeExecutionRequest(
      supabase,
      executionRequestId,
      status === "completed",
      resultPayload.error_message ?? null,
      coerceJsonObject(resultPayload.summary),
    );
  }

  const insertPayload = {
    event_id: eventId,
    office_id: officeId,
    execution_request_id: executionRequestId,
    robot_technical_id: robotRow.technical_id,
    status,
    summary: coerceJsonObject(resultPayload.summary),
    company_results: Array.isArray(resultPayload.company_results) ? resultPayload.company_results : [],
    payload: {
      ...coerceJsonObject(resultPayload.payload),
      _event_hash: eventHash,
    },
    error_message: resultPayload.error_message ?? null,
  };
  const { error: insertError } = await supabase.from("robot_result_events").insert(insertPayload);
  if (insertError) throw insertError;

  await upsertOfficeRobotRuntime(supabase, {
    office_id: officeId,
    office_server_id: runtimePaths.officeServerId,
    robot_technical_id: robotRow.technical_id,
    status: "active",
    last_heartbeat_at: new Date().toISOString(),
    current_execution_request_id: null,
    current_job_id: null,
    heartbeat_payload: {
      source: "result_ingestor",
      status: "active",
      last_result_event_id: eventId,
    },
  });

  writeProcessedJson(runtimePaths.resultFilePath, resultPayload, {
    _ingested_at: new Date().toISOString(),
    _ingested_status: "processed",
  });
  return true;
}

async function readHeartbeatAndSync(supabase, { officeId, officeServerId, robotRow, runtimePaths, logger }) {
  const heartbeat = safeReadJson(runtimePaths.heartbeatFilePath);
  const staleJobPayload = fs.existsSync(runtimePaths.jobFilePath)
    ? safeReadJson(runtimePaths.jobFilePath)
    : null;
  const existingResultPayload = fs.existsSync(runtimePaths.resultFilePath)
    ? safeReadJson(runtimePaths.resultFilePath)
    : null;
  const now = Date.now();
  const heartbeatUpdatedAt = heartbeat?.updated_at ? Date.parse(heartbeat.updated_at) : Number.NaN;
  const isFresh = Number.isFinite(heartbeatUpdatedAt) && now - heartbeatUpdatedAt <= 90_000;
  const heartbeatStatus = String(heartbeat?.status || "active").trim().toLowerCase() || "active";
  const status = isFresh
    ? heartbeatStatus
    : "inactive";

  await upsertOfficeRobotRuntime(supabase, {
    office_id: officeId,
    office_server_id: officeServerId,
    robot_technical_id: robotRow.technical_id,
    status,
    last_heartbeat_at: Number.isFinite(heartbeatUpdatedAt) ? new Date(heartbeatUpdatedAt).toISOString() : null,
    current_execution_request_id: heartbeat?.current_execution_request_id ?? null,
    current_job_id: heartbeat?.current_job_id ?? null,
    runtime_version: heartbeat?.runtime_version ?? null,
    host_name: heartbeat?.host_name ?? null,
    heartbeat_payload: heartbeat ?? {},
  });

  const staleExecutionRequestId =
    String(
      heartbeat?.current_execution_request_id ??
      staleJobPayload?.execution_request_id ??
      staleJobPayload?.job_id ??
      "",
    ).trim() || null;

  const hasUnprocessedJobFile =
    fs.existsSync(runtimePaths.jobFilePath) &&
    (!existingResultPayload || !isResultForJob(existingResultPayload, staleJobPayload));

  if (isFresh && heartbeatStatus === "inactive" && staleExecutionRequestId && hasUnprocessedJobFile) {
    const shutdownResult = {
      event_id: staleExecutionRequestId,
      job_id: executionIdFromPayload(staleJobPayload),
      execution_request_id: staleExecutionRequestId,
      robot_technical_id: robotRow.technical_id,
      status: "failed",
      started_at: staleJobPayload?.created_at ?? new Date().toISOString(),
      finished_at: new Date().toISOString(),
      error_message: "Robot encerrado manualmente antes de concluir o job",
      summary: { aborted_by_shutdown: true },
      company_results: [],
      payload: {},
      _generated_by_connector: true,
    };
    writeProcessedJson(runtimePaths.resultFilePath, shutdownResult);
    removeFileIfExists(runtimePaths.jobFilePath);
    await upsertOfficeRobotRuntime(supabase, {
      office_id: officeId,
      office_server_id: officeServerId,
      robot_technical_id: robotRow.technical_id,
      status: "inactive",
      last_heartbeat_at: Number.isFinite(heartbeatUpdatedAt) ? new Date(heartbeatUpdatedAt).toISOString() : null,
      current_execution_request_id: null,
      current_job_id: null,
      runtime_version: heartbeat?.runtime_version ?? null,
      host_name: heartbeat?.host_name ?? null,
      heartbeat_payload: {
        ...(heartbeat ?? {}),
        status: "inactive",
        message: "job_cleared_after_shutdown",
        cleared_at: new Date().toISOString(),
      },
    });
    await clearOfflineRobotQueue(supabase, {
      officeId,
      robotTechnicalId: robotRow.technical_id,
      skipExecutionRequestId: staleExecutionRequestId,
      reason: "Robô encerrado manualmente antes de concluir o job",
    });
    logger?.warn?.(`[robot-json-runtime] ${robotRow.technical_id}: job.json removido após encerramento manual do robô.`);
    return;
  }

  if (!isFresh && staleExecutionRequestId) {
    try {
      await finalizeExecutionRequest(
        supabase,
        staleExecutionRequestId,
        false,
        "Robot heartbeat timeout",
        { timeout: true },
      );
    } catch (error) {
      logger?.warn?.(`[robot-json-runtime] Timeout finalize falhou para ${robotRow.technical_id}: ${error?.message || error}`);
    }
  }

  if ((isFresh && heartbeatStatus === "inactive") || !isFresh) {
    try {
      await clearOfflineRobotQueue(supabase, {
        officeId,
        robotTechnicalId: robotRow.technical_id,
        skipExecutionRequestId: staleExecutionRequestId,
        reason: !isFresh
          ? "Robô offline; fila removida automaticamente"
          : "Robô marcado como inativo; fila removida automaticamente",
      });
      await pauseOfflineRobotSchedule(supabase, {
        officeId,
        robotTechnicalId: robotRow.technical_id,
      });
    } catch (error) {
      logger?.warn?.(
        `[robot-json-runtime] ${robotRow.technical_id}: falha ao limpar fila/agendamento offline: ${error?.message || error}`,
      );
    }
  }

  if (!isFresh && fs.existsSync(runtimePaths.jobFilePath) && !isResultForJob(existingResultPayload, staleJobPayload)) {
    const timeoutResult = {
      event_id: staleExecutionRequestId ?? `${robotRow.technical_id}-timeout-${Date.now()}`,
      job_id: executionIdFromPayload(staleJobPayload),
      execution_request_id: staleExecutionRequestId,
      robot_technical_id: robotRow.technical_id,
      status: "failed",
      started_at: staleJobPayload?.created_at ?? new Date().toISOString(),
      finished_at: new Date().toISOString(),
      error_message: "Robot heartbeat timeout",
      summary: { timeout: true },
      company_results: [],
      payload: {},
      _generated_by_connector: true,
    };
    writeProcessedJson(runtimePaths.resultFilePath, timeoutResult);
    await upsertOfficeRobotRuntime(supabase, {
      office_id: officeId,
      office_server_id: officeServerId,
      robot_technical_id: robotRow.technical_id,
      status: "inactive",
      last_heartbeat_at: Number.isFinite(heartbeatUpdatedAt) ? new Date(heartbeatUpdatedAt).toISOString() : null,
      current_execution_request_id: null,
      current_job_id: null,
      runtime_version: heartbeat?.runtime_version ?? null,
      host_name: heartbeat?.host_name ?? null,
      heartbeat_payload: {
        ...(heartbeat ?? {}),
        status: "inactive",
        message: "timeout_result_written",
        cleared_at: new Date().toISOString(),
      },
    });
    logger?.warn?.(`[robot-json-runtime] ${robotRow.technical_id}: job.json obsoleto limpo após timeout de heartbeat.`);
  }
  const hasActiveFiles = hasUnprocessedJobFile;
  const heartbeatCurrentExecutionRequestId =
    String(heartbeat?.current_execution_request_id || "").trim() || null;

  if (isFresh && !hasActiveFiles && !heartbeatCurrentExecutionRequestId) {
    const staleBeforeIso = new Date(now - 120_000).toISOString();
    const { data: orphanRunningRows, error: orphanRunningError } = await supabase
      .from("execution_requests")
      .select("id,started_at,claimed_at")
      .eq("office_id", officeId)
      .eq("robot_id", robotRow.id)
      .eq("claimed_by_server_id", officeServerId)
      .eq("status", "running")
      .lt("started_at", staleBeforeIso)
      .order("started_at", { ascending: true });
    if (orphanRunningError) throw orphanRunningError;

    for (const orphanRow of orphanRunningRows ?? []) {
      try {
        await finalizeExecutionRequest(
          supabase,
          orphanRow.id,
          false,
          "Robot execution lost without active job/result file",
          { orphaned: true },
        );
        logger?.warn?.(
          `[robot-json-runtime] ${robotRow.technical_id}: execution_request ${orphanRow.id} finalizado como orfao.`,
        );
      } catch (error) {
        logger?.warn?.(
          `[robot-json-runtime] ${robotRow.technical_id}: falha ao finalizar orfao ${orphanRow.id}: ${error?.message || error}`,
        );
      }
    }
  }
}

async function dispatchPendingJob(supabase, context, robotRow, runtimePaths) {
  if (!fs.existsSync(runtimePaths.runtimeRoot)) {
    return false;
  }

  const heartbeat = safeReadJson(runtimePaths.heartbeatFilePath);
  const heartbeatUpdatedAt = heartbeat?.updated_at ? Date.parse(heartbeat.updated_at) : Number.NaN;
  const robotBusy =
    Number.isFinite(heartbeatUpdatedAt) &&
    Date.now() - heartbeatUpdatedAt <= 90_000 &&
    String(heartbeat?.status || "").trim().toLowerCase() === "processing";
  if (robotBusy) return false;

  const executionRequest = await claimNextExecutionRequest(supabase, {
    officeId: context.officeId,
    officeServerId: context.officeServerId,
    robotRow,
  });
  if (!executionRequest) return false;

  const companies = await loadCompaniesForExecution(supabase, executionRequest, robotRow);
  const jobPayload = buildJobPayload({
    officeId: context.officeId,
    officeServerId: context.officeServerId,
    basePath: context.basePath,
    robotsRootPath: context.robotsRootPath,
    robotRow,
    executionRequest,
    companies,
  });

  await supabase
    .from("execution_requests")
    .update({
      job_payload: jobPayload,
      company_ids: jobPayload.company_ids,
    })
    .eq("id", executionRequest.id);

  writeJsonAtomic(runtimePaths.jobFilePath, jobPayload);
  await upsertOfficeRobotRuntime(supabase, {
    office_id: context.officeId,
    office_server_id: context.officeServerId,
    robot_technical_id: robotRow.technical_id,
    status: "active",
    last_heartbeat_at: null,
    current_execution_request_id: executionRequest.id,
    current_job_id: executionRequest.id,
    heartbeat_payload: {
      source: "dispatcher",
      queued_at: new Date().toISOString(),
    },
  });

  await supabase
    .from("office_servers")
    .update({ last_job_at: new Date().toISOString(), last_seen_at: new Date().toISOString() })
    .eq("id", context.officeServerId);

  return true;
}

function startRobotJsonRuntimeWorker({
  supabase,
  getContext,
  logger = console,
  dispatchIntervalMs = 5_000,
  heartbeatIntervalMs = 10_000,
  resultIntervalMs = 5_000,
}) {
  let dispatchRunning = false;
  let heartbeatRunning = false;
  let resultRunning = false;

  const iterate = async (fn, guardName) => {
    if (guardName.running) return;
    guardName.running = true;
    try {
      const context = await getContext();
      if (!context?.officeId || !context?.officeServerId || !context?.robotsRootPath) return;

      const robotRows = await listRobotRuntimeRows(supabase, context.officeId, context.officeServerId);
      for (const robotRow of robotRows) {
        const runtimePaths = buildRuntimePaths(context.robotsRootPath, robotRow);
        if (!runtimePaths) continue;
        runtimePaths.officeServerId = context.officeServerId;
        await fn(context, robotRow, runtimePaths);
      }
    } catch (error) {
      logger.error?.("[robot-json-runtime] erro:", error?.message ?? error);
    } finally {
      guardName.running = false;
    }
  };

  const dispatchGuard = { running: dispatchRunning };
  const heartbeatGuard = { running: heartbeatRunning };
  const resultGuard = { running: resultRunning };

  const dispatchTick = () =>
    iterate(
      async (context, robotRow, runtimePaths) => {
        await dispatchPendingJob(supabase, context, robotRow, runtimePaths);
      },
      dispatchGuard,
    );

  const heartbeatTick = () =>
    iterate(
      async (context, robotRow, runtimePaths) => {
        await readHeartbeatAndSync(supabase, {
          officeId: context.officeId,
          officeServerId: context.officeServerId,
          robotRow,
          runtimePaths,
          logger,
        });
      },
      heartbeatGuard,
    );

  const resultTick = () =>
    iterate(
      async (context, robotRow, runtimePaths) => {
        await ingestResultFile(supabase, context.officeId, robotRow, runtimePaths, logger);
      },
      resultGuard,
    );

  void dispatchTick();
  void heartbeatTick();
  void resultTick();

  setInterval(() => void dispatchTick(), dispatchIntervalMs);
  setInterval(() => void heartbeatTick(), heartbeatIntervalMs);
  setInterval(() => void resultTick(), resultIntervalMs);
}

  return {
    listRobotRuntimeRows,
    buildRuntimePaths,
    writeJsonAtomic,
    removeFileIfExists,
    startRobotJsonRuntimeWorker,
  };
})();

const {
  listRobotRuntimeRows,
  buildRuntimePaths,
  writeJsonAtomic,
  removeFileIfExists,
} = robotJsonRuntimeModule;

const declarationArtifactsModule = (() => {
const ACTION_TITLES = {
  simples_emitir_guia: "Guias do Simples Nacional",
  simples_extrato: "Extratos do Simples Nacional",
  simples_defis: "DEFIS",
  mei_declaracao_anual: "Declaracao anual do MEI",
  mei_guias_mensais: "Guias mensais do MEI",
};

const ACTION_ROBOT_CANDIDATES = {
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

const ACTION_TEXT_MATCHERS = {
  simples_emitir_guia: ["simples", "guia", "das"],
  simples_extrato: ["simples", "extrato"],
  simples_defis: ["defis"],
  mei_declaracao_anual: ["mei", "declaracao", "anual"],
  mei_guias_mensais: ["mei", "guia"],
};

function coerceJsonObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function normalizeToken(value) {
  return String(value || "")
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .trim()
    .toLowerCase();
}

function isKnownAction(action) {
  return Object.prototype.hasOwnProperty.call(ACTION_TITLES, action);
}

function normalizeAction(action) {
  const value = String(action || "").trim();
  return isKnownAction(value) ? value : null;
}

function splitLogicalSegments(logicalPath) {
  const normalized = String(logicalPath || "").replace(/\\/g, "/");
  const parts = normalized.split("/").map((part) => part.trim()).filter(Boolean);
  if (parts.some((part) => part === "." || part === "..")) {
    const error = new Error("Caminho logico invalido para declaracoes.");
    error.statusCode = 400;
    throw error;
  }
  return parts;
}

function sanitizeDiskSegment(value) {
  const cleaned = String(value || "")
    .replace(/[<>:"/\\|?*\u0000-\u001F]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!cleaned || cleaned === "." || cleaned === "..") {
    const error = new Error("Segmento de disco invalido para declaracoes.");
    error.statusCode = 400;
    throw error;
  }
  return cleaned;
}

function normalizeRelativeFromBase(basePath, fullPath) {
  return path.relative(path.resolve(basePath), fullPath).replace(/\\/g, "/");
}

function resolveWithin(basePath, parts) {
  const root = path.resolve(basePath);
  const target = path.resolve(root, ...parts);
  if (!target.startsWith(root)) {
    const error = new Error("Caminho fora do diretorio base.");
    error.statusCode = 403;
    throw error;
  }
  return target;
}

function ensureSafeDirectory(rootPath, directoryPath) {
  if (!fs.existsSync(directoryPath)) return null;
  const stats = fs.lstatSync(directoryPath);
  if (stats.isSymbolicLink() || !stats.isDirectory()) return null;
  const realPath = fs.realpathSync(directoryPath);
  if (!realPath.startsWith(path.resolve(rootPath))) {
    const error = new Error("Diretorio fora do escopo permitido.");
    error.statusCode = 403;
    throw error;
  }
  return realPath;
}

function ensureSafeFile(rootPath, fullPath) {
  if (!fs.existsSync(fullPath)) return null;
  const stats = fs.lstatSync(fullPath);
  if (stats.isSymbolicLink() || !stats.isFile()) return null;
  const realPath = fs.realpathSync(fullPath);
  if (!realPath.startsWith(path.resolve(rootPath))) {
    const error = new Error("Arquivo fora do escopo permitido.");
    error.statusCode = 403;
    throw error;
  }
  return {
    realPath,
    stats: fs.statSync(realPath),
  };
}

function encodeArtifactKey(relativePath) {
  return Buffer.from(String(relativePath || ""), "utf8").toString("base64url");
}

function decodeArtifactKey(value) {
  try {
    return Buffer.from(String(value || ""), "base64url").toString("utf8");
  } catch {
    return null;
  }
}

function parseCompetence(value) {
  const raw = String(value || "").trim();
  if (!/^\d{4}-\d{2}$/.test(raw)) return null;
  const [year, month] = raw.split("-").map(Number);
  if (month < 1 || month > 12) return null;
  return {
    raw,
    year: String(year),
    month: String(month).padStart(2, "0"),
  };
}

function buildDateSegments(dateRule, competence) {
  if (!dateRule) return [];
  if (!competence) return [];
  if (dateRule === "year") return [competence.year];
  if (dateRule === "year_month" || dateRule === "year_month_day") {
    return [competence.year, competence.month];
  }
  return [];
}

function walkDirectoryFiles(rootPath, directoryPath, recursive) {
  const safeDirectory = ensureSafeDirectory(rootPath, directoryPath);
  if (!safeDirectory) return [];

  const files = [];
  const entries = fs.readdirSync(safeDirectory, { withFileTypes: true });
  for (const entry of entries) {
    if (entry.name.startsWith(".")) continue;
    const entryPath = path.join(safeDirectory, entry.name);
    if (entry.isDirectory()) {
      if (recursive) {
        files.push(...walkDirectoryFiles(rootPath, entryPath, true));
      }
      continue;
    }
    const safeFile = ensureSafeFile(rootPath, entryPath);
    if (safeFile) {
      files.push(safeFile);
    }
  }
  return files;
}

function findRobotForAction(action, robots) {
  const candidates = ACTION_ROBOT_CANDIDATES[action] || [];
  const exact = robots.find((robot) => candidates.includes(robot.technical_id));
  if (exact) return exact;

  const matchers = ACTION_TEXT_MATCHERS[action] || [];
  return (
    robots.find((robot) => {
      const haystack = normalizeToken(`${robot.technical_id} ${robot.display_name} ${robot.segment_path || ""}`);
      return matchers.every((token) => haystack.includes(normalizeToken(token)));
    }) ?? null
  );
}

function resolveFolderTrailByLogicalPath(nodes, logicalPath) {
  const segments = splitLogicalSegments(logicalPath);
  const byParentAndSlug = new Map();
  for (const node of nodes) {
    const slug = normalizeToken(String(node.slug || node.name || ""));
    const key = `${node.parent_id ?? "root"}:${slug}`;
    byParentAndSlug.set(key, node);
  }

  let parentId = null;
  const trail = [];
  for (const segment of segments) {
    const key = `${parentId ?? "root"}:${normalizeToken(segment)}`;
    const currentNode = byParentAndSlug.get(key) ?? null;
    if (!currentNode) return [];
    trail.push(currentNode);
    parentId = currentNode.id;
  }
  return trail;
}

async function resolveDeclarationSource({
  supabase,
  officeId,
  officeServerId,
  action,
}) {
  const mergedRobots = await listRobotRuntimeRows(supabase, officeId, officeServerId);
  const robot = findRobotForAction(action, mergedRobots);
  if (!robot) {
    return {
      status: "robot_missing",
      reason: "Nenhum robo compativel foi configurado para esta rotina.",
      robotTechnicalId: null,
      actionKey: action,
    };
  }

  const segmentPath = String(robot.segment_path || "").trim();
  if (!segmentPath) {
    return {
      status: "segment_missing",
      reason: "O robo desta rotina ainda nao possui segment_path configurado.",
      robotTechnicalId: robot.technical_id,
      actionKey: action,
    };
  }

  const actionKey = action;
  const logicalFolderPath = splitLogicalSegments(segmentPath).join("/");
  const { data: folderNodes, error: folderNodesError } = await supabase
    .from("folder_structure_nodes")
    .select("id, parent_id, name, slug, date_rule, position")
    .eq("office_id", officeId)
    .order("parent_id", { nullsFirst: true })
    .order("position", { ascending: true });
  if (folderNodesError) throw folderNodesError;

  const folderTrail = resolveFolderTrailByLogicalPath(folderNodes ?? [], logicalFolderPath);
  const leafNode = folderTrail[folderTrail.length - 1] ?? null;
  if (!leafNode) {
    return {
      status: "folder_missing",
      reason: "O segment_path desta rotina nao foi encontrado na estrutura de pastas do escritorio.",
      robotTechnicalId: robot.technical_id,
      actionKey,
      segmentPath,
      logicalFolderPath,
    };
  }

  return {
    status: "ready",
    reason: null,
    robotTechnicalId: robot.technical_id,
    robotDisplayName: robot.display_name,
    actionKey,
    segmentPath,
    logicalFolderPath,
    physicalFolderPath: folderTrail
      .map((node) => sanitizeDiskSegment(String(node.name || node.slug || "")))
      .filter(Boolean)
      .join("/"),
    dateRule: leafNode.date_rule ?? null,
  };
}

function buildArtifactResponse(source, items, competence) {
  return {
    action: source.action,
    title: ACTION_TITLES[source.action],
    source: {
      status: source.status,
      reason: source.reason ?? null,
      robot_technical_id: source.robotTechnicalId ?? null,
      robot_display_name: source.robotDisplayName ?? null,
      action_key: source.actionKey ?? null,
      segment_path: source.segmentPath ?? null,
      logical_folder_path: source.logicalFolderPath ?? null,
      physical_folder_path: source.physicalFolderPath ?? null,
      date_rule: source.dateRule ?? null,
      competence: competence?.raw ?? null,
    },
    items,
  };
}

async function listDeclarationArtifacts({
  supabase,
  officeId,
  officeServerId,
  basePath,
  action,
  companyIds,
  competence,
  limit = 200,
}) {
  const normalizedAction = normalizeAction(action);
  if (!normalizedAction) {
    const error = new Error("Acao de declaracao invalida.");
    error.statusCode = 400;
    throw error;
  }

  const parsedCompetence = parseCompetence(competence);
  if (competence && !parsedCompetence) {
    const error = new Error("Competencia invalida. Use o formato AAAA-MM.");
    error.statusCode = 400;
    throw error;
  }

  const source = await resolveDeclarationSource({
    supabase,
    officeId,
    officeServerId,
    action: normalizedAction,
  });
  source.action = normalizedAction;
  if (source.status !== "ready") {
    return buildArtifactResponse(source, [], parsedCompetence);
  }

  const requestedCompanyIds = Array.from(
    new Set((Array.isArray(companyIds) ? companyIds : []).map((value) => String(value || "").trim()).filter(Boolean)),
  );
  const companyQuery = supabase
    .from("companies")
    .select("id, name, document")
    .eq("office_id", officeId);
  const { data: companies, error: companiesError } =
    requestedCompanyIds.length > 0
      ? await companyQuery.in("id", requestedCompanyIds)
      : await companyQuery;
  if (companiesError) throw companiesError;

  const logicalSegments = splitLogicalSegments(
    source.physicalFolderPath || source.logicalFolderPath,
  );
  const dateSegments = buildDateSegments(source.dateRule, parsedCompetence);
  const recursive = !parsedCompetence
    ? Boolean(source.dateRule)
    : source.dateRule === "year_month_day";
  const items = [];

  for (const company of companies ?? []) {
    const companySegments = [sanitizeDiskSegment(company.name), ...logicalSegments, ...dateSegments];
    const directoryPath = resolveWithin(basePath, companySegments);
    const files = walkDirectoryFiles(basePath, directoryPath, recursive);

    for (const file of files) {
      const relativePath = normalizeRelativeFromBase(basePath, file.realPath);
      items.push({
        artifact_key: encodeArtifactKey(relativePath),
        company_id: company.id,
        company_name: company.name,
        company_document: company.document ?? null,
        file_name: path.basename(file.realPath),
        modified_at: file.stats.mtime.toISOString(),
        size_bytes: file.stats.size,
      });
    }
  }

  items.sort((left, right) => {
    const modifiedCompare = String(right.modified_at || "").localeCompare(String(left.modified_at || ""));
    if (modifiedCompare !== 0) return modifiedCompare;
    const companyCompare = String(left.company_name || "").localeCompare(String(right.company_name || ""), "pt-BR");
    if (companyCompare !== 0) return companyCompare;
    return String(left.file_name || "").localeCompare(String(right.file_name || ""), "pt-BR");
  });

  return buildArtifactResponse(source, items.slice(0, Math.max(1, Math.min(limit, 500))), parsedCompetence);
}

async function resolveDeclarationArtifactDownload({
  supabase,
  officeId,
  officeServerId,
  basePath,
  action,
  companyId,
  competence,
  artifactKey,
}) {
  const relativePath = decodeArtifactKey(artifactKey);
  if (!relativePath) {
    const error = new Error("Artefato invalido para download.");
    error.statusCode = 400;
    throw error;
  }

  const listing = await listDeclarationArtifacts({
    supabase,
    officeId,
    officeServerId,
    basePath,
    action,
    companyIds: companyId ? [companyId] : [],
    competence,
    limit: 500,
  });

  const artifact = (listing.items ?? []).find((item) => item.artifact_key === artifactKey);
  if (!artifact) {
    const error = new Error("Artefato nao autorizado para este escritorio.");
    error.statusCode = 403;
    throw error;
  }

  const fullPath = resolveWithin(basePath, splitLogicalSegments(relativePath));
  const safeFile = ensureSafeFile(basePath, fullPath);
  if (!safeFile) {
    const error = new Error("Arquivo nao encontrado no servidor.");
    error.statusCode = 404;
    throw error;
  }

  return {
    artifact,
    fileName: path.basename(safeFile.realPath),
    fullPath: safeFile.realPath,
  };
}

  return {
    listDeclarationArtifacts,
    resolveDeclarationArtifactDownload,
  };
})();

const scheduleRulesWorkerModule = (() => {
  const { listRobotRuntimeRows } = robotJsonRuntimeModule;
  const { filterEligibleCompaniesForRobot, getRequestedCityNameFromJob } = robotEligibility;
/**
 * Dispara execution_requests para regras de agendamento ativas (run_daily) quando
 * o horário em America/Sao_Paulo já passou e ainda não houve execução no dia.
 *
 * O painel só enfileira na primeira gravação se "agora" >= horário; sem este worker,
 * as execuções seguintes (24h) não ocorreriam sem o servidor consultando o Supabase.
 */


const TZ = "America/Sao_Paulo";
const ROBOT_OPERATION_SCOPE = "robot_operation";
const DEFAULT_DAILY_RUN_TIME = "03:00";

function coerceJsonObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function dateKeyInTz(isoOrDate, tz) {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: tz,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(typeof isoOrDate === "string" ? new Date(isoOrDate) : isoOrDate);
}

function todayYmdSp() {
  return dateKeyInTz(new Date(), TZ);
}

function parseYmd(ymd) {
  const raw = String(ymd || "").trim().slice(0, 10);
  return /^\d{4}-\d{2}-\d{2}$/.test(raw) ? raw : null;
}

function subDaysYmd(ymd, days) {
  const [y, m, d] = ymd.split("-").map(Number);
  const t = new Date(Date.UTC(y, m - 1, d - days));
  return `${t.getUTCFullYear()}-${String(t.getUTCMonth() + 1).padStart(2, "0")}-${String(t.getUTCDate()).padStart(2, "0")}`;
}

function monthEndYmd(ymd) {
  const [y, m] = ymd.split("-").map(Number);
  const last = new Date(Date.UTC(y, m, 0)).getUTCDate();
  return `${y}-${String(m).padStart(2, "0")}-${String(last).padStart(2, "0")}`;
}

function monthStartYmd(ymd) {
  const [y, m] = ymd.split("-").map(Number);
  return `${y}-${String(m).padStart(2, "0")}-01`;
}

function parseRunAtTime(rule) {
  const timeStr = String(rule.run_at_time || "").trim().slice(0, 5);
  const parts = timeStr.split(":");
  const h = Math.min(23, Math.max(0, parseInt(parts[0], 10) || 0));
  const m = Math.min(59, Math.max(0, parseInt(parts[1], 10) || 0));
  return { h, m };
}

function getNowHmSp() {
  const f = new Intl.DateTimeFormat("en-GB", {
    timeZone: TZ,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const parts = Object.fromEntries(
    f.formatToParts(new Date()).map((p) => [p.type, p.value]),
  );
  return { h: Number(parts.hour), m: Number(parts.minute) };
}

function isPastRunTimeTodaySp(rule) {
  const { h: rh, m: rm } = parseRunAtTime(rule);
  const { h: nh, m: nm } = getNowHmSp();
  return nh * 60 + nm >= rh * 60 + rm;
}

function isFiscalNotesRobot(robot) {
  const segmentPath = (robot.segment_path || "").toUpperCase();
  return Boolean(
    robot.is_fiscal_notes_robot ||
      robot.fiscal_notes_kind ||
      robot.notes_mode ||
      segmentPath.includes("FISCAL/NFS") ||
      segmentPath.includes("FISCAL/NFE") ||
      segmentPath.includes("FISCAL/NFC"),
  );
}

function getRobotNotesMode(robot) {
  if (!robot.is_fiscal_notes_robot || !robot.fiscal_notes_kind) return null;
  const kind = robot.fiscal_notes_kind;
  const mode = robot.notes_mode;
  const nfs = ["recebidas", "emitidas", "both"];
  const nfe = ["modelo_55", "modelo_65", "modelos_55_65"];
  const allowed = kind === "nfe_nfc" ? nfe : nfs;
  if (!mode || !allowed.includes(mode)) {
    return kind === "nfe_nfc" ? "modelo_55" : "recebidas";
  }
  return mode;
}

function computeScheduledPeriodForRobot(robot, todayYmd) {
  if (isFiscalNotesRobot(robot) && robot.date_execution_mode === "interval") {
    const yesterday = subDaysYmd(todayYmd, 1);
    return { periodStart: yesterday, periodEnd: yesterday };
  }
  if (robot.date_execution_mode === "competencia") {
    const start = monthStartYmd(todayYmd);
    const end = monthEndYmd(todayYmd);
    return { periodStart: start, periodEnd: end };
  }
  if (robot.date_execution_mode === "interval") {
    if (robot.last_period_end) {
      const y = subDaysYmd(todayYmd, 1);
      return { periodStart: y, periodEnd: y };
    }
    if (robot.initial_period_start && robot.initial_period_end) {
      return {
        periodStart: robot.initial_period_start,
        periodEnd: robot.initial_period_end,
      };
    }
  }
  const y = subDaysYmd(todayYmd, 1);
  return { periodStart: y, periodEnd: y };
}

function buildRobotExecutionSnapshot(robot) {
  const executionDefaults = coerceJsonObject(robot.execution_defaults);
  const cityName = String(executionDefaults.city_name ?? "").trim();
  return {
    execution_defaults: executionDefaults,
    admin_settings: coerceJsonObject(robot.admin_settings),
    runtime_defaults: coerceJsonObject(robot.runtime_defaults),
    city_name: cityName || null,
    date_execution_mode: robot.date_execution_mode ?? null,
    segment_path: robot.segment_path ?? null,
    notes_mode: robot.notes_mode ?? null,
  };
}

function buildJobPayloadForRobot(robot, companyIds, periodStart, periodEnd) {
  return {
    ...buildRobotExecutionSnapshot(robot),
    company_ids: companyIds,
    period_start: periodStart,
    period_end: periodEnd,
  };
}

function normalizeSelectionMode(value) {
  return String(value || "").trim() === "manual_companies"
    ? "manual_companies"
    : "all_eligible";
}

function getRobotOperationRuleSettings(rule) {
  const settings = coerceJsonObject(rule?.settings);
  if (String(settings.scope || "").trim() !== ROBOT_OPERATION_SCOPE) return null;
  const robotTechnicalId =
    String(settings.robot_technical_id || "").trim() ||
    String(rule?.robot_technical_ids?.[0] || "").trim();
  if (!robotTechnicalId) return null;

  return {
    scope: ROBOT_OPERATION_SCOPE,
    robotTechnicalId,
    selectionMode: normalizeSelectionMode(settings.selection_mode),
    autoDaily: settings.auto_daily !== false,
  };
}

function isRobotOperationRule(rule) {
  return Boolean(getRobotOperationRuleSettings(rule));
}

function buildRobotOperationRuleSettings(robot, selectionMode = "all_eligible", autoDaily = true) {
  return {
    scope: ROBOT_OPERATION_SCOPE,
    robot_technical_id: robot.technical_id,
    selection_mode: normalizeSelectionMode(selectionMode),
    auto_daily: autoDaily !== false,
    robot_snapshots: {
      [robot.technical_id]: buildRobotExecutionSnapshot(robot),
    },
  };
}

async function ensureRobotOperationDailyRules(supabase, officeId, mergedRobots, logger) {
  const { data: existingRules, error } = await supabase
    .from("schedule_rules")
    .select("*")
    .eq("office_id", officeId);
  if (error) throw error;

  const existingByRobotId = new Map();
  for (const rule of existingRules ?? []) {
    const settings = getRobotOperationRuleSettings(rule);
    if (!settings?.robotTechnicalId) continue;
    existingByRobotId.set(settings.robotTechnicalId, rule);
  }

  const todayYmd = todayYmdSp();
  for (const robot of mergedRobots) {
    if (existingByRobotId.has(robot.technical_id)) continue;

    const row = {
      office_id: officeId,
      company_ids: [],
      robot_technical_ids: [robot.technical_id],
      notes_mode: getRobotNotesMode(robot),
      period_start: null,
      period_end: null,
      run_at_date: todayYmd,
      run_at_time: DEFAULT_DAILY_RUN_TIME,
      run_daily: true,
      execution_mode: "sequential",
      settings: buildRobotOperationRuleSettings(robot, "all_eligible", true),
      status: "active",
      created_by: null,
    };

    const { error: insertError } = await supabase.from("schedule_rules").insert(row);
    if (insertError) throw insertError;
    logger?.log?.(
      `[schedule-rules-worker] regra diaria automatica criada para ${robot.technical_id}.`,
    );
  }
}

function shouldDispatchScheduleRule(rule) {
  if (!rule?.run_daily || rule.status !== "active") return false;
  const todayYmd = todayYmdSp();
  const runDate = parseYmd(rule.run_at_date);
  if (runDate && runDate > todayYmd) return false;

  if (!isPastRunTimeTodaySp(rule)) return false;

  if (rule.last_run_at) {
    const lastKey = dateKeyInTz(rule.last_run_at, TZ);
    if (lastKey === todayYmd) return false;
  }

  return true;
}

function expandRobotTechnicalIds(rule, mergedRobots) {
  const raw = Array.isArray(rule.robot_technical_ids) ? rule.robot_technical_ids : [];
  if (raw.includes("all")) {
    return mergedRobots.map((r) => r.technical_id);
  }
  return raw.filter((id) => id && id !== "all");
}

async function processDueScheduleRules({ supabase, officeId, officeServerId, logger }) {
  if (!officeId || !officeServerId) return;

  const mergedRobots = await listRobotRuntimeRows(supabase, officeId, officeServerId);
  const robotByTechnicalId = new Map(mergedRobots.map((r) => [r.technical_id, r]));
  await ensureRobotOperationDailyRules(supabase, officeId, mergedRobots, logger);

  const { data: rules, error: rulesError } = await supabase
    .from("schedule_rules")
    .select("*")
    .eq("office_id", officeId)
    .eq("status", "active")
    .eq("run_daily", true);

  if (rulesError) throw rulesError;

  const todayYmd = todayYmdSp();
  const { data: officeCompanies, error: officeCompaniesError } = await supabase
    .from("companies")
    .select("id, active, document, auth_mode, cert_blob_b64, cert_password, cert_valid_until, contador_cpf, state_registration, city_name, cae, sefaz_go_logins")
    .eq("office_id", officeId);
  if (officeCompaniesError) throw officeCompaniesError;

  const officeCompanyIds = (officeCompanies ?? []).map((company) => company.id);
  const officeCompaniesById = new Map((officeCompanies ?? []).map((company) => [company.id, company]));

  for (const rule of rules ?? []) {
    if (!shouldDispatchScheduleRule(rule)) continue;

    const { data: inflight, error: inflightError } = await supabase
      .from("execution_requests")
      .select("id")
      .eq("schedule_rule_id", rule.id)
      .in("status", ["pending", "running"])
      .limit(1);
    if (inflightError) throw inflightError;
    if ((inflight ?? []).length > 0) {
      logger?.log?.(
        `[schedule-rules-worker] regra ${rule.id}: fila ainda com pending/running; pulando.`,
      );
      continue;
    }

    const orderedIds = expandRobotTechnicalIds(rule, mergedRobots);
    if (orderedIds.length === 0) continue;
    const robotOperationSettings = getRobotOperationRuleSettings(rule);
    const companyIds =
      robotOperationSettings?.selectionMode === "all_eligible"
        ? officeCompanyIds
        : (Array.isArray(rule.company_ids) ? rule.company_ids : []);
    if (companyIds.length === 0) continue;

    const { data: configRows, error: configError } = await supabase
      .from("company_robot_config")
      .select("*")
      .in("company_id", companyIds)
      .in("robot_technical_id", orderedIds);
    if (configError) throw configError;

    const configByRobotTechnicalId = new Map();
    for (const row of configRows ?? []) {
      const byCompany = configByRobotTechnicalId.get(row.robot_technical_id) ?? new Map();
      byCompany.set(row.company_id, row);
      configByRobotTechnicalId.set(row.robot_technical_id, byCompany);
    }

    const executionGroupId = randomUUID();
    const executionMode = String(rule.execution_mode || "sequential").trim().toLowerCase() === "parallel" ? "parallel" : "sequential";
    let created = 0;

    for (const [index, technicalId] of orderedIds.entries()) {
      const robot = robotByTechnicalId.get(technicalId);
      if (!robot) continue;
      if (String(robot.runtime_status || "").trim().toLowerCase() === "inactive") {
        logger?.log?.(
          `[schedule-rules-worker] regra ${rule.id}: robô ${technicalId} inativo no runtime; não será enfileirado.`,
        );
        continue;
      }

      const scheduledPeriod = computeScheduledPeriodForRobot(robot, todayYmd);
      const periodStart = parseYmd(rule.period_start) || scheduledPeriod.periodStart;
      const periodEnd = parseYmd(rule.period_end) || scheduledPeriod.periodEnd;
      const requestedCityName = getRequestedCityNameFromJob(buildRobotExecutionSnapshot(robot), robot);
      const eligibleCompanies = filterEligibleCompaniesForRobot({
        robotRow: robot,
        companies: companyIds.map((companyId) => {
          const company = officeCompaniesById.get(companyId);
          return company ? { ...company, company_id: company.id } : null;
        }).filter(Boolean),
        configByCompanyId: configByRobotTechnicalId.get(technicalId) ?? new Map(),
        cityName: requestedCityName,
      });
      const eligibleCompanyIds = eligibleCompanies.map((company) => company.company_id || company.id);
      if (eligibleCompanyIds.length === 0) continue;

      const jobPayload = buildJobPayloadForRobot(robot, eligibleCompanyIds, periodStart, periodEnd);
      const row = {
        office_id: officeId,
        company_ids: eligibleCompanyIds,
        robot_technical_ids: [technicalId],
        period_start: periodStart,
        period_end: periodEnd,
        notes_mode: rule.notes_mode ?? getRobotNotesMode(robot),
        schedule_rule_id: rule.id,
        execution_mode: executionMode,
        execution_group_id: executionGroupId,
        execution_order: index,
        job_payload: jobPayload,
        source: robotOperationSettings
          ? (robotOperationSettings.autoDaily ? "robot_auto" : "robot_schedule")
          : "scheduler",
        status: "pending",
        created_by: null,
      };

      const { error: insertError } = await supabase.from("execution_requests").insert(row);
      if (insertError) throw insertError;
      created += 1;
    }

    if (created === 0) {
      logger?.warn?.(
        `[schedule-rules-worker] regra ${rule.id}: nenhuma empresa elegível; last_run_at não atualizado.`,
      );
      continue;
    }

    const { error: updateRuleError } = await supabase
      .from("schedule_rules")
      .update({ last_run_at: new Date().toISOString(), updated_at: new Date().toISOString() })
      .eq("id", rule.id);
    if (updateRuleError) throw updateRuleError;

    logger?.log?.(
      `[schedule-rules-worker] regra ${rule.id}: ${created} job(s) enfileirado(s) (agendador diário).`,
    );
  }
}

function startScheduleRulesWorker({ supabase, officeId, officeServerId, logger = console }) {
  const intervalMs = Number(process.env.SCHEDULE_RULES_WORKER_INTERVAL_MS || 60_000);
  let running = false;
  const tick = async () => {
    if (running) return;
    running = true;
    try {
      await processDueScheduleRules({ supabase, officeId, officeServerId, logger });
    } catch (err) {
      logger?.error?.("[schedule-rules-worker] erro:", err?.message ?? err);
    } finally {
      running = false;
    }
  };
  void tick();
  setInterval(() => void tick(), intervalMs);
}

  return {
    processDueScheduleRules,
    startScheduleRulesWorker,
  };
})();

const { startRobotJsonRuntimeWorker } = robotJsonRuntimeModule;
const { startScheduleRulesWorker } = scheduleRulesWorkerModule;
const { listDeclarationArtifacts, resolveDeclarationArtifactDownload } = declarationArtifactsModule;

const app = express();
const PORT = process.env.PORT || 3001;

// Necessário quando roda atrás de proxy (ngrok) para rate-limit e IP correto
// (ngrok envia X-Forwarded-For)
app.set("trust proxy", 1);

// Base path: .env tem prioridade; Supabase só sobrescreve se BASE_PATH não veio do .env
const ENV_BASE_PATH = (process.env.BASE_PATH || "").trim();
let BASE_PATH = ENV_BASE_PATH || "C:\\Users\\ROBO\\Documents";
const ENV_ROBOTS_ROOT_PATH = (process.env.ROBOTS_ROOT_PATH || "").trim();
let ROBOTS_ROOT_PATH = ENV_ROBOTS_ROOT_PATH || "C:\\Users\\ROBO\\Documents\\ROBOS";
let OFFICE_SERVER_ID = null;
let OFFICE_ID = null;
let OFFICE_NAME = null;
const FISCAL_SYNC_VERSION = "2025-03-18-office-id-fix-v2";

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isTransientSupabaseError(error) {
  const message = String(error?.message ?? error ?? "").toLowerCase();
  return (
    message.includes("fetch failed") ||
    message.includes("bad gateway") ||
    message.includes("gateway") ||
    message.includes("502") ||
    message.includes("503") ||
    message.includes("504") ||
    message.includes("429") ||
    message.includes("econnreset") ||
    message.includes("etimedout")
  );
}

function createRetryingFetch(baseFetch = fetch, options = {}) {
  const maxRetries = Number(options.maxRetries ?? 3);
  const baseDelayMs = Number(options.baseDelayMs ?? 700);
  return async (input, init) => {
    let lastError = null;
    for (let attempt = 0; attempt <= maxRetries; attempt += 1) {
      try {
        const response = await baseFetch(input, init);
        if (
          response.status === 429 ||
          response.status === 502 ||
          response.status === 503 ||
          response.status === 504
        ) {
          if (attempt >= maxRetries) return response;
          await sleep(baseDelayMs * (attempt + 1));
          continue;
        }
        return response;
      } catch (error) {
        lastError = error;
        if (!isTransientSupabaseError(error) || attempt >= maxRetries) {
          throw error;
        }
        await sleep(baseDelayMs * (attempt + 1));
      }
    }
    throw lastError ?? new Error("Falha ao executar fetch com retry");
  };
}

function createConfiguredSupabaseClient(supabaseUrl, key) {
  return createClient(supabaseUrl, key, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
      detectSessionInUrl: false,
    },
    global: {
      fetch: createRetryingFetch(fetch),
    },
  });
}

async function loadBasePathFromSupabase() {
  const url = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !serviceKey) {
    console.warn(
      "[fiscal-watcher] SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY ausente no .env; vínculo com escritório não será carregado.",
    );
    return;
  }
  try {
    const supabase = createConfiguredSupabaseClient(url, serviceKey);
    if (CONNECTOR_SECRET_HASH) {
      const hashPreview = CONNECTOR_SECRET_HASH.slice(0, 8) + "...";
      const { data: credential, error: credError } = await supabase
        .from("office_server_credentials")
        .select("office_server_id")
        .eq("secret_hash", CONNECTOR_SECRET_HASH)
        .maybeSingle();
      if (credError) {
        console.error(
          "[fiscal-watcher] Erro ao buscar credential:",
          credError.message,
        );
        return;
      }
      if (!credential?.office_server_id) {
        console.warn(
          "[fiscal-watcher] Nenhum credential com hash",
          hashPreview,
          "no Supabase. Verifique CONNECTOR_SECRET no .env e office_server_credentials (secret_hash).",
        );
        return;
      }
      const { data: officeServer, error: osError } = await supabase
        .from("office_servers")
        .select("id, office_id, base_path, robots_root_path")
        .eq("id", credential.office_server_id)
        .maybeSingle();
      if (osError) {
        console.error(
          "[fiscal-watcher] Erro ao buscar office_server:",
          osError.message,
        );
        return;
      }
      if (officeServer?.id) {
        OFFICE_SERVER_ID = officeServer.id;
        OFFICE_ID = officeServer.office_id ?? null;
        const { data: office } = await supabase
          .from("offices")
          .select("name")
          .eq("id", OFFICE_ID)
          .maybeSingle();
        OFFICE_NAME = office?.name ?? null;
        if (
          !ENV_BASE_PATH &&
          officeServer?.base_path &&
          String(officeServer.base_path).trim()
        ) {
          BASE_PATH = String(officeServer.base_path).trim();
        }
        if (
          !ENV_ROBOTS_ROOT_PATH &&
          officeServer?.robots_root_path &&
          String(officeServer.robots_root_path).trim()
        ) {
          ROBOTS_ROOT_PATH = String(officeServer.robots_root_path).trim();
        }
        return;
      }
    }
    if (!ENV_BASE_PATH) {
      const { data } = await supabase
        .from("admin_settings")
        .select("value")
        .eq("key", "base_path")
        .maybeSingle();
      if (data?.value && String(data.value).trim())
        BASE_PATH = String(data.value).trim();
    }
  } catch (err) {
    console.error(
      "[fiscal-watcher] loadBasePathFromSupabase:",
      err?.message ?? err,
    );
  }
}

function createSupabaseReadClient() {
  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
  const anonKey = process.env.SUPABASE_ANON_KEY;
  const key = serviceKey || anonKey;
  if (!supabaseUrl || !key) {
    return null;
  }
  return createConfiguredSupabaseClient(supabaseUrl, key);
}

function createSupabaseServiceClient() {
  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!supabaseUrl || !serviceKey) {
    return null;
  }
  return createConfiguredSupabaseClient(supabaseUrl, serviceKey);
}

function getRobotRuntimeContext() {
  return {
    officeId: OFFICE_ID,
    officeServerId: OFFICE_SERVER_ID,
    basePath: BASE_PATH,
    robotsRootPath: ROBOTS_ROOT_PATH,
  };
}

function getBaseResolved() {
  return path.resolve(BASE_PATH);
}

function sha256Hex(value) {
  return createHash("sha256")
    .update(String(value || ""), "utf8")
    .digest("hex");
}

// Normaliza o segredo: remove espaços/quebras (evita .env com \r ou espaço) e aceita só hex quando for 64 chars
const _rawSecret = String(process.env.CONNECTOR_SECRET || "").trim();
const _cleanHex = _rawSecret.replace(/\s+/g, "").replace(/[^0-9a-fA-F]/g, "");
const CONNECTOR_SECRET =
  _cleanHex.length === 64 ? _cleanHex.toLowerCase() : _rawSecret;
const CONNECTOR_SECRET_HASH = CONNECTOR_SECRET
  ? sha256Hex(CONNECTOR_SECRET)
  : "";

function safeTokenEqual(expected, received) {
  const expectedBuffer = Buffer.from(String(expected || ""), "utf8");
  const receivedBuffer = Buffer.from(String(received || ""), "utf8");
  if (
    expectedBuffer.length === 0 ||
    expectedBuffer.length !== receivedBuffer.length
  ) {
    return false;
  }
  return timingSafeEqual(expectedBuffer, receivedBuffer);
}

function getForwardedUserJwt(req) {
  const forwardedToken = String(req.headers["x-office-user-jwt"] || "").trim();
  if (forwardedToken) return forwardedToken;
  if (!CONNECTOR_SECRET_HASH) {
    const authHeader = String(req.headers.authorization || "");
    if (authHeader.startsWith("Bearer ")) return authHeader.slice(7).trim();
  }
  return "";
}

function normalizeRelativePath(inputPath) {
  if (typeof inputPath !== "string") {
    const error = new Error("Path inválido");
    error.statusCode = 400;
    throw error;
  }
  const trimmed = inputPath.trim();
  if (!trimmed) {
    const error = new Error("Path obrigatório");
    error.statusCode = 400;
    throw error;
  }
  if (path.isAbsolute(trimmed)) {
    const error = new Error("Path absoluto não é permitido");
    error.statusCode = 403;
    throw error;
  }
  const normalized = trimmed.replace(/\//g, path.sep);
  const resolved = path.resolve(path.join(BASE_PATH, normalized));
  if (!resolved.startsWith(getBaseResolved())) {
    const error = new Error("Path fora do diretório base");
    error.statusCode = 403;
    throw error;
  }
  return { relative: normalized, resolved };
}

function ensureSafeExistingFile(fullPath) {
  if (!fs.existsSync(fullPath)) {
    const error = new Error("Arquivo não encontrado");
    error.statusCode = 404;
    throw error;
  }
  const stats = fs.lstatSync(fullPath);
  if (stats.isSymbolicLink()) {
    const error = new Error("Symlink não é permitido");
    error.statusCode = 403;
    throw error;
  }
  if (!stats.isFile()) {
    const error = new Error("Arquivo não encontrado");
    error.statusCode = 404;
    throw error;
  }
  const realPath = fs.realpathSync(fullPath);
  if (!realPath.startsWith(getBaseResolved())) {
    const error = new Error("Path fora do diretório base");
    error.statusCode = 403;
    throw error;
  }
  return { realPath };
}

function ensureSafeExistingDirectory(fullPath) {
  if (!fs.existsSync(fullPath)) {
    const error = new Error("Pasta não encontrada");
    error.statusCode = 404;
    throw error;
  }
  const stats = fs.lstatSync(fullPath);
  if (stats.isSymbolicLink()) {
    const error = new Error("Symlink não é permitido");
    error.statusCode = 403;
    throw error;
  }
  if (!stats.isDirectory()) {
    const error = new Error("Pasta não encontrada");
    error.statusCode = 404;
    throw error;
  }
  const realPath = fs.realpathSync(fullPath);
  if (!realPath.startsWith(getBaseResolved())) {
    const error = new Error("Path fora do diretório base");
    error.statusCode = 403;
    throw error;
  }
  return { realPath };
}

/** Dado path lógico (ex.: FISCAL/NFS), encontra date_rule no nó folha da árvore. */
function findDateRuleByPath(nodes, pathLogical) {
  const parts = pathLogical
    .split("/")
    .map((p) => p.trim())
    .filter(Boolean);
  if (parts.length === 0) return null;
  const byParentAndSlug = new Map();
  for (const n of nodes) {
    const slug = (n.slug || n.name || "").toLowerCase();
    const key = `${n.parent_id ?? "root"}:${slug}`;
    byParentAndSlug.set(key, n);
  }
  let parentId = null;
  let node = null;
  for (const part of parts) {
    const key = `${parentId ?? "root"}:${part.toLowerCase()}`;
    node = byParentAndSlug.get(key) ?? null;
    if (!node) return null;
    parentId = node.id;
  }
  return node?.date_rule ?? null;
}

app.disable("x-powered-by");

const allowedOrigins = (process.env.CORS_ORIGINS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const corsOptions = {
  origin: (origin, callback) => {
    // requests sem Origin (curl/health-check) são permitidas
    if (!origin) return callback(null, true);
    if (allowedOrigins.length === 0) return callback(null, false);
    if (allowedOrigins.includes(origin)) return callback(null, true);
    return callback(null, false);
  },
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: [
    "Content-Type",
    "Authorization",
    "X-Office-User-JWT",
    "ngrok-skip-browser-warning",
  ],
};
app.use(cors(corsOptions));

app.use(
  helmet({
    // API JSON — não aplicamos CSP aqui; isso é do frontend (Vercel).
    contentSecurityPolicy: false,
    crossOriginEmbedderPolicy: false,
  }),
);

app.use(
  rateLimit({
    windowMs: 60 * 1000,
    limit: 300,
    standardHeaders: true,
    legacyHeaders: false,
  }),
);

const heavyLimiter = rateLimit({
  windowMs: 60 * 1000,
  limit: 30,
  standardHeaders: true,
  legacyHeaders: false,
});

function requireConnectorSecret(req, res, next) {
  if (!CONNECTOR_SECRET_HASH) {
    return res
      .status(500)
      .json({ error: "Conector da VM não configurado com CONNECTOR_SECRET." });
  }
  const providedHash = String(req.headers.authorization || "")
    .replace(/^Bearer\s+/i, "")
    .trim();
  if (!providedHash || !safeTokenEqual(CONNECTOR_SECRET_HASH, providedHash)) {
    return res.status(401).json({ error: "Conector não autorizado" });
  }
  return next();
}

function requireForwardedUserJwt(req, res, next) {
  const token = getForwardedUserJwt(req);
  if (!token) {
    return res.status(401).json({ error: "JWT do usuário ausente." });
  }
  return next();
}

async function validateSupabaseJwt(req, res, next) {
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_ANON_KEY;
  if (!supabaseUrl || !supabaseKey) {
    return res.status(500).json({ error: "Supabase não configurado" });
  }
  const token = getForwardedUserJwt(req);
  try {
    const supabase = createClient(supabaseUrl, supabaseKey, {
      global: { headers: { Authorization: `Bearer ${token}` } },
      auth: {
        persistSession: false,
        autoRefreshToken: false,
        detectSessionInUrl: false,
      },
    });
    const { data, error } = await supabase.auth.getUser(token);
    if (error || !data?.user?.id) {
      return res.status(401).json({ error: "Token inválido" });
    }
    req.user = data.user;
    return next();
  } catch (e) {
    return res.status(401).json({ error: "Token inválido" });
  }
}

/** Garante que o JWT é de um usuário do mesmo office_id desta VM (CONNECTOR_SECRET). */
async function validateUserBelongsToConnectorOffice(req, res, next) {
  if (!OFFICE_ID) return next();
  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_ANON_KEY;
  const token = getForwardedUserJwt(req);
  if (!supabaseUrl || !supabaseKey || !token) {
    return res.status(500).json({ error: "Supabase não configurado" });
  }
  try {
    const supabase = createClient(supabaseUrl, supabaseKey, {
      global: { headers: { Authorization: `Bearer ${token}` } },
      auth: {
        persistSession: false,
        autoRefreshToken: false,
        detectSessionInUrl: false,
      },
    });
    const { data: m, error } = await supabase
      .from("office_memberships")
      .select("office_id")
      .eq("user_id", req.user.id)
      .order("is_default", { ascending: false })
      .limit(1)
      .maybeSingle();
    if (error) return res.status(500).json({ error: error.message });
    if (!m?.office_id || String(m.office_id) !== String(OFFICE_ID)) {
      return res.status(403).json({
        error:
          "Usuario nao pertence ao escritorio deste conector (WhatsApp isolado por escritorio).",
      });
    }
    return next();
  } catch (e) {
    return res.status(500).json({ error: "Falha ao validar escritorio" });
  }
}

// Não consumir o body nas rotas que o proxy repassa ao WhatsApp (senão o backend recebe body vazio e dá 408)
const whatsappPaths = [
  "/send",
  "/status",
  "/groups",
  "/qr",
  "/connect",
  "/disconnect",
];
app.use((req, res, next) => {
  const p = req.path || "/";
  const normalized = p.startsWith("/api/") ? p.slice(4) : p; // compat com túnel que expõe apenas /api/*
  const isWhatsApp =
    whatsappPaths.includes(normalized) ||
    normalized.startsWith("/qr") ||
    p.startsWith("/api/whatsapp");
  if (isWhatsApp) return next();
  const limit = process.env.BODY_LIMIT || "25mb";
  express.json({ limit })(req, res, (err) => {
    if (err) return next(err);
    express.urlencoded({ extended: true, limit })(req, res, next);
  });
});

// Header para ngrok não bloquear
app.use((req, res, next) => {
  res.setHeader("ngrok-skip-browser-warning", "true");
  next();
});

/**
 * GET /api/files/list?path=Grupo Fleury/NFS
 * Lista arquivos (XML, PDF) de uma pasta. Path é relativo a BASE_PATH.
 */
app.get(
  "/api/files/list",
  requireConnectorSecret,
  requireForwardedUserJwt,
  validateSupabaseJwt,
  (req, res) => {
    const relPath = req.query.path;
    try {
      const { resolved } = normalizeRelativePath(relPath);
      ensureSafeExistingDirectory(resolved);
      const entries = fs.readdirSync(resolved, { withFileTypes: true });
      const files = entries
        .filter((e) => e.isFile())
        .filter((e) => /\.(xml|pdf)$/i.test(e.name))
        .map((e) => ({
          name: e.name,
          ext: path.extname(e.name).toLowerCase(),
          path: path.join(String(relPath), e.name).replace(/\\/g, "/"),
        }));
      return res.json({ files });
    } catch (err) {
      const status = err.statusCode || (err.code === "ENOENT" ? 404 : 500);
      return res.status(status).json({ error: err.message });
    }
  },
);

/**
 * GET /api/files/download?path=Grupo Fleury/NFS/arquivo.xml
 * Baixa um arquivo por path direto (para testes, sem JWT).
 */
app.get(
  "/api/files/download",
  requireConnectorSecret,
  requireForwardedUserJwt,
  validateSupabaseJwt,
  (req, res) => {
    const inputPath = req.query.path;
    try {
      const { resolved } = normalizeRelativePath(inputPath);
      const { realPath } = ensureSafeExistingFile(resolved);
      const filename = path.basename(realPath);
      const ext = path.extname(filename).toLowerCase();
      const contentType =
        ext === ".xml"
          ? "application/xml"
          : ext === ".pdf"
            ? "application/pdf"
            : "application/octet-stream";
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="${filename}"`,
      );
      res.setHeader("Content-Type", contentType);
      fs.createReadStream(realPath).pipe(res);
    } catch (err) {
      return res.status(err.statusCode || 500).json({ error: err.message });
    }
  },
);

app.post(
  "/api/declarations/artifacts/list",
  requireConnectorSecret,
  requireForwardedUserJwt,
  validateSupabaseJwt,
  heavyLimiter,
  async (req, res) => {
    if (!OFFICE_ID || !OFFICE_SERVER_ID) {
      return res.status(503).json({
        error:
          "Conector da VM sem office_id vinculado. Revise CONNECTOR_SECRET e office_server_credentials.",
      });
    }

    const supabase = createSupabaseServiceClient();
    if (!supabase) {
      return res.status(500).json({ error: "Supabase nao configurado" });
    }

    try {
      const body = req.body && typeof req.body === "object" ? req.body : {};
      const response = await listDeclarationArtifacts({
        supabase,
        officeId: OFFICE_ID,
        officeServerId: OFFICE_SERVER_ID,
        basePath: BASE_PATH,
        action: body.action,
        companyIds: body.company_ids,
        competence: body.competence,
        limit: body.limit,
      });
      return res.json(response);
    } catch (err) {
      return res.status(err.statusCode || 500).json({ error: err.message });
    }
  },
);

app.post(
  "/api/declarations/artifacts/download",
  requireConnectorSecret,
  requireForwardedUserJwt,
  validateSupabaseJwt,
  heavyLimiter,
  async (req, res) => {
    if (!OFFICE_ID || !OFFICE_SERVER_ID) {
      return res.status(503).json({
        error:
          "Conector da VM sem office_id vinculado. Revise CONNECTOR_SECRET e office_server_credentials.",
      });
    }

    const supabase = createSupabaseServiceClient();
    if (!supabase) {
      return res.status(500).json({ error: "Supabase nao configurado" });
    }

    try {
      const body = req.body && typeof req.body === "object" ? req.body : {};
      const result = await resolveDeclarationArtifactDownload({
        supabase,
        officeId: OFFICE_ID,
        officeServerId: OFFICE_SERVER_ID,
        basePath: BASE_PATH,
        action: body.action,
        companyId: body.company_id,
        competence: body.competence,
        artifactKey: body.artifact_key,
      });

      const ext = path.extname(result.fileName).toLowerCase();
      const contentType =
        ext === ".xml"
          ? "application/xml"
          : ext === ".pdf"
            ? "application/pdf"
            : ext === ".zip"
              ? "application/zip"
              : ext === ".csv"
                ? "text/csv"
                : "application/octet-stream";
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="${result.fileName}"`,
      );
      res.setHeader("Content-Type", contentType);
      fs.createReadStream(result.fullPath).pipe(res);
    } catch (err) {
      return res.status(err.statusCode || 500).json({ error: err.message });
    }
  },
);

app.post(
  "/api/robots/runtime/stop",
  requireConnectorSecret,
  requireForwardedUserJwt,
  validateSupabaseJwt,
  heavyLimiter,
  async (req, res) => {
    if (!OFFICE_ID || !OFFICE_SERVER_ID) {
      return res.status(503).json({
        error:
          "Conector da VM sem office_id vinculado. Revise CONNECTOR_SECRET e office_server_credentials.",
      });
    }

    const supabase = createSupabaseServiceClient();
    if (!supabase) {
      return res.status(500).json({ error: "Supabase nao configurado" });
    }

    try {
      const body = req.body && typeof req.body === "object" ? req.body : {};
      const requestedTechnicalIds = [
        ...new Set(
          (Array.isArray(body.robot_technical_ids) ? body.robot_technical_ids : [])
            .map((value) => String(value ?? "").trim())
            .filter(Boolean),
        ),
      ];
      if (requestedTechnicalIds.length === 0) {
        return res.status(400).json({ error: "robot_technical_ids e obrigatorio." });
      }

      const reason =
        String(body.reason ?? "").trim() ||
        "Solicitacao cancelada ao limpar o historico no SaaS.";

      const runtimeRows = await listRobotRuntimeRows(supabase, OFFICE_ID, OFFICE_SERVER_ID);
      const runtimeByTechnicalId = new Map(
        runtimeRows.map((row) => [String(row.technical_id || "").trim(), row]),
      );

      const results = [];
      for (const technicalId of requestedTechnicalIds) {
        const robotRow = runtimeByTechnicalId.get(technicalId);
        if (!robotRow) {
          results.push({
            robot_technical_id: technicalId,
            stopped: false,
            reason: "Robo nao encontrado para este escritorio.",
          });
          continue;
        }

        const runtimePaths = buildRuntimePaths(ROBOTS_ROOT_PATH, robotRow);
        if (!runtimePaths || !fs.existsSync(runtimePaths.runtimeRoot)) {
          results.push({
            robot_technical_id: technicalId,
            stopped: false,
            reason: "Pasta de runtime nao encontrada na VM.",
          });
          continue;
        }

        writeJsonAtomic(runtimePaths.stopFilePath, {
          robot_technical_id: technicalId,
          requested_at: new Date().toISOString(),
          reason,
          source: "saas_clear_history",
        });
        removeFileIfExists(runtimePaths.jobFilePath);

        results.push({
          robot_technical_id: technicalId,
          stopped: true,
          runtime_root: runtimePaths.runtimeRoot,
        });
      }

      return res.json({
        ok: true,
        results,
      });
    } catch (err) {
      return res.status(err.statusCode || 500).json({ error: err.message });
    }
  },
);

const MAX_FILES_ZIP_BY_PATHS = 50000;

/**
 * Handler: POST /api/documents/download-zip-by-paths (e /documents/download-zip-by-paths para compat com túnel).
 * Gera um ZIP com os arquivos indicados (paths relativos ao BASE_PATH).
 * Body: { items: [{ file_path, company_name?, category? }], filename_suffix?: string }
 */
const handleDownloadZipByPaths = [
  requireConnectorSecret,
  requireForwardedUserJwt,
  validateSupabaseJwt,
  heavyLimiter,
  async (req, res) => {
    console.log(
      "[server-api] POST download-zip-by-paths recebido, path:",
      req.path || req.url,
    );
    const rawItems = Array.isArray(req.body?.items) ? req.body.items : [];
    if (rawItems.length === 0) {
      return res
        .status(400)
        .json({ error: "Nenhum arquivo informado para o ZIP." });
    }
    if (rawItems.length > MAX_FILES_ZIP_BY_PATHS) {
      return res.status(400).json({
        error: `Limite de ${MAX_FILES_ZIP_BY_PATHS} arquivos por download. Selecione menos itens.`,
      });
    }
    const baseResolved = path.resolve(BASE_PATH);
    const toAdd = [];
    const usedNames = new Set();
    const makeUniqueName = (zipPath) => {
      let n = zipPath;
      let i = 0;
      while (usedNames.has(n)) {
        i++;
        const ext = path.posix.extname(zipPath);
        const base = zipPath.slice(0, zipPath.length - ext.length);
        n = `${base} (${i})${ext}`;
      }
      usedNames.add(n);
      return n;
    };
    const safeFolder = (s) =>
      String(s || "")
        .replace(/[<>:"/\\|?*\u0000-\u001F]/g, "")
        .replace(/\s+/g, " ")
        .trim() || "outros";
    for (const it of rawItems) {
      const filePath =
        typeof it?.file_path === "string" ? it.file_path.trim() : "";
      if (!filePath) continue;
      try {
        const { resolved } = normalizeRelativePath(filePath);
        const { realPath } = ensureSafeExistingFile(resolved);
        const companyName = safeFolder(it.company_name || "EMPRESA");
        const category = safeFolder(it.category || "outros");
        const filename = path.basename(realPath);
        const zipPath = `${companyName}/${category}/${filename}`;
        toAdd.push({
          fullPath: realPath,
          zipPath,
          nameInZip: makeUniqueName(zipPath),
        });
      } catch (_) {
        /* ignora arquivo inexistente ou path inválido */
      }
    }
    if (toAdd.length === 0) {
      return res.status(404).json({
        error:
          "Nenhum arquivo válido encontrado no disco para a lista informada.",
      });
    }
    res.setHeader("Content-Type", "application/zip");
    const suffix =
      typeof req.body?.filename_suffix === "string"
        ? req.body.filename_suffix.trim()
        : "";
    const safeSuffix =
      suffix && /^[a-z0-9-]+$/i.test(suffix) ? `-${suffix}` : "";
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="documentos${safeSuffix}.zip"`,
    );
    const archive = archiver("zip", { zlib: { level: 0 } });
    archive.on("error", (err) => {
      if (!res.headersSent) res.status(500).json({ error: err.message });
    });
    res.on("close", () => {
      try {
        archive.abort();
      } catch (_) {}
    });
    archive.pipe(res);
    const BATCH = 200;
    for (let i = 0; i < toAdd.length; i += BATCH) {
      const batch = toAdd.slice(i, i + BATCH);
      const buffers = await Promise.all(
        batch.map(({ fullPath }) =>
          fs.promises.readFile(fullPath).catch(() => null),
        ),
      );
      for (let j = 0; j < batch.length; j++) {
        if (buffers[j])
          archive.append(buffers[j], { name: batch[j].nameInZip });
      }
    }
    archive.finalize();
  },
];
app.post("/api/documents/download-zip-by-paths", ...handleDownloadZipByPaths);
app.post("/documents/download-zip-by-paths", ...handleDownloadZipByPaths);
/* Se o public_base_url do escritório terminar com /api, a Edge Function chama base_url + "/api/documents/...", resultando em /api/api/documents/... */
app.post(
  "/api/api/documents/download-zip-by-paths",
  ...handleDownloadZipByPaths,
);
/* Fallback: qualquer path que contenha download-zip-by-paths (ex.: proxy que reescreve a URL) */
app.post(/\/.*download-zip-by-paths.*/, ...handleDownloadZipByPaths);

/**
 * GET /api/fiscal-documents/:id/download
 * Baixa arquivo fiscal por ID (busca file_path no Supabase). Requer JWT.
 */
app.get(
  "/api/fiscal-documents/:id/download",
  requireConnectorSecret,
  requireForwardedUserJwt,
  async (req, res) => {
    const token = getForwardedUserJwt(req);
    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_ANON_KEY;
    if (!supabaseUrl || !supabaseKey) {
      return res.status(500).json({ error: "Supabase não configurado" });
    }
    const supabase = createClient(supabaseUrl, supabaseKey, {
      global: { headers: { Authorization: `Bearer ${token}` } },
    });
    const { data: doc, error } = await supabase
      .from("fiscal_documents")
      .select("file_path")
      .eq("id", req.params.id)
      .single();
    if (error || !doc?.file_path) {
      return res.status(404).json({ error: "Documento não encontrado" });
    }
    try {
      const { resolved } = normalizeRelativePath(doc.file_path);
      const { realPath } = ensureSafeExistingFile(resolved);
      const filename = path.basename(realPath);
      const ext = path.extname(filename).toLowerCase();
      const contentType =
        ext === ".xml"
          ? "application/xml"
          : ext === ".pdf"
            ? "application/pdf"
            : "application/octet-stream";
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="${filename}"`,
      );
      res.setHeader("Content-Type", contentType);
      fs.createReadStream(realPath).pipe(res);
    } catch (err) {
      return res.status(err.statusCode || 500).json({ error: err.message });
    }
  },
);

/**
 * POST /api/fiscal-documents/download-zip
 * Cria um ZIP temporário na VM com os arquivos dos documentos solicitados (mesma lista/filtro da tela),
 * envia o ZIP na resposta e apaga o arquivo temporário em seguida.
 * Body: { ids: string[] }. Requer JWT.
 */
app.post(
  "/api/fiscal-documents/download-zip",
  requireConnectorSecret,
  requireForwardedUserJwt,
  validateSupabaseJwt,
  heavyLimiter,
  async (req, res) => {
    const token = getForwardedUserJwt(req);
    const MAX_DOCS_PER_ZIP = 50000;
    const ids = Array.isArray(req.body?.ids)
      ? req.body.ids.filter((id) => id && String(id).trim())
      : [];
    const companyIds = Array.isArray(req.body?.company_ids)
      ? req.body.company_ids.filter((id) => id && String(id).trim())
      : [];
    const types = Array.isArray(req.body?.types)
      ? req.body.types
          .map((t) =>
            String(t || "")
              .trim()
              .toUpperCase(),
          )
          .filter(Boolean)
      : [];
    if (companyIds.length === 0 && ids.length === 0) {
      return res
        .status(400)
        .json({ error: "Nenhum documento/empresa selecionado para baixar." });
    }
    if (ids.length > MAX_DOCS_PER_ZIP) {
      return res.status(400).json({
        error: `Limite de ${MAX_DOCS_PER_ZIP} documentos por download. Selecione menos itens na lista.`,
      });
    }
    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_ANON_KEY;
    if (!supabaseUrl || !supabaseKey) {
      return res.status(500).json({ error: "Supabase não configurado" });
    }
    const supabase = createClient(supabaseUrl, supabaseKey, {
      global: { headers: { Authorization: `Bearer ${token}` } },
    });
    // Não depende de relacionamento (FK) fiscal_documents -> companies (pode não existir no schema cache).
    let q = supabase
      .from("fiscal_documents")
      .select("id, file_path, company_id");
    if (companyIds.length > 0) q = q.in("company_id", companyIds);
    else q = q.in("id", ids);
    if (types.length > 0) q = q.in("type", types);
    const { data: rows, error } = await q;
    if (error) {
      return res.status(500).json({ error: error.message });
    }
    let docs = (rows || []).filter(
      (r) => r?.file_path && String(r.file_path).trim(),
    );
    if (docs.length > MAX_DOCS_PER_ZIP) {
      docs = docs.slice(0, MAX_DOCS_PER_ZIP);
    }
    const companyIdsInDocs = [
      ...new Set(docs.map((d) => d.company_id).filter(Boolean)),
    ];
    const companyNameById = new Map();
    if (companyIdsInDocs.length > 0) {
      try {
        const { data: companies, error: companiesErr } = await supabase
          .from("companies")
          .select("id, name")
          .in("id", companyIdsInDocs);
        if (!companiesErr && Array.isArray(companies)) {
          for (const c of companies) {
            if (c?.id) companyNameById.set(c.id, String(c.name || "").trim());
          }
        }
      } catch (_) {}
    }
    const baseResolved = path.resolve(BASE_PATH);
    const toAdd = [];
    for (const doc of docs) {
      const fullPath = path.join(BASE_PATH, doc.file_path);
      if (!path.resolve(fullPath).startsWith(baseResolved)) continue;
      if (!fs.existsSync(fullPath) || !fs.statSync(fullPath).isFile()) continue;
      const filePathNormalized = String(doc.file_path || "").replace(
        /\\/g,
        "/",
      );
      const parts = filePathNormalized.split("/").filter(Boolean);
      let companyFolder = "EMPRESA";
      let restParts = parts;
      if (parts.length >= 2 && parts[0].toLowerCase() === "empresas") {
        companyFolder = parts[1];
        restParts = parts.slice(2);
      } else if (parts.length >= 1) {
        companyFolder = parts[0];
        restParts = parts.slice(1);
      }
      const companyNameFromDb = companyNameById.get(doc.company_id) || "";
      if (String(companyNameFromDb || "").trim())
        companyFolder = String(companyNameFromDb).trim();
      const safeCompany =
        companyFolder
          .replace(/[<>:"/\\|?*\u0000-\u001F]/g, "")
          .replace(/\s+/g, " ")
          .trim() || "EMPRESA";
      // Dentro da pasta da empresa, separar por tipo (igual ao seletor do app).
      const typeUpper = String(doc.type || "")
        .trim()
        .toUpperCase();
      const categoryFolder =
        typeUpper === "NFS"
          ? "nfs"
          : typeUpper === "NFE" || typeUpper === "NFC"
            ? "nfe-nfc"
            : "fiscal";
      const zipFilename = path.basename(filePathNormalized);
      const zipPath = `${safeCompany}/${categoryFolder}/${zipFilename}`;
      toAdd.push({ fullPath, zipPath });
    }
    if (toAdd.length === 0) {
      return res.status(404).json({
        error:
          "Nenhum arquivo encontrado no disco para os documentos solicitados.",
      });
    }
    const usedNames = new Set();
    const makeUniqueName = (zipPath) => {
      let n = zipPath;
      let i = 0;
      while (usedNames.has(n)) {
        i++;
        const ext = path.posix.extname(zipPath);
        const base = zipPath.slice(0, zipPath.length - ext.length);
        n = `${base} (${i})${ext}`;
      }
      usedNames.add(n);
      return n;
    };
    res.setHeader("Content-Type", "application/zip");
    const suffix =
      typeof req.body?.filename_suffix === "string"
        ? req.body.filename_suffix.trim()
        : "";
    const safeSuffix =
      suffix && /^[a-z0-9-]+$/i.test(suffix) ? `-${suffix}` : "";
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="documentos-fiscais${safeSuffix}.zip"`,
    );
    // STORE (level 0): mais rápido, menor CPU. Mantém downloads rápidos.
    const archive = archiver("zip", { zlib: { level: 0 } });
    archive.on("error", (err) => {
      if (!res.headersSent) res.status(500).json({ error: err.message });
    });
    // Se o cliente (ngrok/browser) abortar, interrompe o zip para não “ficar pendurado” até o final.
    res.on("close", () => {
      try {
        archive.abort();
      } catch (_) {}
    });
    archive.pipe(res);
    const toAddWithNamesFiscal = toAdd.map((e) => ({
      ...e,
      nameInZip: makeUniqueName(e.zipPath),
    }));
    const BATCH_FISCAL = 200;
    for (let i = 0; i < toAddWithNamesFiscal.length; i += BATCH_FISCAL) {
      const batch = toAddWithNamesFiscal.slice(i, i + BATCH_FISCAL);
      const buffers = await Promise.all(
        batch.map(({ fullPath }) =>
          fs.promises.readFile(fullPath).catch(() => null),
        ),
      );
      for (let j = 0; j < batch.length; j++) {
        if (buffers[j])
          archive.append(buffers[j], { name: batch[j].nameInZip });
      }
    }
    archive.finalize();
  },
);

/**
 * POST /api/hub-documents/download-zip
 * Baixa um ZIP unificado com TODOS os documentos do hub (certidões, notas, guias/taxas/impostos),
 * organizando por Empresa/<categoria>/<arquivo>.
 * Body: { company_ids: string[], categories?: string[], filename_suffix?: string }
 */
app.post(
  "/api/hub-documents/download-zip",
  requireConnectorSecret,
  requireForwardedUserJwt,
  validateSupabaseJwt,
  heavyLimiter,
  async (req, res) => {
    const token = getForwardedUserJwt(req);
    const companyIds = Array.isArray(req.body?.company_ids)
      ? req.body.company_ids.filter((id) => id && String(id).trim())
      : [];
    if (companyIds.length === 0) {
      return res
        .status(400)
        .json({ error: "Nenhuma empresa selecionada para baixar." });
    }
    const categoriesRequested = Array.isArray(req.body?.categories)
      ? req.body.categories
          .map((c) =>
            String(c || "")
              .trim()
              .toLowerCase(),
          )
          .filter(Boolean)
      : [];
    const allowAll = categoriesRequested.length === 0;
    const allow = (key) =>
      allowAll || categoriesRequested.includes(String(key || "").toLowerCase());

    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_ANON_KEY;
    if (!supabaseUrl || !supabaseKey) {
      return res.status(500).json({ error: "Supabase não configurado" });
    }
    const supabase = createClient(supabaseUrl, supabaseKey, {
      global: { headers: { Authorization: `Bearer ${token}` } },
    });

    // Nome da empresa para pasta
    const companyNameById = new Map();
    try {
      const { data: companies } = await supabase
        .from("companies")
        .select("id, name")
        .in("id", companyIds);
      for (const c of companies || []) {
        if (c?.id) companyNameById.set(c.id, String(c.name || "").trim());
      }
    } catch (_) {}

    const baseResolved = path.resolve(BASE_PATH);
    const toAdd = [];

    const safeCompanyFolder = (companyId, fallbackFromPath) => {
      let companyFolder =
        String(companyNameById.get(companyId) || "").trim() ||
        String(fallbackFromPath || "EMPRESA");
      return (
        companyFolder
          .replace(/[<>:"/\\|?*\u0000-\u001F]/g, "")
          .replace(/\s+/g, " ")
          .trim() || "EMPRESA"
      );
    };

    const addFile = (companyId, categoryFolder, fileRelPath) => {
      const filePathNormalized = String(fileRelPath || "")
        .replace(/\\/g, "/")
        .trim();
      if (!filePathNormalized) return;
      const fullPath = path.join(BASE_PATH, filePathNormalized);
      if (!path.resolve(fullPath).startsWith(baseResolved)) return;
      if (!fs.existsSync(fullPath) || !fs.statSync(fullPath).isFile()) return;
      const parts = filePathNormalized.split("/").filter(Boolean);
      let fallbackCompany = "EMPRESA";
      if (parts.length >= 2 && parts[0].toLowerCase() === "empresas")
        fallbackCompany = parts[1];
      else if (parts.length >= 1) fallbackCompany = parts[0];
      const safeCompany = safeCompanyFolder(companyId, fallbackCompany);
      const zipFilename = path.basename(filePathNormalized);
      const safeCategory =
        String(categoryFolder || "outros")
          .replace(/[<>:"/\\|?*\u0000-\u001F]/g, "")
          .replace(/\s+/g, " ")
          .trim() || "outros";
      const zipPath = `${safeCompany}/${safeCategory}/${zipFilename}`;
      toAdd.push({ fullPath, zipPath });
    };

    // Notas (fiscal_documents)
    if (allow("nfs") || allow("nfe-nfc") || allow("fiscal")) {
      const { data: fiscalRows } = await supabase
        .from("fiscal_documents")
        .select("company_id, type, file_path")
        .in("company_id", companyIds)
        .not("file_path", "is", null);
      for (const r of fiscalRows || []) {
        const typeUpper = String(r.type || "")
          .trim()
          .toUpperCase();
        const cat =
          typeUpper === "NFS"
            ? "nfs"
            : typeUpper === "NFE" || typeUpper === "NFC"
              ? "nfe-nfc"
              : "fiscal";
        if (!allow(cat) && !allow("fiscal")) continue;
        addFile(r.company_id, cat, r.file_path);
      }
    }

    // Certidões (sync_events payload.arquivo_pdf)
    if (allow("certidoes") || allow("certidões")) {
      const { data: syncRows } = await supabase
        .from("sync_events")
        .select("company_id, payload, created_at")
        .eq("tipo", "certidao_resultado")
        .in("company_id", companyIds)
        .order("created_at", { ascending: false })
        .limit(5000);
      for (const r of syncRows || []) {
        let payload = {};
        try {
          payload = JSON.parse(r.payload || "{}");
        } catch {
          payload = {};
        }
        const p = String(payload.arquivo_pdf || "").trim();
        if (!p) continue;
        addFile(r.company_id, "certidoes", p);
      }
    }

    // Guias / taxas (dp_guias.file_path)
    if (
      allow("taxas e impostos") ||
      allow("taxas_impostos") ||
      allow("taxas") ||
      allow("impostos")
    ) {
      const { data: guiaRows } = await supabase
        .from("dp_guias")
        .select("company_id, file_path")
        .in("company_id", companyIds)
        .not("file_path", "is", null);
      for (const r of guiaRows || []) {
        addFile(r.company_id, "taxas e impostos", r.file_path);
      }
    }

    // Impostos municipais (municipal_tax_debts.guia_pdf_path)
    if (
      allow("taxas e impostos") ||
      allow("taxas_impostos") ||
      allow("taxas") ||
      allow("impostos")
    ) {
      const { data: muniRows } = await supabase
        .from("municipal_tax_debts")
        .select("company_id, guia_pdf_path")
        .in("company_id", companyIds)
        .not("guia_pdf_path", "is", null);
      for (const r of muniRows || []) {
        addFile(r.company_id, "taxas e impostos", r.guia_pdf_path);
      }
    }

    if (toAdd.length === 0) {
      return res.status(404).json({
        error:
          "Nenhum arquivo encontrado no disco para as empresas solicitadas.",
      });
    }
    const MAX_FILES_HUB_ZIP = 50000;
    if (toAdd.length > MAX_FILES_HUB_ZIP) {
      toAdd.length = MAX_FILES_HUB_ZIP;
    }

    const usedNamesHub = new Set();
    const makeUniqueNameHub = (zipPath) => {
      let n = zipPath;
      let i = 0;
      while (usedNamesHub.has(n)) {
        i++;
        const ext = path.posix.extname(zipPath);
        const base = zipPath.slice(0, zipPath.length - ext.length);
        n = `${base} (${i})${ext}`;
      }
      usedNamesHub.add(n);
      return n;
    };
    const toAddWithNamesHub = toAdd.map((e) => ({
      ...e,
      nameInZip: makeUniqueNameHub(e.zipPath),
    }));

    res.setHeader("Content-Type", "application/zip");
    const suffix =
      typeof req.body?.filename_suffix === "string"
        ? req.body.filename_suffix.trim()
        : "";
    const safeSuffix =
      suffix && /^[a-z0-9-]+$/i.test(suffix) ? `-${suffix}` : "";
    res.setHeader(
      "Content-Disposition",
      `attachment; filename="documentos-hub${safeSuffix}.zip"`,
    );
    const archive = archiver("zip", { zlib: { level: 0 } });
    archive.on("error", (err) => {
      if (!res.headersSent) res.status(500).json({ error: err.message });
    });
    res.on("close", () => {
      try {
        archive.abort();
      } catch (_) {}
    });
    archive.pipe(res);
    const BATCH_HUB = 200;
    for (let i = 0; i < toAddWithNamesHub.length; i += BATCH_HUB) {
      const batch = toAddWithNamesHub.slice(i, i + BATCH_HUB);
      const buffers = await Promise.all(
        batch.map(({ fullPath }) =>
          fs.promises.readFile(fullPath).catch(() => null),
        ),
      );
      for (let j = 0; j < batch.length; j++) {
        if (buffers[j])
          archive.append(buffers[j], { name: batch[j].nameInZip });
      }
    }
    archive.finalize();
  },
);

/**
 * POST /api/fiscal-sync
 * Sincroniza arquivos de uma pasta para fiscal_documents.
 * Body: { path, company_id, type }
 * Requer Authorization: Bearer <jwt_do_usuario> — usa só anon key; RLS valida permissão.
 */
app.post(
  "/api/fiscal-sync",
  requireConnectorSecret,
  requireForwardedUserJwt,
  validateSupabaseJwt,
  heavyLimiter,
  async (req, res) => {
    const { path: relPath, company_id, type = "NFS" } = req.body || {};
    if (!relPath || !company_id) {
      return res
        .status(400)
        .json({ error: "path e company_id são obrigatórios" });
    }
    const token = getForwardedUserJwt(req);
    const fullPath = path.join(BASE_PATH, relPath);
    if (!path.resolve(fullPath).startsWith(path.resolve(BASE_PATH))) {
      return res.status(403).json({ error: "Path fora do diretório base" });
    }
    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_ANON_KEY;
    if (!supabaseUrl || !supabaseKey) {
      return res.status(500).json({
        error: "Supabase não configurado (SUPABASE_URL e SUPABASE_ANON_KEY)",
      });
    }
    const supabase = createClient(supabaseUrl, supabaseKey, {
      global: { headers: { Authorization: `Bearer ${token}` } },
    });
    try {
      const entries = fs.readdirSync(fullPath, { withFileTypes: true });
      const files = entries
        .filter((e) => e.isFile())
        .filter((e) => /\.(xml|pdf)$/i.test(e.name));
      const periodo = new Date().toISOString().slice(0, 7);
      const inserted = [];
      const errors = [];
      for (const f of files) {
        const fileRelPath = path.join(relPath, f.name).replace(/\\/g, "/");
        const chave = path.basename(f.name, path.extname(f.name));
        const { data: existingRows } = await supabase
          .from("fiscal_documents")
          .select("id")
          .eq("company_id", company_id)
          .eq("file_path", fileRelPath)
          .limit(1);
        if (existingRows && existingRows.length > 0) continue;
        const { data, error } = await supabase
          .from("fiscal_documents")
          .insert({
            company_id,
            type: type.toUpperCase(),
            chave,
            periodo,
            status: "novo",
            file_path: fileRelPath,
          })
          .select("id");
        const row = Array.isArray(data) ? data[0] : data;
        if (!error && row?.id) inserted.push({ id: row.id, name: f.name });
        else if (error?.code === "23505") {
          /* duplicata, ignorar */
        } else if (error) errors.push({ name: f.name, error: error.message });
      }
      return res.json({
        found: files.length,
        inserted: inserted.length,
        files: inserted,
        errors: errors.length ? errors : undefined,
      });
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  },
);

/**
 * POST /api/fiscal-sync-all
 * Escaneia BASE_PATH, associa cada pasta de empresa ao company_id pelo nome da empresa,
 * e sincroniza arquivos XML/PDF de FISCAL/NFS/Recebidas e FISCAL/NFS/Emitidas para fiscal_documents.
 * Requer Authorization: Bearer <jwt>.
 * Normaliza nomes (remove acentos/cedilha) para casar pasta "SERVICOS" com empresa "SERVIÇOS".
 */
function normalizeCompanyName(name) {
  if (typeof name !== "string") return "";
  return name
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "");
}

function walkDir(dir, baseDir) {
  const results = [];
  const fullDir = path.join(baseDir, dir);
  if (!fs.existsSync(fullDir) || !fs.statSync(fullDir).isDirectory())
    return results;
  const entries = fs.readdirSync(fullDir, { withFileTypes: true });
  for (const e of entries) {
    const rel = path.join(dir, e.name).replace(/\\/g, "/");
    if (e.isDirectory()) {
      results.push(...walkDir(rel, baseDir));
    } else if (e.isFile() && /\.(xml|pdf)$/i.test(e.name)) {
      results.push(rel);
    }
  }
  return results;
}

function parseJsonLike(value) {
  if (!value) return {};
  if (typeof value === "object") return value;
  if (typeof value !== "string") return {};
  try {
    return JSON.parse(value);
  } catch {
    return {};
  }
}

async function fetchAllRows(queryBuilderFactory, pageSize = 1000) {
  const rows = [];
  let from = 0;

  while (true) {
    const to = from + pageSize - 1;
    const { data, error } = await queryBuilderFactory().range(from, to);
    if (error) throw error;

    const chunk = Array.isArray(data) ? data : [];
    rows.push(...chunk);

    if (chunk.length < pageSize) break;
    from += pageSize;
  }

  return rows;
}

function resolveStoredFilePathInfo(filePath) {
  const raw = String(filePath || "").trim();
  if (!raw) {
    return {
      raw,
      normalized: "",
      fullPath: "",
      relativePath: "",
      exists: false,
      insideBase: false,
    };
  }

  const baseResolved = path.resolve(BASE_PATH);
  let fullPath = "";
  let relativePath = "";
  let insideBase = false;

  try {
    if (path.isAbsolute(raw)) {
      fullPath = path.resolve(raw);
      insideBase = fullPath.startsWith(baseResolved);
      if (insideBase) {
        relativePath = path.relative(baseResolved, fullPath).replace(/\\/g, "/");
      }
    } else {
      const normalized = raw.replace(/\//g, path.sep);
      fullPath = path.resolve(path.join(BASE_PATH, normalized));
      insideBase = fullPath.startsWith(baseResolved);
      if (insideBase) {
        relativePath = path.relative(baseResolved, fullPath).replace(/\\/g, "/");
      }
    }
  } catch {
    fullPath = "";
    relativePath = "";
    insideBase = false;
  }

  const exists = fullPath ? fs.existsSync(fullPath) : false;
  return {
    raw,
    normalized: raw.replace(/\\/g, "/"),
    fullPath,
    relativePath,
    exists,
    insideBase,
  };
}

async function clearMissingFileReferences(supabase, effectiveOfficeId, result, allPathsOnDisk) {
  const fileMissingFromBase = (filePath) => {
    const info = resolveStoredFilePathInfo(filePath);
    if (!info.raw) return false;
    if (!info.insideBase) return false;
    return !info.exists;
  };

  // Certidões: o índice usa o evento mais recente. Então, quando o PDF some do disco,
  // sobrescrevemos o mesmo idempotency_key removendo apenas o arquivo_pdf do payload.
  const certRows = await fetchAllRows(() =>
    supabase
      .from("sync_events")
      .select("id, office_id, company_id, payload, status, idempotency_key")
      .eq("office_id", effectiveOfficeId)
      .eq("tipo", "certidao_resultado"),
  );

  for (const row of certRows || []) {
    const payload = parseJsonLike(row.payload);
    const filePath = String(payload.arquivo_pdf || "").trim();
    if (!filePath || !fileMissingFromBase(filePath)) continue;

    const nextPayload = { ...payload };
    delete nextPayload.arquivo_pdf;
    nextPayload.file_removed = true;
    nextPayload.file_removed_at = new Date().toISOString();

    const idemKey =
      String(row.idempotency_key || "").trim() ||
      `${row.company_id || ""}:${payload.tipo_certidao || ""}:${payload.document_date || payload.data_consulta || row.id}`;

    try {
      await supabase
        .from("sync_events")
        .delete()
        .eq("office_id", effectiveOfficeId)
        .eq("tipo", "certidao_resultado")
        .eq("idempotency_key", idemKey);

      const { error: insertError } = await supabase.from("sync_events").insert({
        office_id: String(row.office_id || effectiveOfficeId),
        company_id: row.company_id,
        tipo: "certidao_resultado",
        payload: JSON.stringify(nextPayload),
        status: row.status || "sucesso",
        idempotency_key: idemKey,
      });
      if (insertError) {
        result.errors.push({
          file: filePath,
          error: `sync_events(certidoes): ${insertError.message}`,
        });
      } else {
        result.deleted++;
      }
    } catch (error) {
      result.errors.push({
        file: filePath,
        error: `sync_events(certidoes): ${error.message}`,
      });
    }
  }

  const clearColumnWhenMissing = async ({
    table,
    column,
  }) => {
    const rows = await fetchAllRows(() =>
      supabase
        .from(table)
        .select(`id, ${column}`)
        .eq("office_id", effectiveOfficeId)
        .not(column, "is", null),
    );

    const idsToClear = (rows || [])
      .filter((row) => fileMissingFromBase(row[column]))
      .map((row) => row.id);

    if (idsToClear.length === 0) return;

    const { error } = await supabase
      .from(table)
      .update({ [column]: null })
      .in("id", idsToClear);

    if (error) {
      result.errors.push({
        file: `${table}.${column}`,
        error: error.message,
      });
      return;
    }

    result.deleted += idsToClear.length;
  };

  await clearColumnWhenMissing({ table: "dp_guias", column: "file_path" });
  await clearColumnWhenMissing({
    table: "municipal_tax_debts",
    column: "guia_pdf_path",
  });
}

async function clearMissingNfsStats(supabase, effectiveOfficeId, result) {
  const activeFiscalRows = await fetchAllRows(() =>
    supabase
      .from("fiscal_documents")
      .select("company_id, periodo, file_path")
      .eq("office_id", effectiveOfficeId)
      .eq("type", "NFS")
      .not("file_path", "is", null),
  );

  const activePairs = new Set(
    (activeFiscalRows || [])
      .filter((row) => Boolean(String(row.file_path || "").trim()))
      .map((row) => {
        const companyId = String(row.company_id || "").trim();
        const period = String(row.periodo || "").trim().slice(0, 7);
        return companyId && /^\d{4}-\d{2}$/.test(period)
          ? `${companyId}:${period}`
          : "";
      })
      .filter(Boolean),
  );

  const nfsStatsRows = await fetchAllRows(() =>
    supabase
      .from("nfs_stats")
      .select("id, company_id, period")
      .eq("office_id", effectiveOfficeId),
  );

  const idsToDelete = (nfsStatsRows || [])
    .filter((row) => {
      const companyId = String(row.company_id || "").trim();
      const period = String(row.period || "").trim().slice(0, 7);
      if (!companyId || !/^\d{4}-\d{2}$/.test(period)) return true;
      return !activePairs.has(`${companyId}:${period}`);
    })
    .map((row) => row.id);

  if (idsToDelete.length === 0) return;

  for (let index = 0; index < idsToDelete.length; index += 500) {
    const batch = idsToDelete.slice(index, index + 500);
    const { error } = await supabase.from("nfs_stats").delete().in("id", batch);
    if (error) {
      result.errors.push({
        file: "nfs_stats",
        error: error.message,
      });
      continue;
    }
    result.deleted += batch.length;
  }
}

/**
 * Executa a sincronização completa EMPRESAS -> fiscal_documents.
 * Inclui remoção: registros cujo arquivo não existe mais na pasta são removidos do banco.
 * @param {import('@supabase/supabase-js').SupabaseClient} supabase - Cliente Supabase (JWT do usuário ou service role)
 * @param {string | null} [officeId] - Se informado (ex.: watcher), usa nos inserts; senão usa current_office_id() do JWT.
 * @returns {{ inserted: number, skipped: number, deleted: number, errors: Array<{ file: string, error: string }> }}
 */
async function runFiscalSyncAll(supabase, officeId = null) {
  const result = { inserted: 0, skipped: 0, deleted: 0, errors: [] };
  const effectiveOfficeId = officeId || OFFICE_ID || null;
  console.log(
    `[fiscal-watcher] runFiscalSyncAll effectiveOfficeId=${effectiveOfficeId} (officeId=${officeId}, OFFICE_ID=${OFFICE_ID})`,
  );
  if (!effectiveOfficeId) {
    result.errors.push({
      file: "(início)",
      error:
        "office_id ausente — VM não vinculada ao escritório (CONNECTOR_SECRET no .env e credential no Supabase)",
    });
    return result;
  }
  const empresasPath = BASE_PATH;
  const empresasExists =
    fs.existsSync(empresasPath) && fs.statSync(empresasPath).isDirectory();
  const allPathsOnDisk = empresasExists
    ? new Set(walkDir("", BASE_PATH))
    : new Set();

  let q = supabase.from("companies").select("id, name, office_id");
  q = q.eq("office_id", effectiveOfficeId);
  const { data: companiesRaw } = await q;
  const companies = (companiesRaw || []).filter(
    (c) => c && String(c.office_id) === String(effectiveOfficeId),
  );
  const nameToId = new Map(
    companies.map((c) => [normalizeCompanyName(c.name), c.id]),
  );
  /** Por company_id: Set de file_path que existem no disco. Inicializa para todas as empresas (vazio se pasta não existir). */
  const pathsOnDiskByCompany = new Map(companies.map((c) => [c.id, new Set()]));

  if (!empresasExists) {
    // Pasta base não existe: espelhar “zerando” para qualquer documento salvo no fiscal_documents.
    const rows = await fetchAllRows(() =>
      supabase
        .from("fiscal_documents")
        .select("id, file_path")
        .eq("office_id", effectiveOfficeId)
        .not("file_path", "is", null),
    );
    const idsToDelete = (rows || []).map((r) => r.id);
    if (idsToDelete.length > 0) {
      for (let index = 0; index < idsToDelete.length; index += 500) {
        const batch = idsToDelete.slice(index, index + 500);
        const { error: deleteError } = await supabase
          .from("fiscal_documents")
          .delete()
          .in("id", batch);
        if (!deleteError) result.deleted += batch.length;
        else {
          result.errors.push({
            file: "fiscal_documents",
            error: deleteError.message,
          });
        }
      }
    }
    await clearMissingFileReferences(
      supabase,
      effectiveOfficeId,
      result,
      allPathsOnDisk,
    );
    return result;
  }

  const companyDirs = fs
    .readdirSync(empresasPath, { withFileTypes: true })
    .filter((e) => e.isDirectory());
  let matchedCompanyDirs = 0;

  for (const companyDir of companyDirs) {
    const companyName = companyDir.name;
    const companyId = nameToId.get(normalizeCompanyName(companyName));
    if (!companyId) continue;
    matchedCompanyDirs += 1;
    const pathsOnDisk = pathsOnDiskByCompany.get(companyId);
    if (!pathsOnDisk) continue;

    for (const sub of ["Recebidas", "Emitidas"]) {
      const segment = path
        .join(companyName, "FISCAL", "NFS", sub)
        .replace(/\\/g, "/");
      const files = walkDir(segment, BASE_PATH);
      for (const fileRel of files) {
        pathsOnDisk.add(fileRel);
        const chave = path.basename(fileRel, path.extname(fileRel));
        const parts = fileRel.split(/[/\\]/);
        let periodo = new Date().toISOString().slice(0, 7);
        const y = parts.find((p) => /^\d{4}$/.test(p));
        const m = parts.find(
          (p) =>
            /^\d{2}$/.test(p) && parseInt(p, 10) >= 1 && parseInt(p, 10) <= 12,
        );
        if (y && m) periodo = `${y}-${m}`;
        const { data: existingRows } = await supabase
          .from("fiscal_documents")
          .select("id")
          .eq("company_id", companyId)
          .eq("file_path", fileRel)
          .limit(1);
        if (existingRows && existingRows.length > 0) {
          result.skipped++;
          continue;
        }
        const row = {
          office_id: String(effectiveOfficeId),
          company_id: companyId,
          type: "NFS",
          chave,
          periodo,
          status: "novo",
          file_path: fileRel,
        };
        if (result.inserted + result.skipped === 0)
          console.log(
            "[fiscal-watcher] Primeiro insert NFS payload:",
            JSON.stringify(row),
          );
        const { error } = await supabase.from("fiscal_documents").insert(row);
        if (error) {
          if (error.code === "23505") {
            result.skipped++;
          } else {
            result.errors.push({ file: fileRel, error: error.message });
          }
        } else {
          result.inserted++;
        }
      }
    }

    // NFE-NFC: FISCAL/NFE-NFC/Recebidas e FISCAL/NFE-NFC/Emitidas (tipo NFE ou NFC por nome do arquivo)
    for (const sub of ["Recebidas", "Emitidas"]) {
      const segment = path
        .join(companyName, "FISCAL", "NFE-NFC", sub)
        .replace(/\\/g, "/");
      const files = walkDir(segment, BASE_PATH);
      for (const fileRel of files) {
        pathsOnDisk.add(fileRel);
        const baseName = path.basename(fileRel, path.extname(fileRel));
        const nameLower = baseName.toLowerCase();
        const docType =
          nameLower.includes("nfc") || nameLower.includes("65") ? "NFC" : "NFE";
        const chave = baseName;
        const parts = fileRel.split(/[/\\]/);
        let periodo = new Date().toISOString().slice(0, 7);
        const y = parts.find((p) => /^\d{4}$/.test(p));
        const m = parts.find(
          (p) =>
            /^\d{2}$/.test(p) && parseInt(p, 10) >= 1 && parseInt(p, 10) <= 12,
        );
        if (y && m) periodo = `${y}-${m}`;
        const { data: existingRows } = await supabase
          .from("fiscal_documents")
          .select("id")
          .eq("company_id", companyId)
          .eq("file_path", fileRel)
          .limit(1);
        if (existingRows && existingRows.length > 0) {
          result.skipped++;
          continue;
        }
        const row = {
          office_id: String(effectiveOfficeId),
          company_id: companyId,
          type: docType,
          chave,
          periodo,
          status: "novo",
          file_path: fileRel,
        };
        const { error } = await supabase.from("fiscal_documents").insert(row);
        if (error) {
          if (error.code === "23505") {
            result.skipped++;
          } else {
            result.errors.push({ file: fileRel, error: error.message });
          }
        } else {
          result.inserted++;
        }
      }
    }
  }

  // Espelhamento: remove do banco os registros DESTE ESCRITÓRIO cujo arquivo não existe mais na pasta.
  console.log(
    `[fiscal-watcher] office=${effectiveOfficeId} | base_path=${empresasPath} | db_companies=${companies.length} | disk_dirs=${companyDirs.length} | matched_dirs=${matchedCompanyDirs}`,
  );
  if (companies.length > 0 && matchedCompanyDirs === 0) {
    console.warn(
      `[fiscal-watcher] Nenhuma pasta de empresa em BASE_PATH corresponde às empresas do escritório ${effectiveOfficeId}. Verifique BASE_PATH e o vínculo do conector.`,
    );
  }
  const rowsToMirrorDelete = await fetchAllRows(() =>
    supabase
      .from("fiscal_documents")
      .select("id, file_path")
      .eq("office_id", effectiveOfficeId)
      .not("file_path", "is", null),
  );
  const idsToDelete = (rowsToMirrorDelete || [])
    .filter((r) => {
      const info = resolveStoredFilePathInfo(r.file_path);
      return !info.exists;
    })
    .map((r) => r.id);
  if (idsToDelete.length > 0) {
    for (let index = 0; index < idsToDelete.length; index += 500) {
      const batch = idsToDelete.slice(index, index + 500);
      const { error: deleteError } = await supabase
        .from("fiscal_documents")
        .delete()
        .in("id", batch);
      if (!deleteError) result.deleted += batch.length;
      else {
        result.errors.push({
          file: "fiscal_documents",
          error: deleteError.message,
        });
      }
    }
  }

  await clearMissingFileReferences(
    supabase,
    effectiveOfficeId,
    result,
    allPathsOnDisk,
  );
  await clearMissingNfsStats(supabase, effectiveOfficeId, result);

  return result;
}

app.post(
  "/api/fiscal-sync-all",
  requireConnectorSecret,
  requireForwardedUserJwt,
  validateSupabaseJwt,
  heavyLimiter,
  async (req, res) => {
    const token = getForwardedUserJwt(req);
    const supabaseUrl = process.env.SUPABASE_URL;
    const supabaseKey = process.env.SUPABASE_ANON_KEY;
    if (!supabaseUrl || !supabaseKey) {
      return res.status(500).json({
        error:
          "Supabase não configurado no server-api. No .env da VM defina SUPABASE_URL e SUPABASE_ANON_KEY.",
      });
    }
    const supabase = createClient(supabaseUrl, supabaseKey, {
      global: { headers: { Authorization: `Bearer ${token}` } },
    });
    try {
      const officeId = OFFICE_ID || null;
      const { inserted, skipped, deleted, errors } = await runFiscalSyncAll(
        supabase,
        officeId,
      );
      return res.json({
        ok: true,
        inserted,
        skipped,
        deleted: deleted ?? 0,
        errors: errors.length ? errors : undefined,
      });
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  },
);

app.get("/health", (req, res) => res.json({ ok: true }));

/**
 * GET /api/folder-structure
 * Retorna a árvore de pastas (flat) para robôs montarem o path.
 * Path na VM: BASE_PATH/EMPRESAS/{nome_empresa}/{segmentos do nó}
 * Leitura pública (anon) para robôs sem JWT.
 */
app.get("/api/folder-structure", requireConnectorSecret, async (req, res) => {
  if (!OFFICE_ID) {
    return res.status(503).json({
      error:
        "Conector da VM sem office_id vinculado. Revise CONNECTOR_SECRET e office_server_credentials.",
    });
  }
  const supabase = createSupabaseReadClient();
  if (!supabase) {
    return res.status(500).json({ error: "Supabase não configurado" });
  }
  try {
    res.setHeader("Cache-Control", "no-store");
    const { data, error } = await supabase
      .from("folder_structure_nodes")
      .select("id, parent_id, name, slug, date_rule, position")
      .eq("office_id", OFFICE_ID)
      .order("parent_id", { nullsFirst: true })
      .order("position", { ascending: true });
    if (error) return res.status(500).json({ error: error.message });
    return res.json({ nodes: data ?? [] });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/robot-config?technical_id=xxx
 * Retorna configuração para o robô na VM: base_path (global), segment_path e date_rule do robô.
 * Robôs usam isso em vez de BASE_PATH e ROBOT_SEGMENT_PATH no .env (que passam a ser opcionais).
 */
app.get("/api/robot-config", requireConnectorSecret, async (req, res) => {
  const technicalId = (req.query.technical_id || "").toString().trim();
  if (!technicalId) {
    return res.status(400).json({ error: "technical_id é obrigatório" });
  }
  const supabase = createSupabaseReadClient();
  if (!supabase) {
    return res.status(500).json({ error: "Supabase não configurado" });
  }
  try {
    res.setHeader("Cache-Control", "no-store");
    if (!OFFICE_ID) {
      return res.status(503).json({
        error:
          "Conector da VM sem office_id vinculado. Revise CONNECTOR_SECRET e office_server_credentials.",
      });
    }
    const { data: robot, error: robotErr } = await supabase
      .from("robots")
      .select("segment_path, notes_mode")
      .eq("technical_id", technicalId)
      .maybeSingle();
    if (robotErr) return res.status(500).json({ error: robotErr.message });
    const segmentPath = (robot?.segment_path || "").trim() || null;
    const notesMode = (robot?.notes_mode || "").trim() || null;
    const { data: nodes, error: nodesErr } = await supabase
      .from("folder_structure_nodes")
      .select("id, parent_id, name, slug, date_rule, position")
      .eq("office_id", OFFICE_ID)
      .order("parent_id", { nullsFirst: true })
      .order("position", { ascending: true });
    if (nodesErr) return res.status(500).json({ error: nodesErr.message });
    const dateRule = segmentPath
      ? findDateRuleByPath(nodes ?? [], segmentPath)
      : null;
    return res.json({
      office_id: OFFICE_ID,
      office_server_id: OFFICE_SERVER_ID,
      base_path: BASE_PATH,
      segment_path: segmentPath,
      date_rule: dateRule,
      notes_mode: notesMode,
      folder_structure: nodes ?? [],
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

// WhatsApp: rota única segura /api/whatsapp/* (JWT + escritório + token interno no emissor)
const WHATSAPP_BACKEND_URL =
  process.env.WHATSAPP_BACKEND_URL || "http://localhost:3010";
const whatsappSecureProxy = createProxyMiddleware({
  target: WHATSAPP_BACKEND_URL,
  changeOrigin: true,
  proxyTimeout: 120_000,
  timeout: 120_000,
  pathRewrite: (pathReq) => {
    const noPrefix = pathReq.replace(/^\/api\/whatsapp/, "") || "/";
    return noPrefix.startsWith("/") ? noPrefix : `/${noPrefix}`;
  },
  onProxyReq: (proxyReq) => {
    if (OFFICE_ID) proxyReq.setHeader("X-Office-Id", OFFICE_ID);
    if (CONNECTOR_SECRET) {
      proxyReq.setHeader("Authorization", `Bearer ${CONNECTOR_SECRET}`);
    }
    try {
      proxyReq.removeHeader("x-office-user-jwt");
    } catch (_) {}
  },
  onError: (err, req, res) => {
    res
      .status(502)
      .json({ error: "Backend WhatsApp indisponível", detail: err.message });
  },
});
app.use(
  "/api/whatsapp",
  requireConnectorSecret,
  requireForwardedUserJwt,
  validateSupabaseJwt,
  validateUserBelongsToConnectorOffice,
  whatsappSecureProxy,
);

// Legado (desligado por padrão): expor /status, /qr, /send na raiz — inseguro em produção
if (String(process.env.WA_OPEN_LEGACY_PROXY || "").trim() === "1") {
  const whatsappProxy = createProxyMiddleware({
    target: WHATSAPP_BACKEND_URL,
    changeOrigin: true,
    proxyTimeout: 60_000,
    timeout: 60_000,
    onError: (err, req, res) => {
      res
        .status(502)
        .json({ error: "Backend WhatsApp indisponível", detail: err.message });
    },
  });
  const whatsappProxyViaApiPrefix = createProxyMiddleware({
    pathFilter: [
      "/api/send",
      "/api/status",
      "/api/groups",
      "/api/qr",
      "/api/connect",
      "/api/disconnect",
    ],
    target: WHATSAPP_BACKEND_URL,
    changeOrigin: true,
    proxyTimeout: 60_000,
    timeout: 60_000,
    pathRewrite: (pathReq) =>
      pathReq.startsWith("/api/") ? pathReq.slice(4) : pathReq,
    onError: (err, req, res) => {
      res
        .status(502)
        .json({ error: "Backend WhatsApp indisponível", detail: err.message });
    },
  });
  app.use(
    ["/send", "/status", "/groups", "/qr", "/connect", "/disconnect"],
    whatsappProxy,
  );
  app.use(whatsappProxyViaApiPrefix);
  console.warn(
    "[server-api] WA_OPEN_LEGACY_PROXY=1 — rotas WhatsApp antigas na raiz estão expostas.",
  );
}

app.use((req, res) => {
  const path = req.path || req.url || "";
  console.warn(
    "[server-api] 404 Rota não encontrada:",
    req.method,
    path,
    "| url:",
    req.originalUrl || req.url,
  );
  const hint =
    String(path || "").includes("download-zip-by-paths") ||
    String(req.originalUrl || "").includes("download-zip-by-paths")
      ? " Se esperava baixar ZIP: reinicie o Servidor (start.bat ou pm2 restart server-api) para carregar o código novo."
      : "";
  res.status(404).json({ error: "Rota não encontrada" + hint });
});

/** Monitoramento automático: quando novos arquivos chegam em EMPRESAS, sincroniza com Supabase. */
function startFiscalWatcher() {
  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!supabaseUrl || !serviceKey) {
    console.log(
      "[fiscal-watcher] SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY ausente; monitoramento automático desligado.",
    );
    return;
  }
  const empresasPath = BASE_PATH;
  if (
    !fs.existsSync(empresasPath) ||
    !fs.statSync(empresasPath).isDirectory()
  ) {
    console.log(
      "[fiscal-watcher] Pasta base da VM não encontrada; monitoramento desligado.",
    );
    return;
  }
  const supabase = createConfiguredSupabaseClient(supabaseUrl, serviceKey);
  let debounceTimer = null;
  const DEBOUNCE_MS = 4000;

  const runSync = () => {
    if (!OFFICE_ID) {
      console.warn(
        "[fiscal-watcher] OFFICE_ID ausente (CONNECTOR_SECRET não vinculado a um office_server?). Sync ignorado.",
      );
      return;
    }
    runFiscalSyncAll(supabase, OFFICE_ID)
      .then(({ inserted, skipped, deleted, errors }) => {
        if (inserted > 0 || deleted > 0 || errors.length > 0) {
          console.log(
            `[fiscal-watcher] Sync: ${inserted} inseridos, ${skipped} já existentes${deleted ? `, ${deleted} removidos` : ""}${errors.length ? `, ${errors.length} erros` : ""}`,
          );
          for (const e of errors) {
            console.error(`[fiscal-watcher] Erro: ${e.file} → ${e.error}`);
          }
        }
      })
      .catch((err) =>
        console.error("[fiscal-watcher] Erro ao sincronizar:", err.message),
      );
  };

  try {
    fs.watch(empresasPath, { recursive: true }, (eventType, filename) => {
      if (!filename || !/\.(xml|pdf)$/i.test(filename)) return;
      if (debounceTimer) clearTimeout(debounceTimer);
      debounceTimer = setTimeout(() => {
        debounceTimer = null;
        runSync();
      }, DEBOUNCE_MS);
    });
    console.log(
      "[fiscal-watcher] Monitorando a pasta base da VM — novos XML/PDF serão sincronizados automaticamente.",
    );
    // Sync inicial após 8s para garantir que OFFICE_ID já foi carregado do Supabase
    setTimeout(() => {
      if (!OFFICE_ID)
        console.error(
          "[fiscal-watcher] OFFICE_ID ainda null; sync inicial ignorado. Verifique CONNECTOR_SECRET e office_server_credentials no Supabase.",
        );
      else runSync();
    }, 8000);
    const intervalMs = Number(process.env.FISCAL_SYNC_INTERVAL_MS || 60_000);
    setInterval(runSync, intervalMs);
  } catch (err) {
    console.error(
      "[fiscal-watcher] Não foi possível monitorar a pasta base da VM:",
      err.message,
    );
  }
}

/**
 * Worker de refresh dos resumos/projeções (office_*).
 * Procura jobs em `public.office_analytics_refresh_queue` e processa com `process_office_refresh_queue`.
 * Rodar aqui evita depender de pg_cron/Supabase schedules no estágio atual do projeto.
 */
function startOfficeRefreshWorker() {
  const supabaseUrl = process.env.SUPABASE_URL;
  const serviceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!supabaseUrl || !serviceKey) {
    console.log(
      "[office-refresh-worker] SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY ausente; worker desligado.",
    );
    return;
  }

  const p_limit = Number(process.env.OFFICE_REFRESH_WORKER_LIMIT || 25);
  const intervalMs = Number(
    process.env.OFFICE_REFRESH_WORKER_INTERVAL_MS || 10_000,
  );
  const supabase = createClient(supabaseUrl, serviceKey);

  let running = false;
  let rpcMissingLogged = false;
  const runOnce = async () => {
    if (running) return;
    running = true;
    try {
      const { data, error } = await supabase.rpc(
        "process_office_refresh_queue",
        { p_limit },
      );
      if (error) throw error;
      rpcMissingLogged = false;
      const processed = Number(data ?? 0);
      console.log(`[office-refresh-worker] processed_count=${processed}`);
    } catch (err) {
      const msg = err?.message ?? String(err);
      const isMissingRpc =
        /function.*process_office_refresh_queue.*schema cache/i.test(msg);
      if (isMissingRpc && !rpcMissingLogged) {
        rpcMissingLogged = true;
        console.warn(
          "[office-refresh-worker] RPC process_office_refresh_queue nao encontrada. Rode as migrations no projeto Supabase (SUPABASE_URL do .env). Worker nao sera chamado ate a funcao existir.",
        );
      } else if (!isMissingRpc) {
        console.error("[office-refresh-worker] Erro ao processar fila:", msg);
      }
    } finally {
      running = false;
    }
  };

  void runOnce();
  setInterval(() => {
    void runOnce();
  }, intervalMs);
}

loadBasePathFromSupabase().then(() => {
  app.listen(PORT, () => {
    const robotRuntimeSupabase = createSupabaseServiceClient();
    console.log(`API unificada em http://localhost:${PORT}`);
    console.log(`[fiscal-watcher] ${FISCAL_SYNC_VERSION}`);
    console.log(`BASE_PATH: ${BASE_PATH}`);
    console.log(`ROBOTS_ROOT_PATH: ${ROBOTS_ROOT_PATH}`);
    console.log(
      `WhatsApp seguro: /api/whatsapp/* -> ${WHATSAPP_BACKEND_URL} (Bearer = CONNECTOR_SECRET)`,
    );
    console.log(
      `CONNECTOR_SECRET: ${CONNECTOR_SECRET_HASH ? "configurado" : "ausente"}`,
    );
    if (OFFICE_SERVER_ID) console.log(`OFFICE_SERVER_ID: ${OFFICE_SERVER_ID}`);
    console.log(`OFFICE_ID: ${OFFICE_ID ?? "null"}`);
    if (OFFICE_NAME && OFFICE_ID) {
      console.log(
        `[fiscal-watcher] ENVIO PRO ESCRITORIO: ${OFFICE_NAME} (id=${OFFICE_ID}) — dados fiscais e empresas somente deste escritório.`,
      );
    } else {
      console.log(
        `[fiscal-watcher] ENVIO PRO ESCRITORIO: não configurado (CONNECTOR_SECRET/credential sem vínculo). Nenhum dado será enviado até vincular.`,
      );
    }
    startOfficeRefreshWorker();
    startFiscalWatcher();
    if (robotRuntimeSupabase && OFFICE_ID && OFFICE_SERVER_ID) {
      startScheduleRulesWorker({
        supabase: robotRuntimeSupabase,
        officeId: OFFICE_ID,
        officeServerId: OFFICE_SERVER_ID,
        logger: console,
      });
    } else if (robotRuntimeSupabase && (!OFFICE_ID || !OFFICE_SERVER_ID)) {
      console.warn(
        "[schedule-rules-worker] OFFICE_ID ou OFFICE_SERVER_ID ausente; agendador diário desligado até vincular o conector.",
      );
    }
    if (robotRuntimeSupabase) {
      startRobotJsonRuntimeWorker({
        supabase: robotRuntimeSupabase,
        getContext: getRobotRuntimeContext,
        logger: console,
        dispatchIntervalMs: Number(process.env.ROBOT_JSON_DISPATCH_INTERVAL_MS || 5000),
        heartbeatIntervalMs: Number(process.env.ROBOT_JSON_HEARTBEAT_INTERVAL_MS || 10000),
        resultIntervalMs: Number(process.env.ROBOT_JSON_RESULT_INTERVAL_MS || 5000),
      });
    } else {
      console.warn(
        "[robot-json-runtime] SUPABASE_URL ou SUPABASE_SERVICE_ROLE_KEY ausente; worker desligado.",
      );
    }
  });
});
