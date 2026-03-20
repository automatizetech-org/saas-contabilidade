import fs from "fs";
import path from "path";
import { createHash } from "crypto";

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
  const raw = fs.readFileSync(filePath, "utf8");
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

function archiveJsonFile(filePath, payload, suffix = "snapshot") {
  if (!filePath) return;
  const archiveDir = path.join(path.dirname(filePath), "archive");
  ensureDirectory(archiveDir);
  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const baseName = path.basename(filePath, path.extname(filePath));
  const archivePath = path.join(archiveDir, `${baseName}.${stamp}.${suffix}.json`);
  writeJsonAtomic(archivePath, payload);
  removeFileIfExists(filePath);
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
  const runtimeFolder = String(robotRow.runtime_folder || robotRow.technical_id || "").trim();
  if (!runtimeFolder) return null;

  const runtimeRoot = resolveWithin(robotsRootPath, runtimeFolder);
  return {
    runtimeRoot,
    entrypointPath: resolveWithin(runtimeRoot, normalizeRuntimeRelPath(robotRow.entrypoint_relpath, "bot.py")),
    jobFilePath: resolveWithin(runtimeRoot, normalizeRuntimeRelPath(robotRow.job_file_relpath, path.join("data", "json", "job.json"))),
    resultFilePath: resolveWithin(runtimeRoot, normalizeRuntimeRelPath(robotRow.result_file_relpath, path.join("data", "json", "result.json"))),
    heartbeatFilePath: resolveWithin(runtimeRoot, normalizeRuntimeRelPath(robotRow.heartbeat_file_relpath, path.join("data", "json", "heartbeat.json"))),
  };
}

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
  const { data, error } = await supabase
    .from("execution_requests")
    .select("*")
    .eq("office_id", officeId)
    .eq("status", "pending")
    .contains("robot_technical_ids", [robotRow.technical_id])
    .order("execution_order", { ascending: true, nullsFirst: true })
    .order("created_at", { ascending: true })
    .limit(50);
  if (error) throw error;

  for (const row of data ?? []) {
    const executionMode = String(row.execution_mode || "sequential").trim().toLowerCase();
    if (executionMode === "sequential" && row.execution_group_id) {
      const { data: blockers, error: blockersError } = await supabase
        .from("execution_requests")
        .select("id,execution_order,created_at")
        .eq("office_id", officeId)
        .eq("execution_group_id", row.execution_group_id)
        .in("status", ["pending", "running"])
        .order("execution_order", { ascending: true, nullsFirst: true })
        .order("created_at", { ascending: true });
      if (blockersError) throw blockersError;
      if ((blockers ?? []).length > 0 && blockers[0].id !== row.id) continue;
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
      .eq("id", row.id)
      .eq("status", "pending")
      .select("*")
      .maybeSingle();

    if (!claimError && claimed?.id) return claimed;
  }

  return null;
}

async function loadCompaniesForExecution(supabase, executionRequest, robotRow) {
  const companyIds = Array.isArray(executionRequest.company_ids) ? executionRequest.company_ids : [];
  if (companyIds.length === 0) return [];
  const requestSettings = coerceJsonObject(executionRequest.job_payload);
  const requestedCityName = String(
    requestSettings.city_name ??
    coerceJsonObject(requestSettings.execution_defaults).city_name ??
    "",
  ).trim().toLowerCase();

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
    .filter((company) => {
      if (!requestedCityName) return true;
      return String(company.city_name || "").trim().toLowerCase() === requestedCityName;
    })
    .sort((left, right) => (orderMap.get(left.company_id) ?? 0) - (orderMap.get(right.company_id) ?? 0));

  return rows;
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
    company_ids: Array.isArray(executionRequest.company_ids) ? executionRequest.company_ids : [],
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

  const eventId = resultEventId(resultPayload);
  if (!eventId) {
    logger?.warn?.(`[robot-json-runtime] ${robotRow.technical_id}: result.json sem event_id/execution_request_id.`);
    archiveJsonFile(runtimePaths.resultFilePath, resultPayload, "invalid");
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
    archiveJsonFile(runtimePaths.resultFilePath, resultPayload, "duplicate");
    removeFileIfExists(runtimePaths.jobFilePath);
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
    status: status === "completed" ? "active" : "inactive",
    last_heartbeat_at: new Date().toISOString(),
    current_execution_request_id: null,
    current_job_id: null,
    heartbeat_payload: {
      source: "result_ingestor",
      status: status === "completed" ? "active" : "inactive",
      last_result_event_id: eventId,
    },
  });

  archiveJsonFile(runtimePaths.resultFilePath, resultPayload, "processed");
  removeFileIfExists(runtimePaths.jobFilePath);
  return true;
}

async function readHeartbeatAndSync(supabase, { officeId, officeServerId, robotRow, runtimePaths, logger }) {
  const heartbeat = safeReadJson(runtimePaths.heartbeatFilePath);
  const now = Date.now();
  const heartbeatUpdatedAt = heartbeat?.updated_at ? Date.parse(heartbeat.updated_at) : Number.NaN;
  const isFresh = Number.isFinite(heartbeatUpdatedAt) && now - heartbeatUpdatedAt <= 90_000;
  const status = isFresh
    ? String(heartbeat?.status || "active").trim() || "active"
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

  if (!isFresh && heartbeat?.current_execution_request_id) {
    try {
      await finalizeExecutionRequest(
        supabase,
        heartbeat.current_execution_request_id,
        false,
        "Robot heartbeat timeout",
        { timeout: true },
      );
    } catch (error) {
      logger?.warn?.(`[robot-json-runtime] Timeout finalize falhou para ${robotRow.technical_id}: ${error?.message || error}`);
    }
  }
}

async function dispatchPendingJob(supabase, context, robotRow, runtimePaths) {
  if (fs.existsSync(runtimePaths.jobFilePath) || fs.existsSync(runtimePaths.resultFilePath)) {
    return false;
  }
  if (!fs.existsSync(runtimePaths.runtimeRoot) || !fs.existsSync(runtimePaths.entrypointPath)) {
    return false;
  }

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
    .update({ job_payload: jobPayload })
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

export function startRobotJsonRuntimeWorker({
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
