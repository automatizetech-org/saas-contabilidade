/**
 * Dispara execution_requests para regras de agendamento ativas (run_daily) quando
 * o horário em America/Sao_Paulo já passou e ainda não houve execução no dia.
 *
 * O painel só enfileira na primeira gravação se "agora" >= horário; sem este worker,
 * as execuções seguintes (24h) não ocorreriam sem o servidor consultando o Supabase.
 */

import { randomUUID } from "crypto";
import { listRobotRuntimeRows } from "./robot-json-runtime.js";

const TZ = "America/Sao_Paulo";

function coerceJsonObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function normalizeCityName(value) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .trim()
    .toLowerCase();
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

function getRobotExecutionCity(robot) {
  const executionDefaults = coerceJsonObject(robot.execution_defaults);
  const value = String(executionDefaults.city_name ?? "").trim();
  return value || null;
}

function getEligibleCompanyIds(robot, selectedCompanyIds, companies) {
  const cityFilter = getRobotExecutionCity(robot);
  if (!cityFilter) return selectedCompanyIds;
  const normalizedCity = normalizeCityName(cityFilter);
  const companyById = new Map((companies ?? []).map((c) => [c.id, c]));
  return selectedCompanyIds.filter((companyId) => {
    const company = companyById.get(companyId);
    return normalizeCityName(company?.city_name) === normalizedCity;
  });
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

async function deletePendingForRobot(supabase, officeId, technicalId) {
  const { error } = await supabase
    .from("execution_requests")
    .delete()
    .eq("office_id", officeId)
    .eq("status", "pending")
    .contains("robot_technical_ids", [technicalId]);
  if (error) throw error;
}

export async function processDueScheduleRules({ supabase, officeId, officeServerId, logger }) {
  if (!officeId || !officeServerId) return;

  const { data: rules, error: rulesError } = await supabase
    .from("schedule_rules")
    .select("*")
    .eq("office_id", officeId)
    .eq("status", "active")
    .eq("run_daily", true);

  if (rulesError) throw rulesError;

  const mergedRobots = await listRobotRuntimeRows(supabase, officeId, officeServerId);
  const robotByTechnicalId = new Map(mergedRobots.map((r) => [r.technical_id, r]));

  const todayYmd = todayYmdSp();

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

    const companyIds = Array.isArray(rule.company_ids) ? rule.company_ids : [];
    if (companyIds.length === 0) continue;

    const { data: companies, error: companiesError } = await supabase
      .from("companies")
      .select("id, city_name")
      .in("id", companyIds);
    if (companiesError) throw companiesError;

    const orderedIds = expandRobotTechnicalIds(rule, mergedRobots);
    if (orderedIds.length === 0) continue;

    const executionGroupId = randomUUID();
    const executionMode = String(rule.execution_mode || "sequential").trim().toLowerCase() === "parallel" ? "parallel" : "sequential";
    let created = 0;

    for (const [index, technicalId] of orderedIds.entries()) {
      const robot = robotByTechnicalId.get(technicalId);
      if (!robot) continue;

      const { periodStart, periodEnd } = computeScheduledPeriodForRobot(robot, todayYmd);
      const eligibleCompanyIds = getEligibleCompanyIds(robot, companyIds, companies ?? []);
      if (eligibleCompanyIds.length === 0) continue;

      await deletePendingForRobot(supabase, officeId, technicalId);

      const jobPayload = buildJobPayloadForRobot(robot, eligibleCompanyIds, periodStart, periodEnd);
      const row = {
        office_id: officeId,
        company_ids: eligibleCompanyIds,
        robot_technical_ids: [technicalId],
        period_start: periodStart,
        period_end: periodEnd,
        notes_mode: getRobotNotesMode(robot),
        schedule_rule_id: rule.id,
        execution_mode: executionMode,
        execution_group_id: executionGroupId,
        execution_order: index,
        job_payload: jobPayload,
        source: "scheduler",
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

export function startScheduleRulesWorker({ supabase, officeId, officeServerId, logger = console }) {
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
