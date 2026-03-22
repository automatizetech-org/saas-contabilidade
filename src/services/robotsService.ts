import { supabase } from "./supabaseClient";
import type { Database, FiscalNotesKind, Json, RobotNotesMode } from "@/types/database";
import type { CompanySefazLogin } from "./companiesService";
import { isValidCpf, onlyDigits } from "@/lib/brazilDocuments";
import { getCurrentOfficeContext } from "./officeContextService";

type VisibleRobotRow = Database["public"]["Functions"]["get_visible_robots"]["Returns"][number];

export type Robot = VisibleRobotRow;
export type RobotStatus = Robot["status"];

let visibleRobotsRpcAvailable: boolean | null = null;
const CONFIGURED_PASSWORD_PLACEHOLDER = "__configured__";

function getErrorText(error: unknown) {
  if (error instanceof Error) return error.message;
  if (error && typeof error === "object") {
    const record = error as Record<string, unknown>;
    return [
      record.message,
      record.details,
      record.hint,
      record.code,
      JSON.stringify(record),
    ]
      .filter(Boolean)
      .join(" ");
  }
  return String(error);
}

function isMissingVisibleRobotsRpc(error: unknown) {
  const message = getErrorText(error);
  return (
    message.includes("get_visible_robots") ||
    message.includes("schema cache") ||
    message.includes("PGRST202")
  );
}

function isRobotsReadDenied(error: unknown) {
  const message = getErrorText(error).toLowerCase();
  return (
    message.includes("permission denied") ||
    message.includes("row-level security") ||
    message.includes("not allowed") ||
    message.includes("42501")
  );
}

function isMissingRelation(error: unknown, relationName: string) {
  const message = getErrorText(error).toLowerCase();
  return (
    message.includes(relationName.toLowerCase()) ||
    message.includes("schema cache") ||
    message.includes("pgrst205") ||
    message.includes("relation")
  );
}

function isCatalogWriteSoftFailure(error: unknown) {
  const message = getErrorText(error).toLowerCase();
  return (
    message.includes("permission denied") ||
    message.includes("row-level security") ||
    message.includes("0 rows") ||
    message.includes("no rows") ||
    message.includes("406") ||
    message.includes("not acceptable")
  );
}

function sanitizeGlobalLogins(logins: CompanySefazLogin[] | undefined) {
  if (!Array.isArray(logins)) return undefined;

  const seen = new Set<string>();
  const cleaned = logins
    .map((login) => ({
      cpf: onlyDigits(login.cpf),
      password: String(login.password ?? "").trim(),
      is_default: Boolean(login.is_default),
    }))
    .filter((login) => login.cpf || login.password);

  for (const login of cleaned) {
    if (!isValidCpf(login.cpf)) {
      throw new Error("Informe um CPF valido para os logins globais do robo.");
    }
    if (!login.password) {
      throw new Error("Todo login global do robo precisa ter senha preenchida.");
    }
  }

  const deduped = cleaned.filter((login) => {
    if (seen.has(login.cpf)) return false;
    seen.add(login.cpf);
    return true;
  });

  if (deduped.length === 0) return [];

  const defaultIndex = deduped.findIndex((login) => login.is_default);
  return deduped.map((login, index) => ({
    ...login,
    is_default: defaultIndex === -1 ? index === 0 : index === defaultIndex,
  }));
}

function parseStoredGlobalLogins(value: unknown): CompanySefazLogin[] {
  if (!Array.isArray(value)) return [];

  const seen = new Set<string>();
  return value
    .map((item) => {
      if (!item || typeof item !== "object" || Array.isArray(item)) return null;
      const row = item as Record<string, unknown>;
      const cpf = onlyDigits(String(row.cpf ?? ""));
      const password = String(row.password ?? row.senha ?? "").trim();
      if (!isValidCpf(cpf) || !password) return null;
      return {
        cpf,
        password,
        is_default: Boolean(row.is_default),
      } satisfies CompanySefazLogin;
    })
    .filter((item): item is CompanySefazLogin => Boolean(item))
    .filter((item) => {
      if (seen.has(item.cpf)) return false;
      seen.add(item.cpf);
      return true;
    });
}

async function loadPersistedGlobalLogins(robotId: string, technicalId: string): Promise<CompanySefazLogin[]> {
  const context = await getCurrentOfficeContext().catch(() => null);
  const officeId = context?.officeId ?? null;

  if (officeId) {
    const { data, error } = await supabase
      .from("office_robot_configs")
      .select("global_logins")
      .eq("office_id", officeId)
      .eq("robot_technical_id", technicalId)
      .maybeSingle();

    if (!error && data) {
      return parseStoredGlobalLogins((data as Record<string, unknown>).global_logins);
    }
  }

  const { data, error } = await supabase
    .from("robots")
    .select("global_logins")
    .eq("id", robotId)
    .maybeSingle();

  if (error) return [];
  return parseStoredGlobalLogins((data as Record<string, unknown> | null)?.global_logins);
}

export async function getRobotEditableGlobalLogins(
  robotId: string,
  technicalId: string,
): Promise<CompanySefazLogin[]> {
  return loadPersistedGlobalLogins(robotId, technicalId);
}

function mergeConfiguredGlobalLoginPasswords(
  nextLogins: CompanySefazLogin[],
  persistedLogins: CompanySefazLogin[],
): CompanySefazLogin[] {
  const persistedByCpf = new Map(
    persistedLogins.map((login) => [onlyDigits(login.cpf), String(login.password ?? "").trim()] as const),
  );

  const unresolvedCpfs: string[] = [];
  const merged = nextLogins.map((login) => {
    const cpf = onlyDigits(login.cpf);
    const password = String(login.password ?? "").trim();

    if (password !== CONFIGURED_PASSWORD_PLACEHOLDER) {
      return { ...login, cpf, password };
    }

    const persistedPassword = persistedByCpf.get(cpf);
    if (!persistedPassword) {
      unresolvedCpfs.push(cpf);
      return { ...login, cpf, password };
    }

    return {
      ...login,
      cpf,
      password: persistedPassword,
    };
  });

  if (unresolvedCpfs.length > 0) {
    throw new Error(
      "Nao foi possivel preservar as senhas ja configuradas deste robo. Reabra a tela e tente novamente.",
    );
  }

  return merged;
}

function buildRobotRow(
  rawRow: Record<string, unknown>,
  config: Record<string, unknown> | null,
  runtime: Record<string, unknown> | null,
): Robot {
  const technicalId = String(rawRow.technical_id ?? "");

  return {
    id: String(rawRow.id ?? ""),
    technical_id: technicalId,
    display_name: String(config?.display_name ?? rawRow.display_name ?? technicalId),
    status: (runtime?.status ?? rawRow.status ?? "inactive") as Robot["status"],
    last_heartbeat_at: (runtime?.last_heartbeat_at ?? rawRow.last_heartbeat_at ?? null) as string | null,
    segment_path: (config?.segment_path ?? rawRow.segment_path ?? null) as string | null,
    created_at: String(rawRow.created_at ?? new Date(0).toISOString()),
    updated_at: String(runtime?.updated_at ?? config?.updated_at ?? rawRow.updated_at ?? new Date(0).toISOString()),
    notes_mode: (config?.notes_mode ?? rawRow.notes_mode ?? null) as RobotNotesMode | null,
    date_execution_mode: (config?.date_execution_mode ?? rawRow.date_execution_mode ?? null) as Robot["date_execution_mode"],
    initial_period_start: (config?.initial_period_start ?? rawRow.initial_period_start ?? null) as string | null,
    initial_period_end: (config?.initial_period_end ?? rawRow.initial_period_end ?? null) as string | null,
    last_period_end: (config?.last_period_end ?? rawRow.last_period_end ?? null) as string | null,
    is_fiscal_notes_robot: Boolean(rawRow.is_fiscal_notes_robot),
    fiscal_notes_kind: (rawRow.fiscal_notes_kind ?? null) as FiscalNotesKind | null,
    global_logins: (config?.global_logins ?? rawRow.global_logins ?? []) as Json,
    runtime_folder: (rawRow.runtime_folder ?? technicalId) as string | null,
    entrypoint_relpath: String(rawRow.entrypoint_relpath ?? "bot.py"),
    job_file_relpath: String(rawRow.job_file_relpath ?? "data/json/job.json"),
    result_file_relpath: String(rawRow.result_file_relpath ?? "data/json/result.json"),
    heartbeat_file_relpath: String(rawRow.heartbeat_file_relpath ?? "data/json/heartbeat.json"),
    capabilities: (rawRow.capabilities ?? {}) as Json,
    runtime_defaults: (rawRow.runtime_defaults ?? {}) as Json,
    admin_form_schema: (rawRow.admin_form_schema ?? []) as Json,
    company_form_schema: (rawRow.company_form_schema ?? []) as Json,
    schedule_form_schema: (rawRow.schedule_form_schema ?? []) as Json,
    admin_settings: (config?.admin_settings ?? {}) as Json,
    execution_defaults: (config?.execution_defaults ?? {}) as Json,
  };
}

async function getRobotsSafeFallback(): Promise<Robot[]> {
  const { data, error } = await supabase
    .from("robots")
    .select("*")
    .order("display_name", { ascending: true });

  if (error) {
    if (isRobotsReadDenied(error)) return [];
    throw error;
  }

  const context = await getCurrentOfficeContext().catch(() => null);
  const officeId = context?.officeId ?? null;

  let configRows: Array<Record<string, unknown>> = [];
  if (officeId) {
    const { data: configs, error: configsError } = await supabase
      .from("office_robot_configs")
      .select("*")
      .eq("office_id", officeId);
    if (!configsError) {
      configRows = (configs ?? []) as Array<Record<string, unknown>>;
    } else if (!isMissingRelation(configsError, "office_robot_configs")) {
      throw configsError;
    }
  }

  let runtimeRows: Array<Record<string, unknown>> = [];
  if (officeId) {
    const { data: servers, error: serversError } = await supabase
      .from("office_servers")
      .select("id")
      .eq("office_id", officeId)
      .eq("is_active", true)
      .limit(1);

    if (!serversError && servers?.[0]?.id) {
      const { data: runtimes, error: runtimeError } = await supabase
        .from("office_robot_runtime")
        .select("*")
        .eq("office_server_id", servers[0].id);
      if (!runtimeError) {
        runtimeRows = (runtimes ?? []) as Array<Record<string, unknown>>;
      } else if (!isMissingRelation(runtimeError, "office_robot_runtime")) {
        throw runtimeError;
      }
    }
  }

  const configByTechnicalId = new Map(
    configRows.map((row) => [String(row.robot_technical_id ?? ""), row]),
  );
  const runtimeByTechnicalId = new Map(
    runtimeRows.map((row) => [String(row.robot_technical_id ?? ""), row]),
  );

  return ((data ?? []) as Array<Record<string, unknown>>).map((row) =>
    buildRobotRow(
      row,
      configByTechnicalId.get(String(row.technical_id ?? "")) ?? null,
      runtimeByTechnicalId.get(String(row.technical_id ?? "")) ?? null,
    ),
  );
}

export async function getRobots(): Promise<Robot[]> {
  if (visibleRobotsRpcAvailable !== false) {
    const { data, error } = await supabase.rpc("get_visible_robots");
    if (!error) {
      visibleRobotsRpcAvailable = true;
      return (data ?? []) as Robot[];
    }
    if (!isMissingVisibleRobotsRpc(error)) throw error;
    visibleRobotsRpcAvailable = false;
  }

  return getRobotsSafeFallback();
}

export async function updateRobotDisplayName(
  id: string,
  displayName: string,
): Promise<Robot> {
  const { data, error } = await supabase
    .from("robots")
    .update({ display_name: displayName.trim() })
    .eq("id", id)
    .select()
    .single();
  if (error) throw error;
  return buildRobotRow(data as Record<string, unknown>, null, null);
}

export async function updateRobot(
  id: string,
  updates: {
    display_name?: string;
    segment_path?: string | null;
    is_fiscal_notes_robot?: boolean;
    fiscal_notes_kind?: FiscalNotesKind | null;
    notes_mode?: RobotNotesMode | null;
    date_execution_mode?: "competencia" | "interval" | null;
    initial_period_start?: string | null;
    initial_period_end?: string | null;
    last_period_end?: string | null;
    global_logins?: CompanySefazLogin[];
    admin_settings?: Json;
    execution_defaults?: Json;
  },
): Promise<Robot> {
  const visibleRobots = await getRobots().catch(() => []);
  const existingRobots = visibleRobots.length > 0 ? visibleRobots : await getRobotsSafeFallback();
  const existingRobot = existingRobots.find((item) => item.id === id);
  if (!existingRobot) {
    throw new Error("Robô não encontrado.");
  }

  const sanitizedUpdates: Record<string, unknown> = { ...(updates as Record<string, unknown>) };
  if (updates.global_logins !== undefined) {
    const persistedGlobalLogins = await loadPersistedGlobalLogins(id, existingRobot.technical_id);
    sanitizedUpdates.global_logins = sanitizeGlobalLogins(
      mergeConfiguredGlobalLoginPasswords(updates.global_logins, persistedGlobalLogins),
    );
  }

  const catalogPayload: Record<string, unknown> = {};
  for (const key of [
    "display_name",
    "segment_path",
    "notes_mode",
    "date_execution_mode",
    "initial_period_start",
    "initial_period_end",
    "last_period_end",
    "global_logins",
    "is_fiscal_notes_robot",
    "fiscal_notes_kind",
  ]) {
    if (key in sanitizedUpdates) catalogPayload[key] = sanitizedUpdates[key];
  }

  const officeConfigPayload: Record<string, unknown> = {};
  for (const key of [
    "display_name",
    "segment_path",
    "notes_mode",
    "date_execution_mode",
    "initial_period_start",
    "initial_period_end",
    "last_period_end",
    "global_logins",
    "admin_settings",
    "execution_defaults",
  ]) {
    if (key in sanitizedUpdates) officeConfigPayload[key] = sanitizedUpdates[key];
  }

  const runOfficeConfigUpdate = async (payload: Record<string, unknown>) => {
    if (Object.keys(payload).length === 0) return;

    const context = await getCurrentOfficeContext();
    if (!context?.officeId) {
      throw new Error("Nenhum escritorio ativo encontrado.");
    }

    const { error } = await supabase
      .from("office_robot_configs")
      .upsert(
        {
          office_id: context.officeId,
          robot_technical_id: existingRobot.technical_id,
          ...payload,
          updated_at: new Date().toISOString(),
        },
        { onConflict: "office_id,robot_technical_id" },
      );
    if (error) throw error;
  };

  const runCatalogUpdate = async (payload: Record<string, unknown>) => {
    if (Object.keys(payload).length === 0) return;

    const { error } = await supabase
      .from("robots")
      .update(payload)
      .eq("id", id);
    if (error) throw error;
  };

  const loadMergedRobot = async () => {
    const robots = await getRobots();
    const robot = robots.find((item) => item.id === id || item.technical_id === existingRobot.technical_id);
    if (!robot) throw new Error("Robô não encontrado após atualização.");
    return robot;
  };

  const applyBestEffortCatalogUpdate = async (payload: Record<string, unknown>) => {
    try {
      await runCatalogUpdate(payload);
    } catch (catalogError) {
      if (!isCatalogWriteSoftFailure(catalogError) && !isMissingRelation(catalogError, "robots")) {
        throw catalogError;
      }
    }
  };

  try {
    try {
      await runOfficeConfigUpdate(officeConfigPayload);
    } catch (officeError) {
      if (!isMissingRelation(officeError, "office_robot_configs")) throw officeError;
    }

    await applyBestEffortCatalogUpdate(catalogPayload);
    return await loadMergedRobot();
  } catch (error) {
    const message = getErrorText(error);
    const fallbackOfficePayload = { ...officeConfigPayload };
    const fallbackCatalogPayload = { ...catalogPayload };

    if (
      message.includes("robots_notes_mode_check") ||
      message.includes("notes_mode") ||
      message.includes("23514")
    ) {
      delete fallbackOfficePayload.notes_mode;
      delete fallbackCatalogPayload.notes_mode;
    }

    if (
      message.includes("is_fiscal_notes_robot") ||
      message.includes("fiscal_notes_kind") ||
      message.includes("PGRST204")
    ) {
      delete fallbackCatalogPayload.is_fiscal_notes_robot;
      delete fallbackCatalogPayload.fiscal_notes_kind;
    }

    try {
      await runOfficeConfigUpdate(fallbackOfficePayload);
    } catch (officeError) {
      if (!isMissingRelation(officeError, "office_robot_configs")) throw officeError;
    }

    await applyBestEffortCatalogUpdate(fallbackCatalogPayload);
    return await loadMergedRobot();
  }
}
