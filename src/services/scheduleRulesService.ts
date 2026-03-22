import { supabase } from "./supabaseClient"
import type { Json, RobotExecutionMode, RobotNotesMode, Tables } from "@/types/database"
import { buildScheduleSettings } from "@/lib/robotExecutionPlanning"
import type { Robot } from "@/services/robotsService"

export type ScheduleRule = Tables<"schedule_rules">
export type RobotOperationSelectionMode = "all_eligible" | "manual_companies"
export const ROBOT_OPERATION_SCOPE = "robot_operation"

type RobotOperationRuleSettings = {
  scope: typeof ROBOT_OPERATION_SCOPE
  robot_technical_id: string
  selection_mode: RobotOperationSelectionMode
  auto_daily: boolean
  robot_snapshots?: Record<string, Json>
}

function asObject(value: Json | null | undefined): Record<string, Json> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {}
  return value as Record<string, Json>
}

export async function getScheduleRules(): Promise<ScheduleRule[]> {
  const { data, error } = await supabase
    .from("schedule_rules")
    .select("*")
    .order("created_at", { ascending: false })
  if (error) throw error
  return (data ?? []) as ScheduleRule[]
}

export async function getActiveScheduleRules(): Promise<ScheduleRule[]> {
  const { data, error } = await supabase
    .from("schedule_rules")
    .select("*")
    .eq("status", "active")
    .eq("run_daily", true)
    .order("run_at_time")
  if (error) throw error
  return (data ?? []) as ScheduleRule[]
}

export function getRobotOperationRuleSettings(rule: ScheduleRule | null | undefined): RobotOperationRuleSettings | null {
  const settings = asObject(rule?.settings)
  if (String(settings.scope ?? "").trim() !== ROBOT_OPERATION_SCOPE) return null
  const robotTechnicalId =
    String(settings.robot_technical_id ?? "").trim() ||
    String(rule?.robot_technical_ids?.[0] ?? "").trim()
  if (!robotTechnicalId) return null

  const selectionModeRaw = String(settings.selection_mode ?? "").trim()
  const selectionMode: RobotOperationSelectionMode =
    selectionModeRaw === "manual_companies" ? "manual_companies" : "all_eligible"

  return {
    scope: ROBOT_OPERATION_SCOPE,
    robot_technical_id: robotTechnicalId,
    selection_mode: selectionMode,
    auto_daily: settings.auto_daily !== false,
    robot_snapshots:
      settings.robot_snapshots && typeof settings.robot_snapshots === "object" && !Array.isArray(settings.robot_snapshots)
        ? (settings.robot_snapshots as Record<string, Json>)
        : undefined,
  }
}

export function isRobotOperationRule(rule: ScheduleRule | null | undefined, robotTechnicalId?: string): boolean {
  const settings = getRobotOperationRuleSettings(rule)
  if (!settings) return false
  if (!robotTechnicalId) return true
  return settings.robot_technical_id === robotTechnicalId
}

export function findRobotOperationRule(rules: ScheduleRule[], robotTechnicalId: string): ScheduleRule | null {
  return rules.find((rule) => isRobotOperationRule(rule, robotTechnicalId)) ?? null
}

export async function createScheduleRule(params: {
  companyIds: string[]
  robotTechnicalIds: string[]
  notesMode?: RobotNotesMode | null
  runAtDate: string
  runAtTime: string
  runDaily: boolean
  executionMode?: RobotExecutionMode | null
  settings?: Json | null
}): Promise<ScheduleRule> {
  const { data: { user } } = await supabase.auth.getUser()
  const { data, error } = await supabase
    .from("schedule_rules")
    .insert({
      company_ids: params.companyIds,
      robot_technical_ids: params.robotTechnicalIds,
      notes_mode: params.notesMode ?? null,
      period_start: null,
      period_end: null,
      run_at_date: params.runAtDate,
      run_at_time: params.runAtTime,
      run_daily: params.runDaily,
      execution_mode: params.executionMode ?? "sequential",
      settings: params.settings ?? {},
      status: "active",
      created_by: user?.id ?? null,
    })
    .select()
    .single()
  if (error) throw error
  return data as ScheduleRule
}

export async function updateScheduleRule(
  id: string,
  params: {
    companyIds: string[]
    robotTechnicalIds: string[]
    notesMode?: RobotNotesMode | null
    runAtDate: string
    runAtTime: string
    runDaily: boolean
    executionMode?: RobotExecutionMode | null
    lastRunAt?: string | null
    settings?: Json | null
  }
): Promise<ScheduleRule> {
  const update: Record<string, unknown> = {
    company_ids: params.companyIds,
    robot_technical_ids: params.robotTechnicalIds,
    notes_mode: params.notesMode ?? null,
    period_start: null,
    period_end: null,
    run_at_date: params.runAtDate,
    run_at_time: params.runAtTime,
    run_daily: params.runDaily,
    execution_mode: params.executionMode ?? "sequential",
    settings: params.settings ?? {},
    status: "active",
  }
  if (params.lastRunAt !== undefined) {
    update.last_run_at = params.lastRunAt
  }
  const { data, error } = await supabase
    .from("schedule_rules")
    .update(update)
    .eq("id", id)
    .select()
    .single()
  if (error) throw error
  return data as ScheduleRule
}

export async function updateScheduleRuleStatus(
  id: string,
  status: "active" | "paused" | "completed"
): Promise<ScheduleRule> {
  const payload: { status: "active" | "paused" | "completed"; last_run_at?: null } = { status }
  if (status === "paused") {
    payload.last_run_at = null
  }
  const { data, error } = await supabase
    .from("schedule_rules")
    .update(payload)
    .eq("id", id)
    .select()
    .single()
  if (error) throw error
  return data as ScheduleRule
}

/** Pausa a regra e zera last_run_at para que, ao reativar, a próxima execução seja na data/hora configurada. */
export async function pauseScheduleRule(id: string): Promise<ScheduleRule> {
  return updateScheduleRuleStatus(id, "paused")
}

export async function deleteScheduleRule(id: string): Promise<void> {
  const { error } = await supabase.from("schedule_rules").delete().eq("id", id)
  if (error) throw error
}

export async function upsertRobotOperationScheduleRule(params: {
  robot: Robot
  companyIds: string[]
  runAtDate: string
  runAtTime: string
  selectionMode: RobotOperationSelectionMode
  status?: "active" | "paused"
  autoDaily?: boolean
  executionMode?: RobotExecutionMode | null
}): Promise<ScheduleRule> {
  const rules = await getScheduleRules()
  const existingRule = findRobotOperationRule(rules, params.robot.technical_id)
  const settings: RobotOperationRuleSettings = {
    scope: ROBOT_OPERATION_SCOPE,
    robot_technical_id: params.robot.technical_id,
    selection_mode: params.selectionMode,
    auto_daily: params.autoDaily !== false,
    robot_snapshots: buildScheduleSettings([params.robot]).robot_snapshots as Record<string, Json>,
  }

  if (existingRule) {
    const { data, error } = await supabase
      .from("schedule_rules")
      .update({
        company_ids: params.selectionMode === "manual_companies" ? params.companyIds : [],
        robot_technical_ids: [params.robot.technical_id],
        run_at_date: params.runAtDate,
        run_at_time: params.runAtTime,
        run_daily: true,
        execution_mode: params.executionMode ?? "sequential",
        status: params.status ?? "active",
        settings,
      })
      .eq("id", existingRule.id)
      .select()
      .single()
    if (error) throw error
    return data as ScheduleRule
  }

  return createScheduleRule({
    companyIds: params.selectionMode === "manual_companies" ? params.companyIds : [],
    robotTechnicalIds: [params.robot.technical_id],
    runAtDate: params.runAtDate,
    runAtTime: params.runAtTime,
    runDaily: true,
    executionMode: params.executionMode ?? "sequential",
    settings,
  })
}
