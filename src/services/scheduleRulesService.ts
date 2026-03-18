import { supabase } from "./supabaseClient"
import type { RobotExecutionMode, RobotNotesMode, Tables } from "@/types/database"

export type ScheduleRule = Tables<"schedule_rules">

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

export async function createScheduleRule(params: {
  companyIds: string[]
  robotTechnicalIds: string[]
  notesMode?: RobotNotesMode | null
  runAtDate: string
  runAtTime: string
  runDaily: boolean
  executionMode?: RobotExecutionMode | null
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
