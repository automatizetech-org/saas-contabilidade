import { supabase } from "./supabaseClient"
import type { RobotExecutionMode, RobotNotesMode, Tables } from "@/types/database"

export type ExecutionRequest = Tables<"execution_requests">

export async function createExecutionRequest(params: {
  companyIds: string[]
  robotTechnicalIds: string[]
  periodStart?: string | null
  periodEnd?: string | null
  notesMode?: RobotNotesMode | null
  scheduleRuleId?: string | null
  executionMode?: RobotExecutionMode | null
  executionGroupId?: string | null
  executionOrder?: number | null
}): Promise<ExecutionRequest> {
  const { data: { user } } = await supabase.auth.getUser()
  for (const technicalId of params.robotTechnicalIds) {
    const deleteQuery = supabase
      .from("execution_requests")
      .delete()
      .eq("status", "pending")
      .contains("robot_technical_ids", [technicalId])

    const { error: deleteError } = await deleteQuery
    if (deleteError) throw deleteError
  }

  const row: Record<string, unknown> = {
    company_ids: params.companyIds,
    robot_technical_ids: params.robotTechnicalIds,
    period_start: params.periodStart ?? null,
    period_end: params.periodEnd ?? null,
    notes_mode: params.notesMode ?? null,
    execution_mode: params.executionMode ?? "sequential",
    execution_group_id: params.executionGroupId ?? null,
    execution_order: params.executionOrder ?? null,
    status: "pending",
    created_by: user?.id ?? null,
  }
  if (params.scheduleRuleId != null) row.schedule_rule_id = params.scheduleRuleId
  const { data, error } = await supabase
    .from("execution_requests")
    .insert(row)
    .select()
    .single()
  if (error) throw error
  return data as ExecutionRequest
}

export async function getRecentExecutionRequests(limit = 20): Promise<ExecutionRequest[]> {
  const { data, error } = await supabase
    .from("execution_requests")
    .select("*")
    .order("created_at", { ascending: false })
    .limit(limit)
  if (error) throw error
  return (data ?? []) as ExecutionRequest[]
}

/** Execuções com status running; opcionalmente filtradas por regra de agendamento. */
export async function getRunningExecutionRequests(scheduleRuleId?: string | null): Promise<ExecutionRequest[]> {
  let q = supabase
    .from("execution_requests")
    .select("*")
    .eq("status", "running")
  if (scheduleRuleId) {
    q = q.eq("schedule_rule_id", scheduleRuleId)
  }
  const { data, error } = await q
  if (error) throw error
  return (data ?? []) as ExecutionRequest[]
}

/** Pending ou running da regra: usado para esconder o contador até todos os robôs terminarem. */
export async function getPendingOrRunningExecutionRequests(scheduleRuleId: string | null): Promise<ExecutionRequest[]> {
  if (!scheduleRuleId) return []
  const { data, error } = await supabase
    .from("execution_requests")
    .select("*")
    .eq("schedule_rule_id", scheduleRuleId)
    .in("status", ["pending", "running"])
  if (error) throw error
  return (data ?? []) as ExecutionRequest[]
}

/** Cancela (remove) todos os jobs pendentes da regra de agendamento. Usado ao parar o agendamento. */
export async function cancelPendingByScheduleRuleId(scheduleRuleId: string): Promise<void> {
  const { error } = await supabase
    .from("execution_requests")
    .delete()
    .eq("schedule_rule_id", scheduleRuleId)
    .eq("status", "pending")
  if (error) throw error
}

/** Marca como falha os jobs em execução da regra (para o painel sair de "Executando agora" ao parar). */
export async function markRunningAsCancelledByScheduleRuleId(scheduleRuleId: string): Promise<void> {
  const { error } = await supabase
    .from("execution_requests")
    .update({
      status: "failed",
      error_message: "Cancelado ao parar agendamento",
    })
    .eq("schedule_rule_id", scheduleRuleId)
    .eq("status", "running")
  if (error) throw error
}

export async function cancelPendingByRobotTechnicalIds(robotTechnicalIds: string[]): Promise<void> {
  for (const technicalId of robotTechnicalIds) {
    const { error } = await supabase
      .from("execution_requests")
      .delete()
      .eq("status", "pending")
      .contains("robot_technical_ids", [technicalId])
    if (error) throw error
  }
}

export async function markRunningAsCancelledByRobotTechnicalIds(robotTechnicalIds: string[]): Promise<void> {
  for (const technicalId of robotTechnicalIds) {
    const { error } = await supabase
      .from("execution_requests")
      .update({
        status: "failed",
        error_message: "Cancelado ao parar agendamento",
      })
      .eq("status", "running")
      .contains("robot_technical_ids", [technicalId])
    if (error) throw error
  }
}

export async function deleteAllByScheduleRuleId(scheduleRuleId: string): Promise<void> {
  const { error } = await supabase
    .from("execution_requests")
    .delete()
    .eq("schedule_rule_id", scheduleRuleId)
  if (error) throw error
}

export async function deleteAllByRobotTechnicalIds(robotTechnicalIds: string[]): Promise<void> {
  for (const technicalId of robotTechnicalIds) {
    const { error } = await supabase
      .from("execution_requests")
      .delete()
      .contains("robot_technical_ids", [technicalId])
    if (error) throw error
  }
}
