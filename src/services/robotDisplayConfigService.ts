import { supabase } from "./supabaseClient"
import type { RobotNotesMode, Tables } from "@/types/database"

export type RobotDisplayConfig = Tables<"robot_display_config">

const ROBOT_NFS_ID = "nfs_padrao"

export async function getRobotDisplayConfig(
  robotTechnicalId: string = ROBOT_NFS_ID
): Promise<RobotDisplayConfig | null> {
  const { data, error } = await supabase
    .from("robot_display_config")
    .select("*")
    .eq("robot_technical_id", robotTechnicalId)
    .maybeSingle()
  if (error) throw error
  return data as RobotDisplayConfig | null
}

export async function upsertRobotDisplayConfig(params: {
  robotTechnicalId: string
  companyIds: string[]
  periodStart?: string | null
  periodEnd?: string | null
  notesMode?: RobotNotesMode | null
}): Promise<RobotDisplayConfig> {
  const payload = {
    robot_technical_id: params.robotTechnicalId,
    company_ids: params.companyIds,
    period_start: params.periodStart ?? null,
    period_end: params.periodEnd ?? null,
    notes_mode: params.notesMode ?? null,
    updated_at: new Date().toISOString(),
  }
  const payloadWithoutNotesMode = {
    robot_technical_id: payload.robot_technical_id,
    company_ids: payload.company_ids,
    period_start: payload.period_start,
    period_end: payload.period_end,
    updated_at: payload.updated_at,
  }

  const existing = await getRobotDisplayConfig(params.robotTechnicalId)

  if (existing) {
    try {
      const { data, error } = await supabase
        .from("robot_display_config")
        .update({
          company_ids: payload.company_ids,
          period_start: payload.period_start,
          period_end: payload.period_end,
          notes_mode: payload.notes_mode,
          updated_at: payload.updated_at,
        })
        .eq("robot_technical_id", params.robotTechnicalId)
        .select()
        .single()
      if (error) throw error
      return data as RobotDisplayConfig
    } catch {
      const { data, error: fallbackError } = await supabase
        .from("robot_display_config")
        .update(payloadWithoutNotesMode)
        .eq("robot_technical_id", params.robotTechnicalId)
        .select()
        .single()
      if (fallbackError) throw fallbackError
      return data as RobotDisplayConfig
    }
  }

  try {
    const { data, error } = await supabase
      .from("robot_display_config")
      .insert(payload)
      .select()
      .single()
    if (error) throw error
    return data as RobotDisplayConfig
  } catch {
    const { data, error: fallbackError } = await supabase
      .from("robot_display_config")
      .insert(payloadWithoutNotesMode)
      .select()
      .single()
    if (fallbackError) throw fallbackError
    return data as RobotDisplayConfig
  }
}

export async function upsertRobotDisplayConfigForRobots(
  robotTechnicalIds: string[],
  params: {
    companyIds: string[]
    periodStart?: string | null
    periodEnd?: string | null
    notesMode?: RobotNotesMode | null
  }
): Promise<void> {
  for (const id of robotTechnicalIds) {
    await upsertRobotDisplayConfig({
      robotTechnicalId: id,
      companyIds: params.companyIds,
      periodStart: params.periodStart,
      periodEnd: params.periodEnd,
      notesMode: params.notesMode,
    })
  }
}
