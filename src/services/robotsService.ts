import { supabase } from "./supabaseClient"
import type { FiscalNotesKind, RobotNotesMode, Tables } from "@/types/database"
import type { CompanySefazLogin } from "./companiesService"
import { isValidCpf, onlyDigits } from "@/lib/brazilDocuments"

export type Robot = Tables<"robots">
export type RobotStatus = Robot["status"]

function sanitizeGlobalLogins(logins: CompanySefazLogin[] | undefined) {
  if (!Array.isArray(logins)) return undefined

  const seen = new Set<string>()
  const cleaned = logins
    .map((login) => ({
      cpf: onlyDigits(login.cpf),
      password: String(login.password ?? "").trim(),
      is_default: Boolean(login.is_default),
    }))
    .filter((login) => login.cpf || login.password)

  for (const login of cleaned) {
    if (!isValidCpf(login.cpf)) {
      throw new Error("Informe um CPF valido para os logins globais do robo.")
    }
    if (!login.password) {
      throw new Error("Todo login global do robo precisa ter senha preenchida.")
    }
  }

  const deduped = cleaned.filter((login) => {
    if (seen.has(login.cpf)) return false
    seen.add(login.cpf)
    return true
  })

  if (deduped.length === 0) return []

  const defaultIndex = deduped.findIndex((login) => login.is_default)
  return deduped.map((login, index) => ({
    ...login,
    is_default: defaultIndex === -1 ? index === 0 : index === defaultIndex,
  }))
}

export async function getRobots(): Promise<Robot[]> {
  const { data, error } = await supabase
    .from("robots")
    .select("*")
    .order("display_name")
  if (error) throw error
  return (data ?? []) as Robot[]
}

export async function updateRobotDisplayName(
  id: string,
  displayName: string
): Promise<Robot> {
  const { data, error } = await supabase
    .from("robots")
    .update({ display_name: displayName.trim() })
    .eq("id", id)
    .select()
    .single()
  if (error) throw error
  return data as Robot
}

export async function updateRobot(
  id: string,
  updates: {
    display_name?: string
    segment_path?: string | null
    is_fiscal_notes_robot?: boolean
    fiscal_notes_kind?: FiscalNotesKind | null
    notes_mode?: RobotNotesMode | null
    date_execution_mode?: "competencia" | "interval" | null
    initial_period_start?: string | null
    initial_period_end?: string | null
    last_period_end?: string | null
    global_logins?: CompanySefazLogin[]
  }
): Promise<Robot> {
  const sanitizedUpdates: Record<string, unknown> = {
    ...(updates as Record<string, unknown>),
  }
  if (updates.global_logins !== undefined) {
    sanitizedUpdates.global_logins = sanitizeGlobalLogins(updates.global_logins)
  }

  const getErrorText = (error: unknown) => {
    if (error instanceof Error) return error.message
    if (error && typeof error === "object") {
      const record = error as Record<string, unknown>
      return [
        record.message,
        record.details,
        record.hint,
        record.code,
        JSON.stringify(record),
      ]
        .filter(Boolean)
        .join(" ")
    }
    return String(error)
  }

  const runUpdate = async (payload: Record<string, unknown>) => {
    const { data, error } = await supabase
      .from("robots")
      .update(payload)
      .eq("id", id)
      .select()
      .single()
    if (error) throw error
    return data as Robot
  }

  try {
    return await runUpdate(sanitizedUpdates)
  } catch (error) {
    const message = getErrorText(error)
    const missingNewFields =
      message.includes("is_fiscal_notes_robot") ||
      message.includes("fiscal_notes_kind") ||
      message.includes("PGRST204")
    const notesModeConstraint =
      message.includes("robots_notes_mode_check") ||
      message.includes("notes_mode") ||
      message.includes("23514")

    if (notesModeConstraint) {
      const fallbackUpdates = { ...sanitizedUpdates }
      delete fallbackUpdates.notes_mode
      try {
        return await runUpdate(fallbackUpdates)
      } catch (fallbackError) {
        const fallbackMessage = getErrorText(fallbackError)
        const fallbackMissingNewFields =
          fallbackMessage.includes("is_fiscal_notes_robot") ||
          fallbackMessage.includes("fiscal_notes_kind") ||
          fallbackMessage.includes("PGRST204")
        if (!fallbackMissingNewFields) throw fallbackError

        delete fallbackUpdates.is_fiscal_notes_robot
        delete fallbackUpdates.fiscal_notes_kind
        return await runUpdate(fallbackUpdates)
      }
    }

    if (!missingNewFields) throw error

    const fallbackUpdates = { ...sanitizedUpdates }
    delete fallbackUpdates.is_fiscal_notes_robot
    delete fallbackUpdates.fiscal_notes_kind

    return await runUpdate(fallbackUpdates)
  }
}
