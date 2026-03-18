/**
 * Configurações do painel admin (ex.: retenção de arquivos).
 * Apenas super_admin pode ler/escrever (RLS no Supabase).
 */

import { supabase } from "./supabaseClient"
import { getCurrentOfficeContext } from "./officeContextService"

const KEY_FILE_RETENTION_DAYS = "file_retention_days"
const KEY_BASE_PATH = "base_path"
const KEY_ROBOT_GOIANIA_SKIP_ISS = "robot_goiania_skip_iss"

/** 0 = nunca excluir; 30, 60, 90, 120 = dias desde o último download */
export type FileRetentionDays = 0 | 30 | 60 | 90 | 120

/** Pasta base na VM (ex.: C:\Users\ROBO\Documents). Robôs e server-api usam essa raiz para EMPRESAS/... */
export async function getBasePath(): Promise<string> {
  const { data, error } = await supabase
    .from("admin_settings")
    .select("value")
    .eq("key", KEY_BASE_PATH)
    .maybeSingle()
  if (error) throw error
  const v = (data?.value ?? "").trim()
  return v || ""
}

export async function setBasePath(value: string): Promise<void> {
  const context = await getCurrentOfficeContext()
  if (!context?.officeId) throw new Error("Nenhum escritório ativo encontrado.")
  const { error } = await supabase
    .from("admin_settings")
    .upsert(
      { office_id: context.officeId, key: KEY_BASE_PATH, value: (value || "").trim(), updated_at: new Date().toISOString() },
      { onConflict: "office_id,key" }
    )
  if (error) throw error
}

export async function getFileRetentionDays(): Promise<FileRetentionDays> {
  const { data, error } = await supabase
    .from("admin_settings")
    .select("value")
    .eq("key", KEY_FILE_RETENTION_DAYS)
    .maybeSingle()
  if (error) throw error
  const v = parseInt(String(data?.value ?? "60"), 10)
  if ([0, 30, 60, 90, 120].includes(v)) return v as FileRetentionDays
  return 60
}

export async function setFileRetentionDays(days: FileRetentionDays): Promise<void> {
  const context = await getCurrentOfficeContext()
  if (!context?.officeId) throw new Error("Nenhum escritório ativo encontrado.")
  const { error } = await supabase
    .from("admin_settings")
    .upsert(
      { office_id: context.officeId, key: KEY_FILE_RETENTION_DAYS, value: String(days), updated_at: new Date().toISOString() },
      { onConflict: "office_id,key" }
    )
  if (error) throw error
}

/** Robô Goiânia Taxas e Impostos: se true, não captura nem seleciona débitos de ISS. */
export async function getRobotGoianiaSkipIss(): Promise<boolean> {
  const { data, error } = await supabase
    .from("admin_settings")
    .select("value")
    .eq("key", KEY_ROBOT_GOIANIA_SKIP_ISS)
    .maybeSingle()
  if (error) throw error
  return String(data?.value ?? "").trim().toLowerCase() === "true"
}

export async function setRobotGoianiaSkipIss(skip: boolean): Promise<void> {
  const context = await getCurrentOfficeContext()
  if (!context?.officeId) throw new Error("Nenhum escritório ativo encontrado.")
  const { error } = await supabase
    .from("admin_settings")
    .upsert(
      { office_id: context.officeId, key: KEY_ROBOT_GOIANIA_SKIP_ISS, value: skip ? "true" : "false", updated_at: new Date().toISOString() },
      { onConflict: "office_id,key" }
    )
  if (error) throw error
}

/**
 * Exclui registros de fiscal_documents cujo último download foi há mais de N dias.
 * Só exclui onde last_downloaded_at está preenchido e é anterior ao corte.
 * Retorna quantos foram excluídos.
 */
export async function runFileRetentionCleanup(): Promise<{ deleted: number }> {
  const days = await getFileRetentionDays()
  if (days === 0) return { deleted: 0 }
  const cutoff = new Date()
  cutoff.setDate(cutoff.getDate() - days)
  const cutoffIso = cutoff.toISOString()
  const { data: toDelete, error: selectError } = await supabase
    .from("fiscal_documents")
    .select("id")
    .not("last_downloaded_at", "is", null)
    .lt("last_downloaded_at", cutoffIso)
  if (selectError) throw selectError
  const ids = (toDelete ?? []).map((r) => r.id)
  if (ids.length === 0) return { deleted: 0 }
  const { error: deleteError } = await supabase.from("fiscal_documents").delete().in("id", ids)
  if (deleteError) throw deleteError
  return { deleted: ids.length }
}
