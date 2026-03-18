import { supabase } from "./supabaseClient"
import type { Company } from "./profilesService"
import type { Tables } from "@/types/database"

export const ROBOT_NFS_TECHNICAL_ID = "nfs_padrao"

export type CompanyRobotConfig = Tables<"company_robot_config">
export type CompanySefazLogin = {
  cpf: string
  password: string
  is_default?: boolean
}
export type RobotCompanyConfigInput = {
  enabled: boolean
  auth_mode: "password" | "certificate"
  nfs_password?: string | null
  selected_login_cpf?: string | null
}

export async function getCompanyRobotConfig(
  companyId: string,
  robotTechnicalId: string = ROBOT_NFS_TECHNICAL_ID
): Promise<CompanyRobotConfig | null> {
  const { data, error } = await supabase
    .from("company_robot_config")
    .select("*")
    .eq("company_id", companyId)
    .eq("robot_technical_id", robotTechnicalId)
    .maybeSingle()
  if (error) throw error
  return data as CompanyRobotConfig | null
}

export async function getCompanyRobotConfigs(companyId: string): Promise<CompanyRobotConfig[]> {
  const { data, error } = await supabase
    .from("company_robot_config")
    .select("*")
    .eq("company_id", companyId)
  if (error) throw error
  return (data ?? []) as CompanyRobotConfig[]
}

export async function upsertCompanyRobotConfig(
  companyId: string,
  robotTechnicalId: string,
  config: RobotCompanyConfigInput
) {
  const { data, error } = await supabase
    .from("company_robot_config")
    .upsert(
      {
        company_id: companyId,
        robot_technical_id: robotTechnicalId,
        enabled: config.enabled,
        auth_mode: config.auth_mode,
        nfs_password: config.auth_mode === "password" ? (config.nfs_password ?? null) : null,
        selected_login_cpf: config.selected_login_cpf ?? null,
      },
      { onConflict: "company_id,robot_technical_id" }
    )
    .select()
    .single()
  if (error) throw error
  return data as CompanyRobotConfig
}

export async function getCompaniesForUser(activeFilter?: "active" | "inactive" | "all") {
  let q = supabase.from("companies").select("*").order("name")
  if (activeFilter === "active") q = q.eq("active", true)
  else if (activeFilter === "inactive") q = q.eq("active", false)
  const { data, error } = await q
  if (error) throw error
  return (data ?? []) as Company[]
}

export async function createCompany(params: {
  name: string
  document?: string | null
  state_registration?: string | null
  state_code?: string | null
  city_name?: string | null
  cae?: string | null
  active?: boolean
  sefaz_go_logins?: CompanySefazLogin[]
  auth_mode?: "password" | "certificate" | null
  cert_blob_b64?: string | null
  cert_password?: string | null
  cert_valid_until?: string | null
  contador_nome?: string | null
  contador_cpf?: string | null
}) {
  const { data: { user } } = await supabase.auth.getUser()
  if (!user) throw new Error("Não autenticado")
  const { data, error } = await supabase
    .from("companies")
    .insert({
        name: params.name,
        document: params.document ?? null,
        state_registration: params.state_registration ?? null,
        state_code: params.state_code ?? null,
        city_name: params.city_name ?? null,
        cae: params.cae ?? null,
        active: params.active ?? true,
        sefaz_go_logins: params.sefaz_go_logins ?? [],
        auth_mode: params.auth_mode ?? null,
        cert_blob_b64: params.cert_blob_b64 ?? null,
      cert_password: params.cert_password ?? null,
      cert_valid_until: params.cert_valid_until ?? null,
      contador_nome: params.contador_nome ?? null,
      contador_cpf: params.contador_cpf ?? null,
      created_by: user.id,
    })
    .select()
    .single()
  if (error) throw error
  return data as Company
}

export async function updateCompany(
  id: string,
  updates: {
    name?: string
    document?: string | null
    state_registration?: string | null
    state_code?: string | null
    city_name?: string | null
    cae?: string | null
    sefaz_go_logins?: CompanySefazLogin[]
    active?: boolean
    auth_mode?: "password" | "certificate" | null
    cert_blob_b64?: string | null
    cert_password?: string | null
    cert_valid_until?: string | null
    contador_nome?: string | null
    contador_cpf?: string | null
  }
) {
  const { data, error } = await supabase
    .from("companies")
    .update(updates)
    .eq("id", id)
    .select()
    .single()
  if (error) throw error
  return data as Company
}
