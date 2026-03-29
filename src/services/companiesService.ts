import { supabase } from "./supabaseClient"
import { fetchAllPages } from "./supabasePagination"
import type { Company } from "./profilesService"
import type { Json, Tables } from "@/types/database"
import { isValidCnpj, isValidCpf, onlyDigits } from "@/lib/brazilDocuments"

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
  settings?: Json
}

function sanitizeCompanyLogins(logins: CompanySefazLogin[] | null | undefined): CompanySefazLogin[] {
  if (!Array.isArray(logins)) return []

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
      throw new Error("Um ou mais logins SEFAZ possuem CPF inválido.")
    }
    if (!login.password) {
      throw new Error("Todo login SEFAZ precisa ter senha preenchida.")
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

async function ensureCompanyDocumentAvailable(document: string, companyId?: string) {
  if (!document) return

  let query = supabase.from("companies").select("id").eq("document", document).limit(1)
  if (companyId) query = query.neq("id", companyId)

  const { data, error } = await query
  if (error) throw error
  if ((data ?? []).length > 0) {
    throw new Error("Já existe uma empresa cadastrada com este CNPJ neste escritório.")
  }
}

function sanitizeCompanyPayload(params: {
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
  const name = params.name.trim()
  if (!name) throw new Error("Informe o nome da empresa.")

  const document = onlyDigits(params.document)
  if (document && !isValidCnpj(document)) {
    throw new Error("Informe um CNPJ válido para a empresa.")
  }

  const contadorCpf = onlyDigits(params.contador_cpf)
  if (contadorCpf && !isValidCpf(contadorCpf)) {
    throw new Error("Informe um CPF válido para o contador responsável.")
  }

  return {
    name,
    document: document || null,
    state_registration: params.state_registration?.trim() || null,
    state_code: params.state_code?.trim() || null,
    city_name: params.city_name?.trim() || null,
    cae: params.cae?.trim() || null,
    active: params.active ?? true,
    sefaz_go_logins: sanitizeCompanyLogins(params.sefaz_go_logins),
    auth_mode: params.auth_mode ?? null,
    cert_blob_b64: params.cert_blob_b64 ?? null,
    cert_password: params.cert_password ?? null,
    cert_valid_until: params.cert_valid_until ?? null,
    contador_nome: params.contador_nome?.trim() || null,
    contador_cpf: contadorCpf || null,
  }
}

function asObject(value: Json | null | undefined): Record<string, Json> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {}
  return value as Record<string, Json>
}

function asArray(value: Json | null | undefined): Json[] {
  return Array.isArray(value) ? value : []
}

function companyHasCertificateDefaults(company: Pick<Company, "auth_mode" | "cert_blob_b64" | "cert_password">) {
  return (
    String(company.auth_mode ?? "").trim().toLowerCase() === "certificate" ||
    (String(company.cert_blob_b64 ?? "").trim().length > 0 && String(company.cert_password ?? "").trim().length > 0)
  )
}

function robotPrefersCertificateDefaults(
  robot: Pick<Tables<"robots">, "capabilities" | "company_form_schema">,
) {
  const capabilities = asObject(robot.capabilities)
  const authBehavior = String(capabilities.auth_behavior ?? "").trim().toLowerCase()
  const explicitDefault = String(
    capabilities.default_auth_mode ?? capabilities.preferred_auth_mode ?? "",
  )
    .trim()
    .toLowerCase()
  const hasAuthModeField = asArray(robot.company_form_schema).some((item) => {
    if (!item || typeof item !== "object" || Array.isArray(item)) return false
    const row = item as Record<string, Json>
    return (
      String(row.type ?? "").trim().toLowerCase() === "auth_mode" ||
      String(row.key ?? "").trim().toLowerCase() === "auth_mode"
    )
  })
  return authBehavior === "choice" || explicitDefault === "certificate" || hasAuthModeField
}

async function ensureDefaultCompanyRobotConfigs(company: Company) {
  const { data, error } = await supabase
    .from("robots")
    .select("technical_id, capabilities, company_form_schema")

  if (error) return

  const rows = ((data ?? []) as Array<Pick<Tables<"robots">, "technical_id" | "capabilities" | "company_form_schema">>)
    .map((robot) => {
      const authMode: "password" | "certificate" =
        robotPrefersCertificateDefaults(robot) && companyHasCertificateDefaults(company)
          ? "certificate"
          : "password"

      return {
        office_id: company.office_id,
        company_id: company.id,
        robot_technical_id: robot.technical_id,
        enabled: true,
        auth_mode: authMode,
        settings: { auth_mode: authMode } satisfies Record<string, Json>,
      }
    })

  if (rows.length === 0) return

  await supabase
    .from("company_robot_config")
    .upsert(rows, { onConflict: "company_id,robot_technical_id" })
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

export async function getCompanyRobotConfigsForSelection(params: {
  companyIds: string[]
  robotTechnicalIds?: string[]
}): Promise<CompanyRobotConfig[]> {
  const companyIds = Array.from(new Set(params.companyIds.map((id) => String(id).trim()).filter(Boolean)))
  if (companyIds.length === 0) return []

  let query = supabase
    .from("company_robot_config")
    .select("*")
    .in("company_id", companyIds)

  const robotTechnicalIds = Array.from(
    new Set((params.robotTechnicalIds ?? []).map((id) => String(id).trim()).filter(Boolean)),
  )
  if (robotTechnicalIds.length > 0) {
    query = query.in("robot_technical_id", robotTechnicalIds)
  }

  const { data, error } = await query
  if (error) throw error
  return (data ?? []) as CompanyRobotConfig[]
}

export async function upsertCompanyRobotConfig(
  companyId: string,
  robotTechnicalId: string,
  config: RobotCompanyConfigInput
) {
  const selectedLoginCpf = onlyDigits(config.selected_login_cpf)
  if (selectedLoginCpf && !isValidCpf(selectedLoginCpf)) {
    throw new Error("Informe um CPF válido para o login vinculado do robô.")
  }

  const settings = {
    ...(typeof config.settings === "object" && config.settings && !Array.isArray(config.settings) ? config.settings : {}),
    auth_mode: config.auth_mode,
    nfs_password: config.auth_mode === "password" ? (config.nfs_password ?? null) : null,
    selected_login_cpf: selectedLoginCpf || null,
  } satisfies Record<string, Json | undefined>

  const { data, error } = await supabase
    .from("company_robot_config")
    .upsert(
      {
        company_id: companyId,
        robot_technical_id: robotTechnicalId,
        enabled: config.enabled,
        auth_mode: config.auth_mode,
        nfs_password: config.auth_mode === "password" ? (config.nfs_password ?? null) : null,
        selected_login_cpf: selectedLoginCpf || null,
        settings,
      },
      { onConflict: "company_id,robot_technical_id" }
    )
    .select()
    .single()
  if (error) throw error
  return data as CompanyRobotConfig
}

export async function getCompaniesForUser(activeFilter?: "active" | "inactive" | "all") {
  const data = await fetchAllPages<Company>((from, to) => {
    let q = supabase.from("companies").select("*").order("name").range(from, to)
    if (activeFilter === "active") q = q.eq("active", true)
    else if (activeFilter === "inactive") q = q.eq("active", false)
    return q
  })
  return data
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
  const {
    data: { user },
  } = await supabase.auth.getUser()
  if (!user) throw new Error("Não autenticado")

  const payload = sanitizeCompanyPayload(params)
  if (payload.document) {
    await ensureCompanyDocumentAvailable(payload.document)
  }

  const { data, error } = await supabase
    .from("companies")
    .insert({
      ...payload,
      created_by: user.id,
    })
    .select()
    .single()
  if (error) throw error
  const company = data as Company
  await ensureDefaultCompanyRobotConfigs(company)
  return company
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
  const payload: Record<string, unknown> = {}

  if (updates.name !== undefined) {
    const name = updates.name.trim()
    if (!name) throw new Error("Informe o nome da empresa.")
    payload.name = name
  }

  if (updates.document !== undefined) {
    const document = onlyDigits(updates.document)
    if (document && !isValidCnpj(document)) {
      throw new Error("Informe um CNPJ válido para a empresa.")
    }
    if (document) {
      await ensureCompanyDocumentAvailable(document, id)
    }
    payload.document = document || null
  }

  if (updates.state_registration !== undefined) payload.state_registration = updates.state_registration?.trim() || null
  if (updates.state_code !== undefined) payload.state_code = updates.state_code?.trim() || null
  if (updates.city_name !== undefined) payload.city_name = updates.city_name?.trim() || null
  if (updates.cae !== undefined) payload.cae = updates.cae?.trim() || null
  if (updates.sefaz_go_logins !== undefined) payload.sefaz_go_logins = sanitizeCompanyLogins(updates.sefaz_go_logins)
  if (updates.active !== undefined) payload.active = updates.active
  if (updates.auth_mode !== undefined) payload.auth_mode = updates.auth_mode ?? null
  if (updates.cert_blob_b64 !== undefined) payload.cert_blob_b64 = updates.cert_blob_b64 ?? null
  if (updates.cert_password !== undefined) payload.cert_password = updates.cert_password ?? null
  if (updates.cert_valid_until !== undefined) payload.cert_valid_until = updates.cert_valid_until ?? null
  if (updates.contador_nome !== undefined) payload.contador_nome = updates.contador_nome?.trim() || null
  if (updates.contador_cpf !== undefined) {
    const contadorCpf = onlyDigits(updates.contador_cpf)
    if (contadorCpf && !isValidCpf(contadorCpf)) {
      throw new Error("Informe um CPF válido para o contador responsável.")
    }
    payload.contador_cpf = contadorCpf || null
  }

  const { data, error } = await supabase
    .from("companies")
    .update(payload)
    .eq("id", id)
    .select()
    .single()
  if (error) throw error
  return data as Company
}

const BATCH_CONTADOR_CHUNK = 200

/** Atualiza contador responsável de várias empresas em poucas requisições (`.in("id", …)` por lote). */
export async function updateCompaniesContadorBatch(
  companyIds: string[],
  contador_cpf: string,
  contador_nome: string
): Promise<void> {
  if (companyIds.length === 0) return

  const contadorCpf = onlyDigits(contador_cpf)
  if (contadorCpf && !isValidCpf(contadorCpf)) {
    throw new Error("Informe um CPF válido para o contador responsável.")
  }

  const payload = {
    contador_cpf: contadorCpf || null,
    contador_nome: contador_nome.trim() || null,
  }

  const chunks: string[][] = []
  for (let i = 0; i < companyIds.length; i += BATCH_CONTADOR_CHUNK) {
    chunks.push(companyIds.slice(i, i + BATCH_CONTADOR_CHUNK))
  }

  const results = await Promise.all(
    chunks.map((ids) => supabase.from("companies").update(payload).in("id", ids))
  )
  for (const { error } of results) {
    if (error) throw error
  }
}

export async function deleteCompany(id: string): Promise<void> {
  const { error } = await supabase.from("companies").delete().eq("id", id)
  if (error) throw error
}

const BATCH_DELETE_CHUNK = 200

export async function deleteCompaniesBatch(companyIds: string[]): Promise<void> {
  if (companyIds.length === 0) return
  const chunks: string[][] = []
  for (let i = 0; i < companyIds.length; i += BATCH_DELETE_CHUNK) {
    chunks.push(companyIds.slice(i, i + BATCH_DELETE_CHUNK))
  }
  const results = await Promise.all(chunks.map((ids) => supabase.from("companies").delete().in("id", ids)))
  for (const { error } of results) {
    if (error) throw error
  }
}
