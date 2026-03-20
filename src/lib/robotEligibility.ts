import { onlyDigits } from "@/lib/brazilDocuments"
import type { CompanyRobotConfig } from "@/services/companiesService"
import type { Robot } from "@/services/robotsService"
import type { Json } from "@/types/database"

type CompanyLike = {
  id: string
  name?: string | null
  active?: boolean | null
  document?: string | null
  auth_mode?: string | null
  cert_blob_b64?: string | null
  cert_password?: string | null
  cert_valid_until?: string | null
  contador_cpf?: string | null
  state_registration?: string | null
  city_name?: string | null
  cae?: string | null
  sefaz_go_logins?: Json
}

type LoginRow = {
  cpf: string
  password: string
  is_default: boolean
}

function asObject(value: Json | null | undefined): Record<string, Json> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {}
  return value as Record<string, Json>
}

function asArray(value: Json | null | undefined): Json[] {
  return Array.isArray(value) ? value : []
}

function normalizeCityName(value: string | null | undefined) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .trim()
    .toLowerCase()
}

function normalizeLoginRows(value: Json | null | undefined): LoginRow[] {
  const seen = new Set<string>()
  return asArray(value)
    .map((item) => {
      if (!item || typeof item !== "object" || Array.isArray(item)) return null
      const row = item as Record<string, Json>
      const cpf = onlyDigits(String(row.cpf ?? ""))
      const password = String(row.password ?? "").trim()
      if (!cpf || !password) return null
      return {
        cpf,
        password,
        is_default: Boolean(row.is_default),
      } satisfies LoginRow
    })
    .filter((item): item is LoginRow => Boolean(item))
    .filter((item) => {
      if (seen.has(item.cpf)) return false
      seen.add(item.cpf)
      return true
    })
}

function getCompanySettings(config: CompanyRobotConfig | null | undefined): Record<string, Json> {
  const settings = asObject(config?.settings)
  return {
    ...settings,
    auth_mode: config?.auth_mode ?? settings.auth_mode ?? "password",
    nfs_password:
      (config?.auth_mode ?? settings.auth_mode ?? "password") === "password"
        ? (config?.nfs_password ?? settings.nfs_password ?? null)
        : null,
    selected_login_cpf: config?.selected_login_cpf ?? settings.selected_login_cpf ?? null,
  }
}

function hasActiveCertificate(company: CompanyLike): boolean {
  if (!String(company.cert_blob_b64 ?? "").trim()) return false
  if (!String(company.cert_password ?? "").trim()) return false
  const validUntil = String(company.cert_valid_until ?? "").trim()
  if (!validUntil) return true
  const expiresAt = new Date(validUntil)
  if (Number.isNaN(expiresAt.getTime())) return true
  const today = new Date()
  today.setHours(0, 0, 0, 0)
  return expiresAt.getTime() >= today.getTime()
}

function hasEligiblePortalLogin(
  robot: Robot,
  company: CompanyLike,
  config: CompanyRobotConfig | null | undefined,
  routing: "match_selected_or_accountant" | "any_available" = "match_selected_or_accountant",
): boolean {
  const settings = getCompanySettings(config)
  const selectedLoginCpf = onlyDigits(String(settings.selected_login_cpf ?? ""))
  const contadorCpf = onlyDigits(String(company.contador_cpf ?? ""))
  const availableLogins = [
    ...normalizeLoginRows(robot.global_logins),
    ...normalizeLoginRows(company.sefaz_go_logins ?? null),
  ]

  if (availableLogins.length === 0) return false
  if (routing === "any_available") return true
  const hasCpf = (cpf: string) => availableLogins.some((item) => item.cpf === cpf)
  if (selectedLoginCpf) return hasCpf(selectedLoginCpf)
  if (contadorCpf) return hasCpf(contadorCpf)
  return true
}

function getCapabilityBoolean(robot: Robot, key: string): boolean | null {
  const capabilities = asObject(robot.capabilities)
  const value = capabilities[key]
  return typeof value === "boolean" ? value : null
}

function getCapabilityStringList(robot: Robot, key: string): string[] {
  const capabilities = asObject(robot.capabilities)
  return asArray(capabilities[key])
    .map((value) => String(value ?? "").trim())
    .filter(Boolean)
}

type EligibilityPolicy = {
  requireEnabledConfig: boolean
  requireDocument: boolean
  requireStateRegistration: boolean
  requireCae: boolean
  requireAnyLoginSource: boolean
  loginRouting: "match_selected_or_accountant" | "any_available"
  authBehavior: "choice" | "login_only" | "cnpj_only"
}

export type RobotEligibilityIssue = {
  companyId: string
  companyName: string
  reason: string
}

function getEligibilityPolicy(robot: Robot): EligibilityPolicy {
  const capabilities = asObject(robot.capabilities)
  const authBehaviorRaw = String(capabilities.auth_behavior ?? "").trim()
  const fallbackAuthBehavior =
    robot.technical_id === "nfs_padrao"
      ? "choice"
      : robot.technical_id === "sefaz_xml"
        ? "login_only"
        : "cnpj_only"

  const policy: EligibilityPolicy = {
    requireEnabledConfig: getCapabilityBoolean(robot, "require_enabled_config") ?? true,
    requireDocument: getCapabilityBoolean(robot, "require_document") ?? false,
    requireStateRegistration: getCapabilityBoolean(robot, "require_state_registration") ?? false,
    requireCae: getCapabilityBoolean(robot, "require_cae") ?? false,
    requireAnyLoginSource: getCapabilityBoolean(robot, "require_any_login_source") ?? false,
    loginRouting:
      String(capabilities.login_routing ?? "").trim().toLowerCase() === "any_available"
        ? "any_available"
        : "match_selected_or_accountant",
    authBehavior:
      authBehaviorRaw === "choice" || authBehaviorRaw === "login_only" || authBehaviorRaw === "cnpj_only"
        ? authBehaviorRaw
        : fallbackAuthBehavior,
  }

  for (const field of getCapabilityStringList(robot, "required_company_fields")) {
    if (field === "document") policy.requireDocument = true
    if (field === "state_registration") policy.requireStateRegistration = true
    if (field === "cae") policy.requireCae = true
  }

  if (robot.technical_id === "nfs_padrao") {
    policy.requireEnabledConfig = true
    policy.authBehavior = "choice"
  } else if (robot.technical_id === "sefaz_xml") {
    policy.requireEnabledConfig = false
    policy.requireStateRegistration = true
    policy.requireAnyLoginSource = true
    policy.authBehavior = "login_only"
  } else if (robot.technical_id === "goiania_taxas_impostos") {
    policy.requireEnabledConfig = false
    policy.requireCae = true
    policy.requireAnyLoginSource = true
    policy.loginRouting = "any_available"
  } else if (robot.technical_id === "certidoes" || robot.technical_id === "certidoes_fiscal") {
    policy.requireEnabledConfig = false
    policy.requireDocument = true
  }

  return policy
}

export function indexCompanyRobotConfigs(rows: CompanyRobotConfig[]) {
  const map = new Map<string, Map<string, CompanyRobotConfig>>()
  for (const row of rows) {
    const byRobot = map.get(row.robot_technical_id) ?? new Map<string, CompanyRobotConfig>()
    byRobot.set(row.company_id, row)
    map.set(row.robot_technical_id, byRobot)
  }
  return map
}

export function getEligibleCompanyIdsForRobot(params: {
  robot: Robot
  selectedCompanyIds: string[]
  companies: CompanyLike[]
  companyConfigsByRobot?: Map<string, Map<string, CompanyRobotConfig>>
}): string[] {
  return getRobotEligibilityReport(params).eligibleCompanyIds
}

export function getRobotEligibilityReport(params: {
  robot: Robot
  selectedCompanyIds: string[]
  companies: CompanyLike[]
  companyConfigsByRobot?: Map<string, Map<string, CompanyRobotConfig>>
}): { eligibleCompanyIds: string[]; skipped: RobotEligibilityIssue[] } {
  const { robot, selectedCompanyIds, companies, companyConfigsByRobot } = params
  const policy = getEligibilityPolicy(robot)
  const cityFilter = normalizeCityName(
    String(asObject(robot.execution_defaults).city_name ?? "").trim() || null,
  )
  const companyById = new Map(companies.map((company) => [company.id, company]))
  const configByCompanyId = companyConfigsByRobot?.get(robot.technical_id) ?? new Map<string, CompanyRobotConfig>()
  const eligibleCompanyIds: string[] = []
  const skipped: RobotEligibilityIssue[] = []

  for (const companyId of selectedCompanyIds) {
    const company = companyById.get(companyId)
    const companyName = String(company?.name ?? companyId)
    if (!company?.active) {
      skipped.push({ companyId, companyName, reason: "empresa inativa" })
      continue
    }

    const config = configByCompanyId.get(companyId) ?? null
    const settings = getCompanySettings(config)

    if (policy.requireEnabledConfig && !config?.enabled) {
      skipped.push({ companyId, companyName, reason: "robô não habilitado para a empresa" })
      continue
    }
    if (cityFilter && normalizeCityName(company.city_name) !== cityFilter) {
      skipped.push({
        companyId,
        companyName,
        reason: `cidade diferente de ${String(asObject(robot.execution_defaults).city_name ?? "").trim() || "cidade configurada"}`,
      })
      continue
    }
    if (policy.requireDocument && !onlyDigits(company.document).trim()) {
      skipped.push({ companyId, companyName, reason: "CNPJ não preenchido" })
      continue
    }
    if (policy.requireStateRegistration && !onlyDigits(company.state_registration).trim()) {
      skipped.push({ companyId, companyName, reason: "IE não preenchida" })
      continue
    }
    if (policy.requireCae && !String(company.cae ?? "").trim()) {
      skipped.push({ companyId, companyName, reason: "CAE não preenchido" })
      continue
    }
    if (policy.requireAnyLoginSource && !hasEligiblePortalLogin(robot, company, config, policy.loginRouting)) {
      skipped.push({ companyId, companyName, reason: "sem login disponível para o robô" })
      continue
    }

    if (policy.authBehavior === "choice") {
      const authMode = String(settings.auth_mode ?? config?.auth_mode ?? "password").trim().toLowerCase()
      if (authMode === "certificate") {
        if (!hasActiveCertificate(company)) {
          skipped.push({ companyId, companyName, reason: "certificado digital ausente ou vencido" })
          continue
        }
      } else if (!String(settings.nfs_password ?? config?.nfs_password ?? "").trim()) {
        skipped.push({ companyId, companyName, reason: "senha do portal não preenchida" })
        continue
      }
    }

    eligibleCompanyIds.push(companyId)
  }

  return { eligibleCompanyIds, skipped }
}
