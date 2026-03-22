import type { RobotCompanyConfigInput } from "@/services/companiesService"
import type { Robot } from "@/services/robotsService"
import type { Json } from "@/types/database"

function onlyDigits(value: string) {
  return value.replace(/\D/g, "")
}

function asObject(value: Json | null | undefined): Record<string, Json> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {}
  return value as Record<string, Json>
}

function asArray(value: Json | null | undefined): Json[] {
  return Array.isArray(value) ? value : []
}

export function companyHasStateRegistration(stateRegistration?: string | null) {
  return onlyDigits(stateRegistration ?? "").length > 0
}

function companyHasDocument(document?: string | null) {
  return onlyDigits(document ?? "").length > 0
}

function companyHasCae(cae?: string | null) {
  return String(cae ?? "").trim().length > 0
}

type RobotRequirementTarget =
  | Pick<Robot, "technical_id" | "capabilities">
  | string

type CompanyRequirementInput = {
  stateRegistration?: string | null
  document?: string | null
  cae?: string | null
}

function getRequiredCompanyFields(robot: RobotRequirementTarget): string[] {
  if (typeof robot === "string") {
    return robot === "sefaz_xml" ? ["state_registration"] : []
  }

  const capabilities = asObject(robot.capabilities)
  const requiredFields = asArray(capabilities.required_company_fields)
    .map((value) => String(value ?? "").trim())
    .filter(Boolean)

  if (requiredFields.length > 0) return requiredFields
  if (getCapabilityBoolean(robot, "require_state_registration")) return ["state_registration"]
  if (getCapabilityBoolean(robot, "require_document")) return ["document"]
  if (getCapabilityBoolean(robot, "require_cae")) return ["cae"]
  if (robot.technical_id === "sefaz_xml") return ["state_registration"]
  return []
}

function getCapabilityBoolean(robot: Pick<Robot, "capabilities">, key: string): boolean {
  const capabilities = asObject(robot.capabilities)
  return capabilities[key] === true
}

function getMissingRequiredCompanyFields(
  robot: RobotRequirementTarget,
  company: CompanyRequirementInput = {},
): string[] {
  const requiredFields = getRequiredCompanyFields(robot)
  return requiredFields.filter((field) => {
    if (field === "state_registration") return !companyHasStateRegistration(company.stateRegistration)
    if (field === "document") return !companyHasDocument(company.document)
    if (field === "cae") return !companyHasCae(company.cae)
    return false
  })
}

export function canEnableRobotForCompany(
  robot: RobotRequirementTarget,
  company: CompanyRequirementInput = {},
) {
  return getMissingRequiredCompanyFields(robot, company).length === 0
}

export function getRobotEnableRequirementMessage(robot: RobotRequirementTarget) {
  const missingFields = getRequiredCompanyFields(robot)
  if (missingFields.includes("state_registration")) {
    return "Este robô só pode ser ligado quando a empresa tiver inscrição estadual cadastrada."
  }
  if (missingFields.includes("document")) {
    return "Este robô só pode ser ligado quando a empresa tiver CNPJ cadastrado."
  }
  if (missingFields.includes("cae")) {
    return "Este robô só pode ser ligado quando a empresa tiver CAE cadastrado."
  }
  return null
}

export function sanitizeRobotConfigForCompany(
  robot: RobotRequirementTarget,
  config: RobotCompanyConfigInput,
  company: CompanyRequirementInput = {},
): RobotCompanyConfigInput {
  if (canEnableRobotForCompany(robot, company)) return config
  return { ...config, enabled: false }
}
