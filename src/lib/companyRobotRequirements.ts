import type { RobotCompanyConfigInput } from "@/services/companiesService"

function onlyDigits(value: string) {
  return value.replace(/\D/g, "")
}

export function companyHasStateRegistration(stateRegistration?: string | null) {
  return onlyDigits(stateRegistration ?? "").length > 0
}

export function robotRequiresStateRegistration(robotTechnicalId: string) {
  return robotTechnicalId === "sefaz_xml"
}

export function canEnableRobotForCompany(robotTechnicalId: string, stateRegistration?: string | null) {
  if (!robotRequiresStateRegistration(robotTechnicalId)) return true
  return companyHasStateRegistration(stateRegistration)
}

export function getRobotEnableRequirementMessage(robotTechnicalId: string) {
  if (robotRequiresStateRegistration(robotTechnicalId)) {
    return "Este robô só pode ser ligado quando a empresa tiver inscrição estadual cadastrada."
  }
  return null
}

export function sanitizeRobotConfigForCompany(
  robotTechnicalId: string,
  config: RobotCompanyConfigInput,
  stateRegistration?: string | null
): RobotCompanyConfigInput {
  if (canEnableRobotForCompany(robotTechnicalId, stateRegistration)) return config
  return { ...config, enabled: false }
}
