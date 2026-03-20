function onlyDigits(value) {
  return String(value ?? "").replace(/\D/g, "");
}

function coerceJsonObject(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function coerceJsonArray(value) {
  return Array.isArray(value) ? value : [];
}

export function normalizeCityName(value) {
  return String(value ?? "")
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .trim()
    .toLowerCase();
}

function normalizeLoginRows(value) {
  const seen = new Set();
  return coerceJsonArray(value)
    .map((item) => {
      if (!item || typeof item !== "object" || Array.isArray(item)) return null;
      const cpf = onlyDigits(item.cpf);
      const password = String(item.password ?? "").trim();
      if (!cpf || !password) return null;
      return {
        cpf,
        password,
        is_default: Boolean(item.is_default),
      };
    })
    .filter(Boolean)
    .filter((item) => {
      if (seen.has(item.cpf)) return false;
      seen.add(item.cpf);
      return true;
    });
}

function getCompanySettings(configRow) {
  const settings = coerceJsonObject(configRow?.settings);
  const authMode = configRow?.auth_mode || settings.auth_mode || "password";
  return {
    ...settings,
    auth_mode: authMode,
    nfs_password: authMode === "password" ? (configRow?.nfs_password ?? settings.nfs_password ?? null) : null,
    selected_login_cpf: configRow?.selected_login_cpf ?? settings.selected_login_cpf ?? null,
  };
}

function hasActiveCertificate(company) {
  if (!String(company?.cert_blob_b64 ?? "").trim()) return false;
  if (!String(company?.cert_password ?? "").trim()) return false;
  const validUntil = String(company?.cert_valid_until ?? "").trim();
  if (!validUntil) return true;
  const expiresAt = new Date(validUntil);
  if (Number.isNaN(expiresAt.getTime())) return true;
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return expiresAt.getTime() >= today.getTime();
}

function getCapabilityBoolean(robotRow, key) {
  const capabilities = coerceJsonObject(robotRow?.capabilities);
  return typeof capabilities[key] === "boolean" ? capabilities[key] : null;
}

function getCapabilityStringList(robotRow, key) {
  const capabilities = coerceJsonObject(robotRow?.capabilities);
  return coerceJsonArray(capabilities[key])
    .map((item) => String(item ?? "").trim())
    .filter(Boolean);
}

function getEligibilityPolicy(robotRow) {
  const capabilities = coerceJsonObject(robotRow?.capabilities);
  const authBehaviorRaw = String(capabilities.auth_behavior ?? "").trim();
  const fallbackAuthBehavior =
    robotRow?.technical_id === "nfs_padrao"
      ? "choice"
      : robotRow?.technical_id === "sefaz_xml"
        ? "login_only"
        : "cnpj_only";

  const policy = {
    requireEnabledConfig: getCapabilityBoolean(robotRow, "require_enabled_config") ?? true,
    requireDocument: getCapabilityBoolean(robotRow, "require_document") ?? false,
    requireStateRegistration: getCapabilityBoolean(robotRow, "require_state_registration") ?? false,
    requireCae: getCapabilityBoolean(robotRow, "require_cae") ?? false,
    requireAnyLoginSource: getCapabilityBoolean(robotRow, "require_any_login_source") ?? false,
    authBehavior:
      authBehaviorRaw === "choice" || authBehaviorRaw === "login_only" || authBehaviorRaw === "cnpj_only"
        ? authBehaviorRaw
        : fallbackAuthBehavior,
  };

  for (const field of getCapabilityStringList(robotRow, "required_company_fields")) {
    if (field === "document") policy.requireDocument = true;
    if (field === "state_registration") policy.requireStateRegistration = true;
    if (field === "cae") policy.requireCae = true;
  }

  if (robotRow?.technical_id === "nfs_padrao") {
    policy.requireEnabledConfig = true;
    policy.authBehavior = "choice";
  } else if (robotRow?.technical_id === "sefaz_xml") {
    policy.requireEnabledConfig = false;
    policy.requireStateRegistration = true;
    policy.requireAnyLoginSource = true;
    policy.authBehavior = "login_only";
  } else if (robotRow?.technical_id === "goiania_taxas_impostos") {
    policy.requireEnabledConfig = false;
    policy.requireCae = true;
    policy.requireAnyLoginSource = true;
  } else if (robotRow?.technical_id === "certidoes" || robotRow?.technical_id === "certidoes_fiscal") {
    policy.requireEnabledConfig = false;
    policy.requireDocument = true;
  }

  return policy;
}

function hasEligiblePortalLogin(robotRow, company, configRow) {
  const settings = getCompanySettings(configRow);
  const selectedLoginCpf = onlyDigits(settings.selected_login_cpf);
  const contadorCpf = onlyDigits(company?.contador_cpf);
  const availableLogins = [
    ...normalizeLoginRows(robotRow?.global_logins),
    ...normalizeLoginRows(company?.sefaz_go_logins),
  ];

  if (availableLogins.length === 0) return false;
  const hasCpf = (cpf) => availableLogins.some((item) => item.cpf === cpf);
  if (selectedLoginCpf) return hasCpf(selectedLoginCpf);
  if (contadorCpf) return hasCpf(contadorCpf);
  return true;
}

export function filterEligibleCompaniesForRobot({
  robotRow,
  companies,
  configByCompanyId,
  cityName = null,
}) {
  const policy = getEligibilityPolicy(robotRow);
  const normalizedCity = normalizeCityName(cityName);

  return (companies ?? []).filter((company) => {
    if (!company?.active) return false;
    const config = configByCompanyId?.get(company.company_id || company.id) ?? null;
    const settings = getCompanySettings(config);

    if (policy.requireEnabledConfig && !config?.enabled) return false;
    if (normalizedCity && normalizeCityName(company.city_name) !== normalizedCity) return false;
    if (policy.requireDocument && !onlyDigits(company.document).trim()) return false;
    if (policy.requireStateRegistration && !onlyDigits(company.state_registration).trim()) return false;
    if (policy.requireCae && !String(company.cae ?? "").trim()) return false;
    if (policy.requireAnyLoginSource && !hasEligiblePortalLogin(robotRow, company, config)) return false;

    if (policy.authBehavior === "choice") {
      const authMode = String(settings.auth_mode ?? config?.auth_mode ?? company.auth_mode ?? "password").trim().toLowerCase();
      if (authMode === "certificate") return hasActiveCertificate(company);
      return Boolean(String(settings.nfs_password ?? config?.nfs_password ?? "").trim());
    }

    return true;
  });
}

export function getRequestedCityNameFromJob(jobPayload, robotRow) {
  const settings = coerceJsonObject(jobPayload);
  return String(
    settings.city_name ??
    coerceJsonObject(settings.execution_defaults).city_name ??
    coerceJsonObject(robotRow?.execution_defaults).city_name ??
    "",
  ).trim();
}
