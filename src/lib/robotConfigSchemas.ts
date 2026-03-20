import type { CompanySefazLogin } from "@/services/companiesService";
import type { Robot } from "@/services/robotsService";
import type { Json } from "@/types/database";

export type RobotConfigTarget =
  | "admin_settings"
  | "execution_defaults"
  | "company_settings"
  | "global_logins";

export type RobotConfigFieldType =
  | "text"
  | "password"
  | "textarea"
  | "boolean"
  | "select"
  | "number"
  | "city_select"
  | "auth_mode"
  | "login_binding"
  | "login_list";

export type RobotConfigFieldOption = {
  label: string;
  value: string;
};

export type RobotConfigFieldSchema = {
  key: string;
  label: string;
  type: RobotConfigFieldType;
  target?: RobotConfigTarget;
  placeholder?: string;
  help_text?: string;
  required?: boolean;
  options?: RobotConfigFieldOption[];
  default_value?: Json;
  visible_when?: {
    key: string;
    equals?: Json;
    not_equals?: Json;
    target?: RobotConfigTarget;
  };
};

function asObject(value: Json | null | undefined): Record<string, Json> {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value as Record<string, Json>;
}

function asArray(value: Json | null | undefined): Json[] {
  return Array.isArray(value) ? value : [];
}

function normalizeFieldOption(value: Json): RobotConfigFieldOption | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const row = value as Record<string, Json>;
  const optionValue = String(row.value ?? "").trim();
  const label = String(row.label ?? optionValue).trim();
  if (!optionValue || !label) return null;
  return { label, value: optionValue };
}

function normalizeFieldSchema(value: Json): RobotConfigFieldSchema | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const row = value as Record<string, Json>;
  const key = String(row.key ?? "").trim();
  const label = String(row.label ?? "").trim();
  const type = String(row.type ?? "").trim() as RobotConfigFieldType;
  const allowedTypes: RobotConfigFieldType[] = [
    "text",
    "password",
    "textarea",
    "boolean",
    "select",
    "number",
    "city_select",
    "auth_mode",
    "login_binding",
    "login_list",
  ];

  if (!key || !label || !allowedTypes.includes(type)) return null;

  const target =
    (String(row.target ?? "").trim() as RobotConfigTarget) || "company_settings";
  const allowedTargets: RobotConfigTarget[] = [
    "admin_settings",
    "execution_defaults",
    "company_settings",
    "global_logins",
  ];

  const visibleWhenRaw =
    row.visible_when && typeof row.visible_when === "object" && !Array.isArray(row.visible_when)
      ? (row.visible_when as Record<string, Json>)
      : null;

  return {
    key,
    label,
    type,
    target: allowedTargets.includes(target) ? target : "company_settings",
    placeholder: String(row.placeholder ?? "").trim() || undefined,
    help_text: String(row.help_text ?? "").trim() || undefined,
    required: Boolean(row.required),
    options: asArray(row.options).map(normalizeFieldOption).filter((item): item is RobotConfigFieldOption => Boolean(item)),
    default_value: row.default_value,
    visible_when: visibleWhenRaw
      ? {
          key: String(visibleWhenRaw.key ?? "").trim(),
          equals: visibleWhenRaw.equals,
          not_equals: visibleWhenRaw.not_equals,
          target: (String(visibleWhenRaw.target ?? "").trim() as RobotConfigTarget) || undefined,
        }
      : undefined,
  };
}

export function getRobotCapabilities(robot: Robot) {
  const capabilities = asObject(robot.capabilities);
  const authBehavior = String(capabilities.auth_behavior ?? "").trim();

  if (authBehavior === "choice" || authBehavior === "login_only" || authBehavior === "cnpj_only") {
    return {
      authBehavior,
      usesLoginBinding: Boolean(capabilities.uses_login_binding),
      showsPasswordField: Boolean(capabilities.shows_password_field),
      helperText: String(capabilities.helper_text ?? "").trim(),
    } as const;
  }

  if (robot.technical_id === "nfs_padrao") {
    return {
      authBehavior: "choice",
      usesLoginBinding: false,
      showsPasswordField: true,
      helperText: "Este robô permite escolher entre login no portal e certificado digital.",
    } as const;
  }

  if (robot.technical_id === "sefaz_xml") {
    return {
      authBehavior: "login_only",
      usesLoginBinding: true,
      showsPasswordField: false,
      helperText: "Este robô usa login global por CPF, vinculado empresa por empresa.",
    } as const;
  }

  if (robot.technical_id === "certidoes_fiscal") {
    return {
      authBehavior: "cnpj_only",
      usesLoginBinding: false,
      showsPasswordField: false,
      helperText: "Este robô usa apenas o CNPJ e os dados da empresa para consultar.",
    } as const;
  }

  return {
    authBehavior: "cnpj_only",
    usesLoginBinding: false,
    showsPasswordField: false,
    helperText: "Este robô só exibe as opções necessárias para o modo de execução dele.",
  } as const;
}

function getFallbackAdminSchema(robot: Robot): RobotConfigFieldSchema[] {
  const fields: RobotConfigFieldSchema[] = [
    {
      key: "city_name",
      label: "Cidade filtrada",
      type: "city_select",
      target: "execution_defaults",
      help_text: "Se preenchido, o agendador e o job local enviam apenas empresas dessa cidade para este robô.",
    },
  ];

  if (robot.technical_id === "sefaz_xml") {
    fields.push({
      key: "global_logins",
      label: "Logins globais",
      type: "login_list",
      target: "global_logins",
      help_text: "Cadastre aqui os logins CPF/senha usados por este robô.",
    });
  }

  if (robot.technical_id === "goiania_taxas_impostos") {
    fields.push(
      {
        key: "global_logins",
        label: "Login da prefeitura",
        type: "login_list",
        target: "global_logins",
        help_text: "Cadastre o login CPF/senha do portal da Prefeitura de Goiânia.",
      },
      {
        key: "skip_iss",
        label: "Ignorar débitos de ISS",
        type: "boolean",
        target: "admin_settings",
        help_text: "Se marcado, o robô não captura débitos de ISS.",
      },
    );
  }

  return fields;
}

function getFallbackCompanySchema(robot: Robot): RobotConfigFieldSchema[] {
  const capabilities = getRobotCapabilities(robot);
  const fields: RobotConfigFieldSchema[] = [];

  if (capabilities.authBehavior === "choice") {
    fields.push({
      key: "auth_mode",
      label: "Modo de autenticação",
      type: "auth_mode",
      target: "company_settings",
      help_text: capabilities.helperText,
    });
  }

  if (capabilities.showsPasswordField) {
    fields.push({
      key: "nfs_password",
      label: "Senha do portal",
      type: "password",
      target: "company_settings",
      placeholder: "Senha de acesso ao portal",
      visible_when: {
        key: "auth_mode",
        equals: "password",
        target: "company_settings",
      },
    });
  }

  if (capabilities.usesLoginBinding) {
    fields.push({
      key: "selected_login_cpf",
      label: "Login vinculado",
      type: "login_binding",
      target: "company_settings",
      help_text: capabilities.helperText,
    });
  }

  return fields;
}

function getFallbackScheduleSchema(robot: Robot): RobotConfigFieldSchema[] {
  const fields = getFallbackAdminSchema(robot).filter(
    (field) => field.target === "execution_defaults",
  );
  return fields;
}

export function normalizeRobotConfigSchema(value: Json | null | undefined): RobotConfigFieldSchema[] {
  return asArray(value).map(normalizeFieldSchema).filter((field): field is RobotConfigFieldSchema => Boolean(field));
}

export function getRobotAdminFormSchema(robot: Robot): RobotConfigFieldSchema[] {
  const dbSchema = normalizeRobotConfigSchema(robot.admin_form_schema);
  return dbSchema.length > 0 ? dbSchema : getFallbackAdminSchema(robot);
}

export function getRobotCompanyFormSchema(robot: Robot): RobotConfigFieldSchema[] {
  const dbSchema = normalizeRobotConfigSchema(robot.company_form_schema);
  return dbSchema.length > 0 ? dbSchema : getFallbackCompanySchema(robot);
}

export function getRobotScheduleFormSchema(robot: Robot): RobotConfigFieldSchema[] {
  const dbSchema = normalizeRobotConfigSchema(robot.schedule_form_schema);
  return dbSchema.length > 0 ? dbSchema : getFallbackScheduleSchema(robot);
}

export function getRobotConfigFieldDefaultValue(field: RobotConfigFieldSchema): Json {
  if (field.default_value !== undefined) return field.default_value;
  if (field.type === "boolean") return false;
  if (field.type === "login_list") return [];
  if (field.type === "auth_mode") return "password";
  return "";
}

export function getRobotConfigRecord(value: Json | null | undefined): Record<string, Json> {
  return asObject(value);
}

export function getRobotGlobalLogins(value: Json | null | undefined): CompanySefazLogin[] {
  return asArray(value)
    .map((item) => {
      if (!item || typeof item !== "object" || Array.isArray(item)) return null;
      const row = item as Record<string, Json>;
      const cpf = String(row.cpf ?? "").trim();
      const password = String(row.password ?? "").trim();
      if (!cpf || !password) return null;
      return {
        cpf,
        password,
        is_default: Boolean(row.is_default),
      } satisfies CompanySefazLogin;
    })
    .filter((item): item is CompanySefazLogin => Boolean(item));
}

export function hasRobotFieldType(
  fields: RobotConfigFieldSchema[],
  type: RobotConfigFieldType,
): boolean {
  return fields.some((field) => field.type === type);
}

export function isRobotFieldVisible(
  field: RobotConfigFieldSchema,
  valuesByTarget: Partial<Record<RobotConfigTarget, Record<string, Json>>>,
): boolean {
  const rule = field.visible_when;
  if (!rule?.key) return true;

  const target = rule.target ?? field.target ?? "company_settings";
  const source = valuesByTarget[target] ?? {};
  const currentValue = source[rule.key];

  if (rule.equals !== undefined) return currentValue === rule.equals;
  if (rule.not_equals !== undefined) return currentValue !== rule.not_equals;
  return Boolean(currentValue);
}

export function getUniqueCityOptions(cityNames: Array<string | null | undefined>): RobotConfigFieldOption[] {
  return Array.from(
    new Set(
      cityNames
        .map((city) => String(city ?? "").trim())
        .filter(Boolean),
    ),
  )
    .sort((left, right) => left.localeCompare(right, "pt-BR"))
    .map((city) => ({ label: city, value: city }));
}

export function getRobotExecutionCity(robot: Robot): string | null {
  const executionDefaults = getRobotConfigRecord(robot.execution_defaults);
  const value = String(executionDefaults.city_name ?? "").trim();
  return value || null;
}

export function buildRobotExecutionSnapshot(robot: Robot): Record<string, Json> {
  return {
    execution_defaults: getRobotConfigRecord(robot.execution_defaults),
    admin_settings: getRobotConfigRecord(robot.admin_settings),
    runtime_defaults: getRobotConfigRecord(robot.runtime_defaults),
    city_name: getRobotExecutionCity(robot),
    date_execution_mode: robot.date_execution_mode ?? null,
    segment_path: robot.segment_path ?? null,
    notes_mode: robot.notes_mode ?? null,
  };
}
