import { RobotConfigFieldGroup } from "@/components/robots/RobotConfigFieldGroup";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import type { Accountant } from "@/services/accountantsService";
import type { RobotCompanyConfigInput } from "@/services/companiesService";
import type { Robot } from "@/services/robotsService";
import type { Json } from "@/types/database";
import {
  getRobotCapabilities,
  getRobotCompanyFormSchema,
  getRobotConfigRecord,
  getRobotGlobalLogins,
} from "@/lib/robotConfigSchemas";
import { canEnableRobotForCompany, getRobotEnableRequirementMessage } from "@/lib/companyRobotRequirements";
import { cn } from "@/utils";

function onlyDigits(value: string) {
  return value.replace(/\D/g, "");
}

function formatCpf(value: string) {
  const digits = onlyDigits(value);
  if (digits.length !== 11) return value;
  return digits.replace(/(\d{3})(\d{3})(\d{3})(\d{2})/, "$1.$2.$3-$4");
}

function getDefaultLoginCpf(robot: Robot, contadorCpf: string, fallbackSelected?: string | null) {
  const logins = getRobotGlobalLogins(robot.global_logins);
  const contador = onlyDigits(contadorCpf);
  const selected = onlyDigits(fallbackSelected ?? "");
  if (selected && logins.some((login) => login.cpf === selected)) return selected;
  if (contador && logins.some((login) => login.cpf === contador)) return contador;
  const explicitDefault = logins.find((login) => login.is_default);
  return explicitDefault?.cpf ?? logins[0]?.cpf ?? "";
}

export function CompanyRobotsEditor({
  robots,
  accountants,
  selectedRobotTechnicalId,
  onSelectedRobotTechnicalIdChange,
  configsByRobot,
  onConfigChange,
  contadorCpf,
  stateRegistration,
  document,
  cae,
  availableCities = [],
  disabled = false,
}: {
  robots: Robot[];
  accountants: Accountant[];
  selectedRobotTechnicalId: string;
  onSelectedRobotTechnicalIdChange: (value: string) => void;
  configsByRobot: Record<string, RobotCompanyConfigInput>;
  onConfigChange: (robotTechnicalId: string, next: RobotCompanyConfigInput) => void;
  contadorCpf: string;
  stateRegistration?: string | null;
  document?: string | null;
  cae?: string | null;
  availableCities?: Array<string | null | undefined>;
  disabled?: boolean;
}) {
  const selectedRobot =
    robots.find((robot) => robot.technical_id === selectedRobotTechnicalId) ??
    robots[0] ??
    null;

  if (!selectedRobot) {
    return <p className="text-sm text-muted-foreground">Nenhum robo vinculado encontrado.</p>;
  }

  const config =
    configsByRobot[selectedRobot.technical_id] ??
    {
      enabled: false,
      auth_mode: "password" as const,
      nfs_password: null,
      selected_login_cpf: getDefaultLoginCpf(selectedRobot, contadorCpf, null),
      settings: {},
    };

  const robotLogins = getRobotGlobalLogins(selectedRobot.global_logins);
  const resolvedSelectedLogin = getDefaultLoginCpf(selectedRobot, contadorCpf, config.selected_login_cpf);
  const companySettings = {
    ...getRobotConfigRecord(config.settings),
    auth_mode: config.auth_mode ?? "password",
    nfs_password: config.nfs_password ?? null,
    selected_login_cpf: config.selected_login_cpf ?? resolvedSelectedLogin ?? null,
  } satisfies Record<string, Json>;
  const fields = getRobotCompanyFormSchema(selectedRobot);
  const capabilities = getRobotCapabilities(selectedRobot);
  const canEnableSelectedRobot = canEnableRobotForCompany(selectedRobot, {
    stateRegistration,
    document,
    cae,
  });
  const enableRequirementMessage = getRobotEnableRequirementMessage(selectedRobot);
  const accountantNameByCpf = new Map(
    accountants.map((accountant) => [onlyDigits(accountant.cpf), accountant.name]),
  );

  const handleCompanyFieldChange = (key: string, value: Json) => {
    const nextSettings = {
      ...getRobotConfigRecord(config.settings),
      [key]: value,
    } satisfies Record<string, Json>;

    const authMode =
      key === "auth_mode"
        ? (String(value || "password") as "password" | "certificate")
        : config.auth_mode;
    const selectedLoginCpf =
      key === "selected_login_cpf"
        ? (value ? String(value) : null)
        : config.selected_login_cpf;
    const password =
      key === "nfs_password"
        ? (value ? String(value) : "")
        : (config.nfs_password ?? "");

    onConfigChange(selectedRobot.technical_id, {
      ...config,
      auth_mode: authMode,
      nfs_password: authMode === "password" ? password : null,
      selected_login_cpf: selectedLoginCpf,
      settings: {
        ...nextSettings,
        auth_mode: authMode,
        nfs_password: authMode === "password" ? password : null,
        selected_login_cpf: selectedLoginCpf,
      },
    });
  };

  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <Label>Robo</Label>
        <Select value={selectedRobot.technical_id} onValueChange={onSelectedRobotTechnicalIdChange} disabled={disabled}>
          <SelectTrigger>
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {robots.map((robot) => (
              <SelectItem key={robot.id} value={robot.technical_id}>
                {robot.display_name}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <p className="text-[10px] text-muted-foreground">
          Selecione o robo vinculado para configurar como esta empresa roda nele.
        </p>
      </div>

      <div className="space-y-4 rounded-lg border border-border bg-muted/20 p-4">
        <div className="flex items-start justify-between gap-3">
          <div>
            <p className="text-sm font-medium">{selectedRobot.display_name}</p>
            <p className="font-mono text-[10px] text-muted-foreground">{selectedRobot.technical_id}</p>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-sm text-muted-foreground">Desligado</span>
            <button
              type="button"
              role="switch"
              aria-checked={config.enabled}
              onClick={() => {
                if (!config.enabled && !canEnableSelectedRobot) return;
                onConfigChange(selectedRobot.technical_id, { ...config, enabled: !config.enabled });
              }}
              disabled={disabled || (!config.enabled && !canEnableSelectedRobot)}
              className={cn(
                "relative inline-flex h-6 w-11 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:opacity-50",
                config.enabled ? "bg-primary" : "bg-muted",
              )}
              title={!config.enabled && !canEnableSelectedRobot ? enableRequirementMessage ?? undefined : undefined}
            >
              <span
                className={cn(
                  "pointer-events-none inline-block h-5 w-5 transform rounded-full bg-white shadow ring-0 transition",
                  config.enabled ? "translate-x-5" : "translate-x-1",
                )}
              />
            </button>
            <span className="text-sm text-muted-foreground">Ligado</span>
          </div>
        </div>

        {config.enabled ? (
          fields.length > 0 ? (
            <RobotConfigFieldGroup
              fields={fields}
              valuesByTarget={{ company_settings: companySettings }}
              onChangeField={(target, key, value) => {
                if (target !== "company_settings") return;
                handleCompanyFieldChange(key, value);
              }}
              globalLogins={robotLogins}
              cityNames={availableCities}
              disabled={disabled}
              loginLabelByCpf={accountantNameByCpf}
            />
          ) : (
            <div className="space-y-1 rounded-md border border-border bg-background/60 p-3">
              <p className="text-sm font-medium">Execucao automatica</p>
              <p className="text-[10px] text-muted-foreground">
                {capabilities.helperText || "Este robo nao precisa de configuracoes extras por empresa."}
              </p>
            </div>
          )
        ) : (
          <div className="space-y-1">
            <p className="text-xs text-muted-foreground">
              Este robo ficara ignorado para esta empresa ate ser ligado.
            </p>
            {!canEnableSelectedRobot && enableRequirementMessage ? (
              <p className="text-xs text-amber-600">{enableRequirementMessage}</p>
            ) : null}
          </div>
        )}

        {config.enabled && robotLogins.length > 0 ? (
          <div className="rounded-md border border-border bg-background/60 p-3">
            <p className="text-[10px] text-muted-foreground">
              Logins disponiveis: {robotLogins.map((login) => formatCpf(login.cpf)).join(", ")}
            </p>
          </div>
        ) : null}
      </div>
    </div>
  );
}
