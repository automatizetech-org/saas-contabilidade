import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { canEnableRobotForCompany, getRobotEnableRequirementMessage } from "@/lib/companyRobotRequirements"
import type { Accountant } from "@/services/accountantsService"
import type { Robot } from "@/services/robotsService"
import type { CompanySefazLogin, RobotCompanyConfigInput } from "@/services/companiesService"
import { cn } from "@/utils"

function onlyDigits(value: string) {
  return value.replace(/\D/g, "")
}

function formatCpf(value: string) {
  const digits = onlyDigits(value)
  if (digits.length !== 11) return value
  return digits.replace(/(\d{3})(\d{3})(\d{3})(\d{2})/, "$1.$2.$3-$4")
}

function normalizeRobotLogins(value: unknown): CompanySefazLogin[] {
  if (!Array.isArray(value)) return []
  return value
    .map((item) => {
      if (!item || typeof item !== "object") return null
      const row = item as { cpf?: string; password?: string; senha?: string; is_default?: boolean }
      const cpf = onlyDigits(row.cpf ?? "")
      const password = String(row.password ?? row.senha ?? "").trim()
      if (cpf.length !== 11 || !password) return null
      return { cpf, password, is_default: Boolean(row.is_default) }
    })
    .filter((item): item is CompanySefazLogin => Boolean(item))
}

function getDefaultLoginCpf(robot: Robot, contadorCpf: string, fallbackSelected?: string | null) {
  const logins = normalizeRobotLogins(robot.global_logins)
  const contador = onlyDigits(contadorCpf)
  const selected = onlyDigits(fallbackSelected ?? "")
  if (selected && logins.some((login) => login.cpf === selected)) return selected
  if (contador && logins.some((login) => login.cpf === contador)) return contador
  const explicitDefault = logins.find((login) => login.is_default)
  return explicitDefault?.cpf ?? logins[0]?.cpf ?? ""
}

function getRobotCapabilities(robot: Robot) {
  if (robot.technical_id === "nfs_padrao") {
    return {
      authBehavior: "choice" as const,
      usesLoginBinding: false,
      showsPasswordField: true,
      helperText: "Este robô permite escolher entre login no portal e certificado digital.",
    }
  }

  if (robot.technical_id === "sefaz_xml") {
    return {
      authBehavior: "login_only" as const,
      usesLoginBinding: true,
      showsPasswordField: false,
      helperText: "Este robô usa login global por CPF, vinculado empresa por empresa.",
    }
  }

  if (robot.technical_id === "certidoes_fiscal") {
    return {
      authBehavior: "cnpj_only" as const,
      usesLoginBinding: false,
      showsPasswordField: false,
      helperText: "Este robô usa apenas o CNPJ e os dados da empresa para consultar.",
    }
  }

  return {
    authBehavior: "cnpj_only" as const,
    usesLoginBinding: false,
    showsPasswordField: false,
    helperText: "Este robô só exibe as opções necessárias para o modo de execução dele.",
  }
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
  disabled = false,
}: {
  robots: Robot[]
  accountants: Accountant[]
  selectedRobotTechnicalId: string
  onSelectedRobotTechnicalIdChange: (value: string) => void
  configsByRobot: Record<string, RobotCompanyConfigInput>
  onConfigChange: (robotTechnicalId: string, next: RobotCompanyConfigInput) => void
  contadorCpf: string
  stateRegistration?: string | null
  disabled?: boolean
}) {
  const selectedRobot =
    robots.find((robot) => robot.technical_id === selectedRobotTechnicalId) ??
    robots[0] ??
    null

  if (!selectedRobot) {
    return <p className="text-sm text-muted-foreground">Nenhum robô vinculado encontrado.</p>
  }

  const config =
    configsByRobot[selectedRobot.technical_id] ??
    {
      enabled: false,
      auth_mode: "password" as const,
      nfs_password: null,
      selected_login_cpf: getDefaultLoginCpf(selectedRobot, contadorCpf, null),
    }

  const robotLogins = normalizeRobotLogins(selectedRobot.global_logins)
  const resolvedSelectedLogin = getDefaultLoginCpf(selectedRobot, contadorCpf, config.selected_login_cpf)
  const capabilities = getRobotCapabilities(selectedRobot)
  const canEnableSelectedRobot = canEnableRobotForCompany(selectedRobot.technical_id, stateRegistration)
  const enableRequirementMessage = getRobotEnableRequirementMessage(selectedRobot.technical_id)
  const accountantNameByCpf = new Map(
    accountants.map((accountant) => [onlyDigits(accountant.cpf), accountant.name])
  )

  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <Label>Robô</Label>
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
          Selecione o robô vinculado para configurar como esta empresa roda nele.
        </p>
      </div>

      <div className="rounded-lg border border-border bg-muted/20 p-4 space-y-4">
        <div className="flex items-start justify-between gap-3">
          <div>
            <p className="text-sm font-medium">{selectedRobot.display_name}</p>
            <p className="text-[10px] text-muted-foreground font-mono">{selectedRobot.technical_id}</p>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-sm text-muted-foreground">Desligado</span>
            <button
              type="button"
              role="switch"
              aria-checked={config.enabled}
              onClick={() => {
                if (!config.enabled && !canEnableSelectedRobot) return
                onConfigChange(selectedRobot.technical_id, { ...config, enabled: !config.enabled })
              }}
              disabled={disabled || (!config.enabled && !canEnableSelectedRobot)}
              className={cn(
                "relative inline-flex h-6 w-11 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2 disabled:opacity-50",
                config.enabled ? "bg-primary" : "bg-muted"
              )}
              title={!config.enabled && !canEnableSelectedRobot ? enableRequirementMessage ?? undefined : undefined}
            >
              <span
                className={cn(
                  "pointer-events-none inline-block h-5 w-5 transform rounded-full bg-white shadow ring-0 transition",
                  config.enabled ? "translate-x-5" : "translate-x-1"
                )}
              />
            </button>
            <span className="text-sm text-muted-foreground">Ligado</span>
          </div>
        </div>

        {config.enabled && (
          <>
            {capabilities.authBehavior === "choice" && (
              <div className="space-y-3">
                <p className="text-sm font-medium">Modo de autenticação da empresa</p>
                <div className="flex items-center gap-4">
                  <label className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="radio"
                      name={`auth-mode-${selectedRobot.technical_id}`}
                      checked={config.auth_mode === "password"}
                      onChange={() => onConfigChange(selectedRobot.technical_id, { ...config, auth_mode: "password" })}
                      disabled={disabled}
                      className="rounded-full border-input"
                    />
                    <span className="text-sm">Login</span>
                  </label>
                  <label className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="radio"
                      name={`auth-mode-${selectedRobot.technical_id}`}
                      checked={config.auth_mode === "certificate"}
                      onChange={() => onConfigChange(selectedRobot.technical_id, { ...config, auth_mode: "certificate" })}
                      disabled={disabled}
                      className="rounded-full border-input"
                    />
                    <span className="text-sm">Certificado</span>
                  </label>
                </div>
                <p className="text-[10px] text-muted-foreground">{capabilities.helperText}</p>
              </div>
            )}

            {capabilities.authBehavior === "login_only" && (
              <div className="space-y-1 rounded-md border border-border bg-background/60 p-3">
                <p className="text-sm font-medium">Autenticação do robô</p>
                <p className="text-[10px] text-muted-foreground">{capabilities.helperText}</p>
              </div>
            )}

            {capabilities.authBehavior === "cnpj_only" && (
              <div className="space-y-1 rounded-md border border-border bg-background/60 p-3">
                <p className="text-sm font-medium">Execução automática</p>
                <p className="text-[10px] text-muted-foreground">{capabilities.helperText}</p>
              </div>
            )}

            {capabilities.showsPasswordField && config.auth_mode === "password" && (
                <div className="space-y-1">
                  <Label>Senha do portal</Label>
                  <Input
                    type="password"
                    value={config.nfs_password ?? ""}
                    onChange={(e) => onConfigChange(selectedRobot.technical_id, { ...config, nfs_password: e.target.value })}
                    placeholder="Senha de acesso ao portal"
                    disabled={disabled}
                    autoComplete="off"
                  />
                </div>
            )}

            {capabilities.usesLoginBinding && (
              <div className="space-y-2 rounded-md border border-border bg-background/60 p-3">
                <Label>Login vinculado a esta empresa</Label>
                <Select
                  value={resolvedSelectedLogin || "__none__"}
                  onValueChange={(value) =>
                    onConfigChange(selectedRobot.technical_id, {
                      ...config,
                      selected_login_cpf: value === "__none__" ? null : value,
                    })
                  }
                  disabled={disabled || robotLogins.length === 0}
                >
                  <SelectTrigger>
                    <SelectValue placeholder={robotLogins.length === 0 ? "Cadastre logins no editar robô" : "Selecione o login"} />
                  </SelectTrigger>
                  <SelectContent>
                    {robotLogins.length === 0 ? (
                      <SelectItem value="__none__">Nenhum login global cadastrado</SelectItem>
                    ) : (
                      robotLogins.map((login) => (
                        <SelectItem key={login.cpf} value={login.cpf}>
                          {formatCpf(login.cpf)}
                          {accountantNameByCpf.get(login.cpf) ? ` • ${accountantNameByCpf.get(login.cpf)}` : ""}
                          {login.is_default ? " • padrão" : ""}
                        </SelectItem>
                      ))
                    )}
                  </SelectContent>
                </Select>
                <p className="text-[10px] text-muted-foreground">
                  Se não escolher manualmente, o sistema prioriza o login do contador da empresa e depois o login padrão do robô.
                </p>
              </div>
            )}
          </>
        )}

        {!config.enabled && (
          <div className="space-y-1">
            <p className="text-xs text-muted-foreground">
              Este robô ficará ignorado para esta empresa até ser ligado.
            </p>
            {!canEnableSelectedRobot && enableRequirementMessage && (
              <p className="text-xs text-amber-600">{enableRequirementMessage}</p>
            )}
          </div>
        )}
      </div>
    </div>
  )
}
