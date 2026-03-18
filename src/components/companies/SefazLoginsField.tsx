import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import type { CompanySefazLogin } from "@/services/companiesService"
import { isValidCpf } from "@/lib/brazilDocuments"

function onlyDigits(value: string) {
  return value.replace(/\D/g, "")
}

function formatCpf(value: string) {
  const digits = onlyDigits(value).slice(0, 11)
  if (digits.length <= 3) return digits
  if (digits.length <= 6) return `${digits.slice(0, 3)}.${digits.slice(3)}`
  if (digits.length <= 9) return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6)}`
  return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6, 9)}-${digits.slice(9)}`
}

export function sanitizeSefazLogins(logins: CompanySefazLogin[]): CompanySefazLogin[] {
  const cleaned = logins
    .map((login) => ({
      cpf: onlyDigits(login.cpf ?? ""),
      password: String(login.password ?? "").trim(),
      is_default: Boolean(login.is_default),
    }))
    .filter((login) => isValidCpf(login.cpf) && login.password)

  const seen = new Set<string>()
  const deduped = cleaned.filter((login) => {
    if (seen.has(login.cpf)) return false
    seen.add(login.cpf)
    return true
  })

  if (deduped.length === 0) return []
  const defaultIndex = deduped.findIndex((login) => login.is_default)
  return deduped.map((login, index) => ({ ...login, is_default: defaultIndex === -1 ? index === 0 : index === defaultIndex }))
}

export function SefazLoginsField({
  value,
  onChange,
  disabled = false,
  title = "Logins SEFAZ GO",
  description = "Cadastre um ou mais logins CPF/senha. Marque qual deles é o padrão para o robô.",
  defaultLabel = "Login padrão desta empresa",
}: {
  value: CompanySefazLogin[]
  onChange: (next: CompanySefazLogin[]) => void
  disabled?: boolean
  title?: string
  description?: string
  defaultLabel?: string
}) {
  const update = (index: number, patch: Partial<CompanySefazLogin>) => {
    onChange(value.map((item, itemIndex) => (itemIndex === index ? { ...item, ...patch } : item)))
  }

  const addLogin = () => {
    onChange([...value, { cpf: "", password: "", is_default: value.length === 0 }])
  }

  const removeLogin = (index: number) => {
    const next = value.filter((_, itemIndex) => itemIndex !== index)
    if (next.length > 0 && !next.some((item) => item.is_default)) next[0] = { ...next[0], is_default: true }
    onChange(next)
  }

  const setDefault = (index: number) => {
    onChange(value.map((item, itemIndex) => ({ ...item, is_default: itemIndex === index })))
  }

  return (
    <div className="space-y-3 rounded-lg border border-border bg-muted/20 p-3">
      <div className="flex items-center justify-between gap-2">
        <div>
          <Label>{title}</Label>
          <p className="text-[10px] text-muted-foreground mt-1">
            {description}
          </p>
        </div>
        <Button type="button" variant="outline" size="sm" onClick={addLogin} disabled={disabled}>
          Adicionar login
        </Button>
      </div>

      {value.length === 0 ? (
        <p className="text-xs text-muted-foreground">Nenhum login cadastrado.</p>
      ) : (
        <div className="space-y-3">
          {value.map((login, index) => (
            <div key={`${index}-${login.cpf}`} className="rounded-md border border-border bg-background/60 p-3 space-y-3">
              <div className="grid gap-3 md:grid-cols-[1fr_1fr_auto]">
                <div className="space-y-1">
                  <Label className="text-[10px]">CPF do login</Label>
                  <Input
                    value={formatCpf(login.cpf)}
                    onChange={(e) => update(index, { cpf: onlyDigits(e.target.value) })}
                    placeholder="000.000.000-00"
                    disabled={disabled}
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-[10px]">Senha</Label>
                  <Input
                    type="password"
                    value={login.password}
                    onChange={(e) => update(index, { password: e.target.value })}
                    placeholder="Senha do portal"
                    disabled={disabled}
                    autoComplete="off"
                  />
                </div>
                <div className="flex items-end">
                  <Button type="button" variant="ghost" size="sm" onClick={() => removeLogin(index)} disabled={disabled}>
                    Remover
                  </Button>
                </div>
              </div>
              <label className="flex items-center gap-2 text-xs text-muted-foreground">
                <input
                  type="radio"
                  name="sefaz-default-login"
                  checked={Boolean(login.is_default)}
                  onChange={() => setDefault(index)}
                  disabled={disabled}
                />
                {defaultLabel}
              </label>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
