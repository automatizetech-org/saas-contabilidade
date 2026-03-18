import { useState, useRef } from "react"
import { useNavigate } from "react-router-dom"
import { useQuery } from "@tanstack/react-query"
import { createCompany, upsertCompanyRobotConfig, ROBOT_NFS_TECHNICAL_ID } from "@/services/companiesService"
import { findAccountantByCpf, formatCpf, getAccountants } from "@/services/accountantsService"
import { useSelectedCompanyIds } from "@/hooks/useSelectedCompanies"
import { fetchCnpjPublica } from "@/services/cnpjPublicaService"
import { getBrazilStates, getCitiesByState } from "@/services/ibgeLocationsService"
import { GlassCard } from "@/components/dashboard/GlassCard"
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
import { getPfxInfo } from "@/lib/validatePfxPassword"
import { toast } from "sonner"
import { cn } from "@/utils"

function onlyDigits(s: string) {
  return s.replace(/\D/g, "")
}

function formatCnpjDigits(d: string) {
  if (d.length !== 14) return d
  return `${d.slice(0, 2)}.${d.slice(2, 5)}.${d.slice(5, 8)}/${d.slice(8, 12)}-${d.slice(12)}`
}

function fileToBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => {
      const result = reader.result
      if (result instanceof ArrayBuffer) {
        const bytes = new Uint8Array(result)
        let binary = ""
        for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i])
        resolve(btoa(binary))
      } else {
        reject(new Error("Leitura do arquivo falhou"))
      }
    }
    reader.onerror = () => reject(reader.error)
    reader.readAsArrayBuffer(file)
  })
}

export default function EmpresasNovaPage() {
  const navigate = useNavigate()
  const { setSelectedCompanyIds } = useSelectedCompanyIds()
  const [name, setName] = useState("")
  const [document, setDocument] = useState("")
  const [stateRegistration, setStateRegistration] = useState("")
  const [stateCode, setStateCode] = useState("")
  const [cityName, setCityName] = useState("")
  const [cae, setCae] = useState("")
  const [useCertificate, setUseCertificate] = useState(false)
  const [certFile, setCertFile] = useState<File | null>(null)
  const [certPassword, setCertPassword] = useState("")
  const certInputRef = useRef<HTMLInputElement>(null)
  const [contadorCpf, setContadorCpf] = useState("")
  const [error, setError] = useState("")
  const [loading, setLoading] = useState(false)
  const [loadingCnpj, setLoadingCnpj] = useState(false)
  const { data: accountants = [] } = useQuery({
    queryKey: ["accountants"],
    queryFn: () => getAccountants(true),
    staleTime: 30000,
  })
  const { data: states = [] } = useQuery({
    queryKey: ["ibge-states"],
    queryFn: getBrazilStates,
    staleTime: 24 * 60 * 60 * 1000,
  })
  const { data: cities = [] } = useQuery({
    queryKey: ["ibge-cities", stateCode],
    queryFn: () => getCitiesByState(stateCode),
    enabled: !!stateCode,
    staleTime: 24 * 60 * 60 * 1000,
  })

  const [nfsRobotEnabled, setNfsRobotEnabled] = useState(false)
  const [nfsRobotAuthMode, setNfsRobotAuthMode] = useState<"password" | "certificate">("password")
  const [nfsRobotPassword, setNfsRobotPassword] = useState("")

  const fetchByCnpj = async () => {
    const digits = onlyDigits(document)
    if (digits.length !== 14) {
      setError("Informe um CNPJ válido (14 dígitos) para buscar.")
      return
    }
    setError("")
    setLoadingCnpj(true)
    try {
      const data = await fetchCnpjPublica(digits)
      if (!data) {
        setError("CNPJ não encontrado.")
        return
      }
      if (data.razao_social && !name.trim()) setName(data.razao_social)
      if (data.inscricao_estadual && !stateRegistration.trim()) setStateRegistration(data.inscricao_estadual)
      if (data.state_code) setStateCode(data.state_code)
      if (data.city_name) setCityName(data.city_name)
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : "Erro ao consultar a Receita. Verifique a conexão.")
    } finally {
      setLoadingCnpj(false)
    }
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError("")
    if (useCertificate && (!certFile || !certPassword.trim())) {
      setError("Ao usar certificado digital, selecione o arquivo .pfx e informe a senha.")
      return
    }
    setLoading(true)
    try {
      let cert_blob_b64: string | null = null
      let cert_password: string | null = null
      let cert_valid_until: string | null = null
      if (useCertificate && certFile && certPassword.trim()) {
        cert_blob_b64 = await fileToBase64(certFile)
        cert_password = certPassword.trim()
        const info = getPfxInfo(cert_blob_b64, cert_password)
        if (!info.valid) {
          setError("Senha do certificado incorreta. Não foi possível cadastrar.")
          toast.error("Senha do certificado incorreta.")
          return
        }
        const docDigits = onlyDigits(document)
        if (docDigits.length !== 14) {
          setError("Para vincular o certificado corretamente, informe um CNPJ válido (14 dígitos) antes de enviar o .pfx.")
          toast.error("Informe um CNPJ válido antes de enviar o certificado.")
          return
        }
        if (info.cnpj && info.cnpj !== docDigits) {
          setError(`CNPJ do certificado (${formatCnpjDigits(info.cnpj)}) não corresponde ao CNPJ da empresa (${formatCnpjDigits(docDigits)}). Não foi possível cadastrar.`)
          toast.error("CNPJ do certificado não corresponde ao da empresa.")
          return
        }
        cert_valid_until = info.validUntil ?? null
      }
      const company = await createCompany({
        name: name.trim(),
        document: document.trim() || null,
        state_registration: stateRegistration.trim() || null,
        state_code: stateCode || null,
        city_name: cityName || null,
        cae: cae.trim() || null,
        auth_mode: useCertificate ? "certificate" : null,
        cert_blob_b64,
        cert_password,
        cert_valid_until,
        contador_nome: contadorCpf ? (findAccountantByCpf(accountants, contadorCpf)?.name ?? null) : null,
        contador_cpf: contadorCpf || null,
      })
      await upsertCompanyRobotConfig(company.id, ROBOT_NFS_TECHNICAL_ID, {
        enabled: nfsRobotEnabled,
        auth_mode: nfsRobotAuthMode,
        nfs_password: nfsRobotAuthMode === "password" ? nfsRobotPassword.trim() || null : null,
      })
      setSelectedCompanyIds([company.id])
      toast.success("Empresa cadastrada com sucesso. Certificado enviado ao Supabase.")
      navigate("/dashboard", { replace: true })
    } catch (err: unknown) {
      const message = err && typeof err === "object" && "message" in err ? String((err as { message: string }).message) : "Erro ao cadastrar"
      setError(message)
      toast.error(message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold font-display tracking-tight">Nova empresa</h1>
        <p className="text-sm text-muted-foreground mt-1">Cadastre uma nova empresa para gerenciar no dashboard.</p>
      </div>

      <GlassCard className="p-6 max-w-md">
        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="name">Nome</Label>
            <Input
              id="name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Razão social"
              required
              disabled={loading}
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="document">Documento (CNPJ)</Label>
            <div className="flex gap-2">
              <Input
                id="document"
                value={document}
                onChange={(e) => setDocument(e.target.value)}
                onBlur={() => {
                  if (!name.trim() && onlyDigits(document).length === 14) fetchByCnpj()
                }}
                placeholder="00.000.000/0001-00"
                disabled={loading}
                className="flex-1"
              />
              <Button
                type="button"
                variant="outline"
                onClick={fetchByCnpj}
                disabled={loading || loadingCnpj}
                title="Preencher nome pela Receita Federal se estiver vazio"
              >
                {loadingCnpj ? "Buscando..." : "Buscar"}
              </Button>
            </div>
            <p className="text-xs text-muted-foreground">Se o nome estiver vazio, use Buscar para preencher pela Receita.</p>
          </div>
          <div className="space-y-2">
            <Label htmlFor="state-registration">IE</Label>
            <Input
              id="state-registration"
              value={stateRegistration}
              onChange={(e) => setStateRegistration(e.target.value)}
              placeholder="Inscrição estadual"
              disabled={loading}
            />
            <p className="text-xs text-muted-foreground">
              Logins específicos de robôs são configurados depois, em editar empresa ou editar robô.
            </p>
          </div>
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="state-code">Estado</Label>
              <Select
                value={stateCode || "none"}
                onValueChange={(value) => {
                  const nextState = value === "none" ? "" : value
                  setStateCode(nextState)
                  setCityName("")
                }}
                disabled={loading}
              >
                <SelectTrigger id="state-code">
                  <SelectValue placeholder="Selecione o estado" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">Não informado</SelectItem>
                  {states.map((state) => (
                    <SelectItem key={state.code} value={state.code}>
                      {state.code} - {state.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="city-name">Município</Label>
              <Select
                value={cityName || "none"}
                onValueChange={(value) => setCityName(value === "none" ? "" : value)}
                disabled={loading || !stateCode}
              >
                <SelectTrigger id="city-name">
                  <SelectValue placeholder={stateCode ? "Selecione o município" : "Selecione o estado primeiro"} />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">Não informado</SelectItem>
                  {cities.map((city) => (
                    <SelectItem key={city.name} value={city.name}>
                      {city.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
          <div className="space-y-2">
            <Label htmlFor="cae">CAE (Inscrição Municipal)</Label>
            <Input
              id="cae"
              value={cae}
              onChange={(e) => setCae(e.target.value)}
              disabled={loading}
              placeholder="Ex.: 2163519"
            />
            <p className="text-xs text-muted-foreground">Opcional. Inscrição municipal (ex.: Prefeitura de Goiânia). Usado pelo robô de taxas para localizar a empresa no portal.</p>
          </div>
          <div className="space-y-3 pt-2 border-t border-border">
            <div className="flex items-center gap-2">
              <input
                type="checkbox"
                id="use-cert"
                checked={useCertificate}
                onChange={(e) => setUseCertificate(e.target.checked)}
                disabled={loading}
                className="rounded border-input"
              />
              <Label htmlFor="use-cert" className="font-normal cursor-pointer">Certificado digital (uso geral)</Label>
            </div>
            <p className="text-xs text-muted-foreground">Opcional. Use para NFS-e, emissões e outras situações que exijam certificado A1 (.pfx).</p>
            {useCertificate && (
              <div className="space-y-2 pl-4 border-l-2 border-border">
                <div className="space-y-1">
                  <Label>Arquivo .pfx</Label>
                  <div className="flex gap-2 items-center min-w-0">
                    <Input
                      ref={certInputRef}
                      type="file"
                      accept=".pfx"
                      onChange={(e) => setCertFile(e.target.files?.[0] ?? null)}
                      disabled={loading}
                      className="flex-1 min-w-0 max-w-[180px]"
                    />
                    {certFile && (
                      <span className="text-xs text-muted-foreground truncate min-w-0 flex-1" title={certFile.name}>
                        {certFile.name}
                      </span>
                    )}
                  </div>
                </div>
                <div className="space-y-1">
                  <Label>Senha do certificado</Label>
                  <Input
                    type="password"
                    value={certPassword}
                    onChange={(e) => setCertPassword(e.target.value)}
                    placeholder="Senha do .pfx"
                    disabled={loading}
                    autoComplete="off"
                  />
                </div>
              </div>
            )}
          </div>

          <div className="space-y-3 pt-2 border-t border-border">
            <p className="text-sm font-medium">Robô NFS (portal nacional)</p>
            <p className="text-xs text-muted-foreground">Ative para esta empresa rodar no robô de download de NFS-e. Se desligado, o robô não processa esta empresa.</p>
            <div className="flex items-center gap-3">
              <span className="text-sm text-muted-foreground">Desligado</span>
              <button
                type="button"
                role="switch"
                aria-checked={nfsRobotEnabled}
                onClick={() => setNfsRobotEnabled((v) => !v)}
                className={cn(
                  "relative inline-flex h-6 w-11 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-2",
                  nfsRobotEnabled ? "bg-primary" : "bg-muted"
                )}
              >
                <span
                  className={cn(
                    "pointer-events-none inline-block h-5 w-5 transform rounded-full bg-white shadow ring-0 transition",
                    nfsRobotEnabled ? "translate-x-5" : "translate-x-1"
                  )}
                />
              </button>
              <span className="text-sm text-muted-foreground">Ligado</span>
            </div>
            {nfsRobotEnabled && (
              <div className="pl-4 border-l-2 border-border space-y-3">
                <div className="flex items-center gap-4">
                  <label className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="radio"
                      name="nfs-auth"
                      checked={nfsRobotAuthMode === "password"}
                      onChange={() => setNfsRobotAuthMode("password")}
                      disabled={loading}
                      className="rounded-full border-input"
                    />
                    <span className="text-sm">Login (CNPJ + senha)</span>
                  </label>
                  <label className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="radio"
                      name="nfs-auth"
                      checked={nfsRobotAuthMode === "certificate"}
                      onChange={() => setNfsRobotAuthMode("certificate")}
                      disabled={loading}
                      className="rounded-full border-input"
                    />
                    <span className="text-sm">Certificado</span>
                  </label>
                </div>
                <p className="text-xs text-muted-foreground">
                  {nfsRobotAuthMode === "certificate"
                    ? "Usa o certificado cadastrado acima (uso geral)."
                    : "Senha para acesso no portal nacional com CNPJ."}
                </p>
                {nfsRobotAuthMode === "password" && (
                  <div className="space-y-1">
                    <Label>Senha do portal NFS</Label>
                    <Input
                      type="password"
                      value={nfsRobotPassword}
                      onChange={(e) => setNfsRobotPassword(e.target.value)}
                      placeholder="Senha de acesso ao portal"
                      disabled={loading}
                      autoComplete="off"
                    />
                  </div>
                )}
              </div>
            )}
          </div>

          <div className="space-y-2">
            <Label>Contador responsável</Label>
            <Select
              value={contadorCpf || "none"}
              onValueChange={(v) => setContadorCpf(v === "none" ? "" : v)}
              disabled={loading}
            >
              <SelectTrigger className="min-h-10 [&>span]:line-clamp-none [&>span]:whitespace-normal [&>span]:text-left py-2">
                <SelectValue placeholder="Selecione o contador" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="none">Nenhum</SelectItem>
                {accountants.map((c) => (
                  <SelectItem key={c.cpf} value={c.cpf}>
                    {c.name} — CPF {formatCpf(c.cpf)}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>

          {error && (
            <p className="text-sm text-destructive">{error}</p>
          )}
          <div className="flex gap-2">
            <Button type="submit" disabled={loading}>
              {loading ? "Salvando..." : "Cadastrar"}
            </Button>
            <Button type="button" variant="outline" onClick={() => navigate(-1)} disabled={loading}>
              Cancelar
            </Button>
          </div>
        </form>
      </GlassCard>
    </div>
  )
}
