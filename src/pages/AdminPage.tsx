import { useEffect, useMemo, useRef, useState } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { getUsersForAdmin, getProfile, updateProfile } from "@/services/profilesService"
import { supabase } from "@/services/supabaseClient"
import { getCompaniesForUser, updateCompany, getCompanyRobotConfigs, upsertCompanyRobotConfig, type RobotCompanyConfigInput } from "@/services/companiesService"
import { findAccountantByCpf, formatCpf, getAccountants } from "@/services/accountantsService"
import type { Company, AdminUser } from "@/services/profilesService"
import { GlassCard } from "@/components/dashboard/GlassCard"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { Users, Building2, Shield, Key, Pencil, Loader2 } from "lucide-react"
import { AdminFolderStructure } from "@/components/admin/AdminFolderStructure"
import { AdminRobotsList } from "@/components/admin/AdminRobotsList"
import { AdminScheduler } from "@/components/admin/AdminScheduler"
import { AdminFileRetention } from "@/components/admin/AdminFileRetention"
import { AdminBasePath } from "@/components/admin/AdminBasePath"
import { AdminBrandingBlock } from "@/components/admin/branding"
import { getRobots } from "@/services/robotsService"
import { PANEL_KEYS, PANEL_LABELS } from "@/lib/panelAccess"
import { getPfxInfo } from "@/lib/validatePfxPassword"
import { toast } from "sonner"
import { CompanyRobotsEditor } from "@/components/companies/CompanyRobotsEditor"
import { sanitizeRobotConfigForCompany } from "@/lib/companyRobotRequirements"
import { getBrazilStates, getCitiesByState } from "@/services/ibgeLocationsService"

const SUPABASE_URL = import.meta.env.SUPABASE_URL
const SUPABASE_ANON_KEY = import.meta.env.SUPABASE_ANON_KEY ?? ""

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
      } else reject(new Error("Leitura do arquivo falhou"))
    }
    reader.onerror = () => reject(reader.error)
    reader.readAsArrayBuffer(file)
  })
}

export default function AdminPage() {
  const queryClient = useQueryClient()
  const [modalOpen, setModalOpen] = useState(false)
  const [username, setUsername] = useState("")
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [role, setRole] = useState<"user" | "super_admin">("user")
  const [submitError, setSubmitError] = useState("")
  const [submitting, setSubmitting] = useState(false)
  const [bootstrapSecret, setBootstrapSecret] = useState("")
  const [bootstrapError, setBootstrapError] = useState("")
  const [bootstrapLoading, setBootstrapLoading] = useState(false)
  const [editingCompany, setEditingCompany] = useState<Company | null>(null)
  const [editName, setEditName] = useState("")
  const [editDocument, setEditDocument] = useState("")
  const [editStateRegistration, setEditStateRegistration] = useState("")
  const [editStateCode, setEditStateCode] = useState("")
  const [editCityName, setEditCityName] = useState("")
  const [editActive, setEditActive] = useState(true)
  const [editContadorCpf, setEditContadorCpf] = useState("")
  const [editUseCertificate, setEditUseCertificate] = useState(false)
  const [editCertReplacing, setEditCertReplacing] = useState(false)
  const [editCertFile, setEditCertFile] = useState<File | null>(null)
  const [editCertPassword, setEditCertPassword] = useState("")
  const editCertInputRef = useRef<HTMLInputElement>(null)
  const [editSaving, setEditSaving] = useState(false)
  const [editError, setEditError] = useState("")
  const [selectedRobotTechnicalId, setSelectedRobotTechnicalId] = useState("")
  const [editRobotConfigs, setEditRobotConfigs] = useState<Record<string, RobotCompanyConfigInput>>({})
  const [editingUser, setEditingUser] = useState<AdminUser | null>(null)
  const [editUsername, setEditUsername] = useState("")
  const [editEmail, setEditEmail] = useState("")
  const [editPassword, setEditPassword] = useState("")
  const [editRole, setEditRole] = useState<"user" | "super_admin">("user")
  const [editPanelAccess, setEditPanelAccess] = useState<Record<string, boolean>>({})
  const [editUserSaving, setEditUserSaving] = useState(false)
  const [editUserError, setEditUserError] = useState("")
  const [passwordJustSet, setPasswordJustSet] = useState<string | null>(null)
  const [companySearch, setCompanySearch] = useState("")

  const defaultPanelAccess: Record<string, boolean> = {
    dashboard: true,
    fiscal: true,
    dp: true,
    inteligencia_tributaria: true,
    ir: true,
    paralegal: true,
    financeiro: true,
    operacoes: true,
    documentos: true,
    empresas: true,
    sync: true,
  }

  const { data: session } = useQuery({
    queryKey: ["auth-session"],
    queryFn: async () => {
      const { data } = await supabase.auth.getSession()
      return data.session
    },
  })
  const userId = session?.user?.id
  const { data: myProfile } = useQuery({
    queryKey: ["profile", userId],
    queryFn: () => getProfile(userId!),
    enabled: !!userId,
  })

  const { data: profiles = [], isLoading: profilesLoading } = useQuery({
    queryKey: ["admin-profiles"],
    queryFn: async () => {
      try {
        return await getUsersForAdmin()
      } catch {
        const list = await supabase.from("profiles").select("id, username, role, created_at").order("id", { ascending: true })
        if (list.error) throw list.error
        return (list.data ?? []).map((p) => ({
          id: (p as { id: string }).id,
          username: (p as { username?: string | null }).username ?? "",
          role: (p as { role?: string }).role ?? "user",
          created_at: "",
          panel_access: {},
          email: null as string | null,
        })) as AdminUser[]
      }
    },
    staleTime: 0,
    refetchOnMount: "always",
  })
  const adminUsers = profiles as AdminUser[]

  const { data: companies = [] } = useQuery({
    queryKey: ["admin-companies"],
    queryFn: () => getCompaniesForUser("all"),
  })
  const filteredCompanies = useMemo(() => {
    const q = companySearch.trim().toLowerCase()
    if (!q) return companies
    const digits = q.replace(/\D/g, "")
    return companies.filter((company) => {
      const name = String(company.name || "").toLowerCase()
      const document = String((company as { document?: string | null }).document || "")
      return name.includes(q) || (!!digits && document.replace(/\D/g, "").includes(digits))
    })
  }, [companies, companySearch])

  const { data: robotsForPaths = [] } = useQuery({
    queryKey: ["admin-robots"],
    queryFn: getRobots,
    enabled: myProfile?.role === "super_admin",
    refetchOnWindowFocus: true,
    refetchInterval: 5000,
    staleTime: 5000,
  })
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
    queryKey: ["ibge-cities", editStateCode],
    queryFn: () => getCitiesByState(editStateCode),
    enabled: !!editStateCode,
    staleTime: 24 * 60 * 60 * 1000,
  })

  useEffect(() => {
    if (!selectedRobotTechnicalId && robotsForPaths.length > 0) {
      setSelectedRobotTechnicalId(robotsForPaths[0].technical_id)
    }
  }, [robotsForPaths, selectedRobotTechnicalId])

  const handleCreateUser = async (e: React.FormEvent) => {
    e.preventDefault()
    setSubmitError("")
    setSubmitting(true)
    try {
      const { data: refreshData } = await supabase.auth.refreshSession()
      const session = refreshData?.session ?? (await supabase.auth.getSession()).data.session
      if (!session?.access_token) throw new Error("Não autenticado. Faça login novamente.")
      const url = `${SUPABASE_URL}/functions/v1/create-user`
      const res = await fetch(url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
          apikey: SUPABASE_ANON_KEY,
          "X-User-Token": session.access_token,
        },
        body: JSON.stringify({ email: email.trim(), password, username: username.trim(), role }),
      })
      const json = await res.json().catch(() => ({}))
      if (!res.ok) {
        const msg =
          json?.detail ||
          json?.error ||
          (res.status === 401 ? "Sessão expirada ou inválida. Faça login novamente." : null) ||
          (res.status === 404 ? "Função create-user não encontrada. Execute: npx supabase functions deploy create-user" : null) ||
          "Falha ao criar usuário"
        throw new Error(msg)
      }
      queryClient.invalidateQueries({ queryKey: ["admin-profiles"] })
      setModalOpen(false)
      setUsername("")
      setEmail("")
      setPassword("")
      setRole("user")
    } catch (err: unknown) {
      if (err instanceof Error) setSubmitError(err.message)
      else if (typeof err === "object" && err !== null && "message" in err) setSubmitError(String((err as { message: string }).message))
      else setSubmitError("Erro ao criar usuário. Verifique a conexão e se a Edge Function create-user está publicada (npx supabase functions deploy create-user).")
    } finally {
      setSubmitting(false)
    }
  }

  const handleBootstrapAdmin = async (e: React.FormEvent) => {
    e.preventDefault()
    setBootstrapError("")
    setBootstrapLoading(true)
    try {
      if (!session?.access_token) throw new Error("Não autenticado")
      const res = await fetch(`${SUPABASE_URL}/functions/v1/auth`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${session.access_token}`,
        },
        body: JSON.stringify({ bootstrap_secret: bootstrapSecret }),
      })
      const data = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(data.error || data.detail || "Falha ao definir admin")
      queryClient.invalidateQueries({ queryKey: ["profile", userId] })
      queryClient.invalidateQueries({ queryKey: ["admin-profiles"] })
      setBootstrapSecret("")
    } catch (err: unknown) {
      setBootstrapError(err instanceof Error ? err.message : "Erro")
    } finally {
      setBootstrapLoading(false)
    }
  }

  const handleSetRole = async (profileId: string, newRole: "super_admin" | "user") => {
    try {
      await updateProfile(profileId, { role: newRole })
      queryClient.invalidateQueries({ queryKey: ["admin-profiles"] })
    } catch {
      // toast or inline error
    }
  }

  const openEditCompany = async (emp: (typeof companies)[0]) => {
    // Busca o registro completo no banco ao abrir o modal para evitar snapshot antigo
    // (ex.: após imports via SQL Editor).
    let row = emp as unknown as Company
    try {
      const { data, error } = await supabase
        .from("companies")
        .select("*")
        .eq("id", emp.id)
        .single()
      if (!error && data) row = data as Company
    } catch {
      // fallback para o item da lista
    }

    setEditingCompany(row)
    setEditName(row.name)
    setEditDocument(row.document ?? "")
    setEditStateRegistration((row as { state_registration?: string | null }).state_registration ?? "")
    setEditStateCode((row as { state_code?: string | null }).state_code ?? "")
    setEditCityName((row as { city_name?: string | null }).city_name ?? "")
    setEditActive((row as { active?: boolean }).active !== false)
    setEditContadorCpf((row as { contador_cpf?: string | null }).contador_cpf ?? "")
    const withCert = row as { cert_blob_b64?: string | null; auth_mode?: string | null }
    setEditUseCertificate(!!(withCert.cert_blob_b64 || withCert.auth_mode === "certificate"))
    setEditCertReplacing(false)
    setEditCertFile(null)
    setEditCertPassword("")
    setEditError("")
    setSelectedRobotTechnicalId((current) => current || robotsForPaths[0]?.technical_id || "")
    try {
      const configs = await getCompanyRobotConfigs(row.id)
      const configsByRobot = Object.fromEntries(
        configs.map((config) => [
          config.robot_technical_id,
          {
            enabled: config.enabled,
            auth_mode: config.auth_mode ?? "password",
            nfs_password: config.nfs_password ?? null,
            selected_login_cpf: config.selected_login_cpf ?? null,
          } satisfies RobotCompanyConfigInput,
        ])
      )
      setEditRobotConfigs(configsByRobot)
    } catch {
      setEditRobotConfigs({})
    }
  }

  const handleSaveCompany = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!editingCompany) return
    setEditError("")
    if (editUseCertificate && (editCertFile || editCertPassword) && (!editCertFile || !editCertPassword.trim())) {
      setEditError("Selecione o arquivo .pfx e informe a senha do certificado.")
      return
    }
    setEditSaving(true)
    try {
      const updates: Parameters<typeof updateCompany>[1] = {
        name: editName.trim(),
        document: editDocument.trim() || null,
        state_registration: editStateRegistration.trim() || null,
        state_code: editStateCode || null,
        city_name: editCityName || null,
        active: editActive,
        contador_nome: editContadorCpf ? (findAccountantByCpf(accountants, editContadorCpf)?.name ?? null) : null,
        contador_cpf: editContadorCpf || null,
      }
      if (!editUseCertificate) {
        updates.auth_mode = null
        updates.cert_blob_b64 = null
        updates.cert_password = null
        updates.cert_valid_until = null
      } else if (editCertFile && editCertPassword.trim()) {
        updates.auth_mode = "certificate"
        const b64 = await fileToBase64(editCertFile)
        const pwd = editCertPassword.trim()
        const info = getPfxInfo(b64, pwd)
        if (!info.valid) {
          setEditError("Senha do certificado incorreta. Não foi possível salvar.")
          toast.error("Senha do certificado incorreta.")
          return
        }
        const docDigits = onlyDigits(editDocument)
        if (docDigits.length !== 14) {
          setEditError("Para vincular o certificado corretamente, informe um CNPJ válido (14 dígitos) antes de enviar o .pfx.")
          toast.error("Informe um CNPJ válido antes de enviar o certificado.")
          return
        }
        if (info.cnpj && info.cnpj !== docDigits) {
          setEditError(`CNPJ do certificado (${formatCnpjDigits(info.cnpj)}) não corresponde ao CNPJ da empresa (${formatCnpjDigits(docDigits)}). Não foi possível salvar.`)
          toast.error("CNPJ do certificado não corresponde ao da empresa.")
          return
        }
        updates.cert_blob_b64 = b64
        updates.cert_password = pwd
        updates.cert_valid_until = info.validUntil ?? null
      } else if (editUseCertificate && (editingCompany as { cert_blob_b64?: string | null }).cert_blob_b64) {
        updates.auth_mode = "certificate"
      }
      await updateCompany(editingCompany.id, updates)
      await Promise.all(
        robotsForPaths.map((robot) => {
          const rawConfig = editRobotConfigs[robot.technical_id] ?? {
            enabled: false,
            auth_mode: "password" as const,
            nfs_password: null,
            selected_login_cpf: null,
          }
          const config = sanitizeRobotConfigForCompany(robot.technical_id, rawConfig, editStateRegistration)
          return upsertCompanyRobotConfig(editingCompany.id, robot.technical_id, {
            enabled: config.enabled,
            auth_mode: config.auth_mode,
            nfs_password: config.auth_mode === "password" ? config.nfs_password ?? null : null,
            selected_login_cpf: config.selected_login_cpf ?? null,
          })
        })
      )
      queryClient.invalidateQueries({ queryKey: ["admin-companies"] })
      queryClient.invalidateQueries({ queryKey: ["admin-robots"] })
      setEditingCompany(null)
      toast.success("Empresa salva com sucesso. Certificado enviado ao Supabase.")
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : "Erro ao salvar"
      setEditError(msg)
      toast.error(msg)
    } finally {
      setEditSaving(false)
    }
  }

  const openEditUser = (user: AdminUser) => {
    setEditingUser(user)
    setEditUsername((user.username ?? "").trim())
    setEditEmail(user.email ?? "")
    setEditPassword("")
    setEditRole((user.role === "super_admin" ? "super_admin" : "user") as "user" | "super_admin")
    const current = (user.panel_access as Record<string, boolean>) || {}
    setEditPanelAccess({ ...defaultPanelAccess, ...current })
    setEditUserError("")
  }

  const handleSaveUserAccess = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!editingUser) return
    setEditUserError("")
    setEditUserSaving(true)
    try {
      const { data: refreshData } = await supabase.auth.refreshSession()
      const session = refreshData?.session ?? (await supabase.auth.getSession()).data.session
      if (!session?.access_token) throw new Error("Não autenticado. Faça login novamente.")
      const res = await fetch(`${SUPABASE_URL}/functions/v1/update-user`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
          apikey: SUPABASE_ANON_KEY,
          "X-User-Token": session.access_token,
        },
        body: JSON.stringify({
          user_id: editingUser.id,
          username: editUsername.trim(),
          email: editEmail.trim() || undefined,
          password: editPassword ? editPassword : undefined,
          role: editRole,
          panel_access: editPanelAccess,
        }),
      })
      const json = await res.json().catch(() => ({}))
      if (!res.ok) throw new Error(json?.detail || json?.error || "Falha ao atualizar usuário")
      if (editPassword) setPasswordJustSet(editPassword)
      queryClient.invalidateQueries({ queryKey: ["admin-profiles"] })
      queryClient.invalidateQueries({ queryKey: ["profile", editingUser.id] })
      setEditingUser(null)
    } catch (err: unknown) {
      setEditUserError(err instanceof Error ? err.message : "Erro ao salvar")
    } finally {
      setEditUserSaving(false)
    }
  }

  const isSuperAdmin = myProfile?.role === "super_admin"
  const segmentPath = robotsForPaths[0]?.segment_path || "FISCAL/NFS"
  const segmentSlug = segmentPath.replace(/\//g, "\\")

  return (
    <div className="space-y-6">
      {userId && myProfile && !isSuperAdmin && (
        <GlassCard className="p-4 border-amber-200 dark:border-amber-800 bg-amber-50/50 dark:bg-amber-900/10">
          <h3 className="text-sm font-semibold font-display mb-2">Definir primeiro administrador</h3>
          <p className="text-xs text-muted-foreground mb-3">
            Você está logado mas ainda não há um administrador. Insira o segredo de bootstrap (configurado na Edge Function <code className="bg-muted px-1 rounded">auth</code>) para se tornar admin.
          </p>
          <form onSubmit={handleBootstrapAdmin} className="flex flex-wrap items-end gap-2">
            <div className="flex-1 min-w-[200px]">
              <Label htmlFor="bootstrap-secret" className="sr-only">Segredo</Label>
              <Input
                id="bootstrap-secret"
                type="password"
                value={bootstrapSecret}
                onChange={(e) => setBootstrapSecret(e.target.value)}
                placeholder="Segredo de bootstrap"
                disabled={bootstrapLoading}
              />
            </div>
            <Button type="submit" disabled={bootstrapLoading || !bootstrapSecret.trim()}>
              {bootstrapLoading ? "Enviando..." : "Definir como administrador"}
            </Button>
          </form>
          {bootstrapError && <p className="text-sm text-destructive mt-2">{bootstrapError}</p>}
        </GlassCard>
      )}

      <div>
        <h1 className="text-2xl font-bold font-display tracking-tight">Administração</h1>
        <p className="text-sm text-muted-foreground mt-1">Gerenciamento de usuários, empresas e permissões</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <GlassCard className="p-5 flex items-center gap-4">
          <div className="rounded-lg bg-primary/10 p-3"><Users className="h-5 w-5 text-primary-icon" /></div>
          <div>
            <p className="text-2xl font-bold font-display">{profilesLoading ? "—" : adminUsers.length}</p>
            <p className="text-xs text-muted-foreground">Usuários</p>
          </div>
        </GlassCard>
        <GlassCard className="p-5 flex items-center gap-4">
          <div className="rounded-lg bg-accent/20 p-3"><Building2 className="h-5 w-5 text-accent" /></div>
          <div>
            <p className="text-2xl font-bold font-display">{companies.length}</p>
            <p className="text-xs text-muted-foreground">Empresas</p>
          </div>
        </GlassCard>
        <GlassCard className="p-5 flex items-center gap-4">
          <div className="rounded-lg bg-success/15 p-3"><Shield className="h-5 w-5 text-success" /></div>
          <div>
            <p className="text-2xl font-bold font-display">{adminUsers.length}</p>
            <p className="text-xs text-muted-foreground">Perfis</p>
          </div>
        </GlassCard>
        <GlassCard className="p-5 flex items-center gap-4">
          <div className="rounded-lg bg-info/15 p-3"><Key className="h-5 w-5 text-info" /></div>
          <div>
            <p className="text-2xl font-bold font-display">—</p>
            <p className="text-xs text-muted-foreground">Integrações</p>
          </div>
        </GlassCard>
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <GlassCard className="overflow-hidden">
          <div className="p-4 border-b border-border flex items-center justify-between">
            <h3 className="text-sm font-semibold font-display">Usuários</h3>
            <Button size="sm" onClick={() => setModalOpen(true)}>Cadastrar novo usuário</Button>
          </div>
          <div className="divide-y divide-border">
            {profilesLoading
              ? <div className="px-4 py-6 text-center text-muted-foreground text-sm">Carregando...</div>
              : adminUsers.length === 0
                ? <div className="px-4 py-6 text-center text-muted-foreground text-sm">Nenhum usuário.</div>
                : adminUsers.map((user) => {
                    const displayName = user.username?.trim() || "Sem nome"
                    const initials = (user.username?.trim() || "?").slice(0, 2).toUpperCase()
                    return (
                    <div key={user.id} className="px-4 py-3 flex items-center justify-between hover:bg-muted/30 transition-colors">
                      <div className="flex items-center gap-3">
                        <div className="h-8 w-8 rounded-full bg-primary/20 flex items-center justify-center text-[10px] font-bold text-primary-icon">
                          {initials}
                        </div>
                        <div>
                          <p className="text-xs font-medium">{displayName}</p>
                          <p className="text-[10px] text-muted-foreground">{user.email ?? user.role}</p>
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        {isSuperAdmin && (
                          <Button
                            type="button"
                            variant="ghost"
                            size="sm"
                            className="text-[10px] h-7 gap-1"
                            onClick={() => openEditUser(user)}
                          >
                            <Pencil className="h-3 w-3" />
                            Editar
                          </Button>
                        )}
                        {isSuperAdmin && user.id !== userId && (
                          user.role === "super_admin" ? (
                            <Button
                              type="button"
                              variant="outline"
                              size="sm"
                              className="text-[10px] h-7"
                              onClick={() => handleSetRole(user.id, "user")}
                            >
                              Remover admin
                            </Button>
                          ) : (
                            <Button
                              type="button"
                              variant="outline"
                              size="sm"
                              className="text-[10px] h-7"
                              onClick={() => handleSetRole(user.id, "super_admin")}
                            >
                              Tornar admin
                            </Button>
                          )
                        )}
                        <span className={`rounded px-2 py-0.5 text-[10px] font-medium ${user.role === "super_admin" ? "bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-200" : "bg-primary/10 text-primary-icon"}`}>
                          {user.role}
                        </span>
                      </div>
                    </div>
                    )
                  })}
          </div>
        </GlassCard>

        <GlassCard className="overflow-hidden">
          <div className="p-4 border-b border-border">
            <h3 className="text-sm font-semibold font-display">Empresas</h3>
            <div className="relative mt-3">
              <Input
                value={companySearch}
                onChange={(event) => setCompanySearch(event.target.value)}
                placeholder="Buscar por nome ou CNPJ..."
                className="h-9 text-xs"
              />
            </div>
          </div>
          <div className="max-h-[28rem] overflow-y-auto divide-y divide-border">
            {filteredCompanies.length === 0
              ? <div className="px-4 py-6 text-center text-muted-foreground text-sm">Nenhuma empresa.</div>
              : filteredCompanies.map((emp) => (
                  <div key={emp.id} className="px-4 py-3 flex items-center justify-between hover:bg-muted/30 transition-colors">
                    <div>
                      <p className="text-xs font-medium">{emp.name}</p>
                      <p className="text-[10px] text-muted-foreground">{(emp as { document?: string | null }).document ?? "—"}</p>
                      {(emp as { contador_nome?: string | null }).contador_nome && (
                        <p className="text-[10px] text-muted-foreground mt-0.5 truncate max-w-[200px]" title={(emp as { contador_nome?: string | null }).contador_nome ?? undefined}>
                          {(emp as { contador_nome?: string | null }).contador_nome}
                        </p>
                      )}
                    </div>
                    <div className="flex items-center gap-2">
                      <span className={`text-[10px] font-medium ${(emp as { active?: boolean }).active !== false ? "text-success" : "text-muted-foreground"}`}>
                        {(emp as { active?: boolean }).active !== false ? "Ativa" : "Inativa"}
                      </span>
                      <Button
                        type="button"
                        variant="ghost"
                        size="icon"
                        className="h-8 w-8"
                        onClick={() => openEditCompany(emp)}
                        aria-label="Editar empresa"
                      >
                        <Pencil className="h-3.5 w-3.5" />
                      </Button>
                    </div>
                  </div>
                ))}
          </div>
        </GlassCard>
      </div>

      <AdminFolderStructure isSuperAdmin={!!isSuperAdmin} />

      {isSuperAdmin && <AdminBrandingBlock />}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <AdminRobotsList isSuperAdmin={!!isSuperAdmin} robots={robotsForPaths} />
        <AdminScheduler isSuperAdmin={!!isSuperAdmin} robots={robotsForPaths} />
      </div>
      {isSuperAdmin && (
        <>
          <AdminBasePath isSuperAdmin={!!isSuperAdmin} />
          <AdminFileRetention isSuperAdmin={!!isSuperAdmin} />
        </>
      )}

      <GlassCard className="overflow-hidden">
        <div className="p-4 border-b border-border">
          <h3 className="text-sm font-semibold font-display">Matriz de Permissões</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-xs">
            <thead>
              <tr className="border-b border-border bg-muted/50">
                <th className="text-left px-4 py-3 font-medium text-muted-foreground">Perfil</th>
                <th className="text-center px-4 py-3 font-medium text-muted-foreground">Dashboard</th>
                <th className="text-center px-4 py-3 font-medium text-muted-foreground">Fiscal</th>
                <th className="text-center px-4 py-3 font-medium text-muted-foreground">DP</th>
                <th className="text-center px-4 py-3 font-medium text-muted-foreground">Financeiro</th>
                <th className="text-center px-4 py-3 font-medium text-muted-foreground">Operações</th>
                <th className="text-center px-4 py-3 font-medium text-muted-foreground">Admin</th>
              </tr>
            </thead>
            <tbody>
              {[
                { perfil: "super_admin", perms: [true, true, true, true, true, true] },
                { perfil: "user", perms: [true, true, true, true, true, false] },
              ].map((row) => (
                <tr key={row.perfil} className="border-b border-border">
                  <td className="px-4 py-3 font-medium">{row.perfil}</td>
                  {row.perms.map((perm, j) => (
                    <td key={j} className="px-4 py-3 text-center">
                      {perm ? (
                        <span className="inline-block h-4 w-4 rounded-full bg-success/20 text-success text-[10px] leading-4">✓</span>
                      ) : (
                        <span className="inline-block h-4 w-4 rounded-full bg-muted text-muted-foreground text-[10px] leading-4">—</span>
                      )}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </GlassCard>

      <Dialog open={!!editingCompany} onOpenChange={(open) => !open && !editSaving && setEditingCompany(null)}>
        <DialogContent aria-describedby={undefined}>
          {editSaving && (
            <div className="absolute inset-0 z-50 flex items-center justify-center rounded-lg bg-background/80 backdrop-blur-sm">
              <div className="flex flex-col items-center gap-3">
                <Loader2 className="h-10 w-10 animate-spin text-primary-icon" />
                <p className="text-sm font-medium">Salvando empresa e enviando certificado ao Supabase...</p>
              </div>
            </div>
          )}
          <DialogHeader>
            <DialogTitle>Editar empresa</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleSaveCompany} className="space-y-4 min-w-0 overflow-hidden">
            <Tabs defaultValue="geral" className="space-y-4">
              <TabsList className="grid w-full grid-cols-3">
                <TabsTrigger value="geral">Geral</TabsTrigger>
                <TabsTrigger value="certificado">Certificado digital</TabsTrigger>
                <TabsTrigger value="robos">Robôs</TabsTrigger>
              </TabsList>
              <TabsContent value="geral" className="space-y-4">
            <div className="space-y-2">
              <Label>Nome</Label>
              <Input value={editName} onChange={(e) => setEditName(e.target.value)} required disabled={editSaving} />
            </div>
            <div className="space-y-2">
              <Label>Documento (CNPJ)</Label>
              <Input value={editDocument} onChange={(e) => setEditDocument(e.target.value)} disabled={editSaving} placeholder="00.000.000/0001-00" />
            </div>
            <div className="space-y-2">
              <Label>IE</Label>
              <Input value={editStateRegistration} onChange={(e) => setEditStateRegistration(e.target.value)} disabled={editSaving} placeholder="Inscrição estadual" />
            </div>
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="space-y-2">
                <Label>Estado</Label>
                <Select
                  value={editStateCode || "none"}
                  onValueChange={(value) => {
                    const nextState = value === "none" ? "" : value
                    setEditStateCode(nextState)
                    setEditCityName("")
                  }}
                  disabled={editSaving}
                >
                  <SelectTrigger>
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
                <Label>Município</Label>
                <Select
                  value={editCityName || "none"}
                  onValueChange={(value) => setEditCityName(value === "none" ? "" : value)}
                  disabled={editSaving || !editStateCode}
                >
                  <SelectTrigger>
                    <SelectValue placeholder={editStateCode ? "Selecione o município" : "Selecione o estado primeiro"} />
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
            <div className="flex items-center gap-2">
              <input type="checkbox" id="edit-active" checked={editActive} onChange={(e) => setEditActive(e.target.checked)} disabled={editSaving} className="rounded border-input" />
              <Label htmlFor="edit-active">Ativa</Label>
            </div>
            <div className="space-y-2">
              <Label>Contador responsável</Label>
              <Select
                value={editContadorCpf || "none"}
                onValueChange={(v) => setEditContadorCpf(v === "none" ? "" : v)}
                disabled={editSaving}
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
              </TabsContent>
              <TabsContent value="certificado" className="space-y-3">
            <div className="space-y-3">
              <div className="flex items-center gap-2">
                <input
                  type="checkbox"
                  id="edit-cert"
                  checked={editUseCertificate}
                  onChange={(e) => {
                    setEditUseCertificate(e.target.checked)
                    if (!e.target.checked) setEditCertReplacing(false)
                  }}
                  disabled={editSaving}
                  className="rounded border-input"
                />
                <Label htmlFor="edit-cert" className="font-normal cursor-pointer">Certificado digital (uso geral)</Label>
              </div>
              {editUseCertificate && (
                <div className="pl-4 border-l-2 border-border space-y-2">
                  {(!!(editingCompany as { cert_blob_b64?: string | null; auth_mode?: string | null })?.cert_blob_b64 || (editingCompany as { auth_mode?: string | null })?.auth_mode === "certificate") && !editCertReplacing ? (
                    <div className="space-y-1">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="text-sm text-muted-foreground">Certificado cadastrado</span>
                        <Button type="button" variant="outline" size="sm" onClick={() => { setEditCertReplacing(true); setEditCertFile(null); setEditCertPassword(""); editCertInputRef.current?.click(); }} disabled={editSaving}>
                          Substituir
                        </Button>
                        <Button type="button" variant="ghost" size="sm" onClick={() => setEditUseCertificate(false)} disabled={editSaving}>
                          Remover
                        </Button>
                      </div>
                      {(editingCompany as { cert_valid_until?: string | null })?.cert_valid_until && (
                        <p className="text-sm text-green-600 dark:text-green-400 font-medium">
                          Certificado ativo — Válido até {(() => {
                            const [y, m, d] = (editingCompany as { cert_valid_until: string }).cert_valid_until.split("-")
                            return `${d}/${m}/${y}`
                          })()}
                        </p>
                      )}
                    </div>
                  ) : (
                    <>
                      <input
                        ref={editCertInputRef}
                        type="file"
                        accept=".pfx"
                        onChange={(e) => setEditCertFile(e.target.files?.[0] ?? null)}
                        className="hidden"
                      />
                      <div className="space-y-1">
                        <Label>Arquivo .pfx</Label>
                        <div className="flex gap-2 items-center min-w-0 overflow-hidden">
                          <Button type="button" variant="outline" size="sm" onClick={() => editCertInputRef.current?.click()} disabled={editSaving} className="w-full min-w-0 overflow-hidden justify-start text-left">
                            <span className="truncate block w-full" title={editCertFile?.name ?? undefined}>
                              {editCertFile ? editCertFile.name : "Selecionar"}
                            </span>
                          </Button>
                        </div>
                      </div>
                      <div className="space-y-1">
                        <Label>Senha do certificado</Label>
                        <Input
                          type="password"
                          value={editCertPassword}
                          onChange={(e) => setEditCertPassword(e.target.value)}
                          placeholder="Senha do .pfx"
                          disabled={editSaving}
                          autoComplete="off"
                        />
                      </div>
                      {(!!(editingCompany as { cert_blob_b64?: string | null })?.cert_blob_b64 || (editingCompany as { auth_mode?: string | null })?.auth_mode === "certificate") && (
                        <Button type="button" variant="ghost" size="sm" onClick={() => { setEditCertReplacing(false); setEditCertFile(null); setEditCertPassword(""); }} disabled={editSaving}>
                          Manter certificado atual
                        </Button>
                      )}
                    </>
                  )}
                </div>
              )}
            </div>
              </TabsContent>
              <TabsContent value="robos" className="space-y-4">
                <div className="space-y-1">
                  <p className="text-sm font-medium">Robôs vinculados</p>
                  <p className="text-xs text-muted-foreground">
                    Escolha um robô e configure esta empresa nele. Para o Sefaz Xml, os logins são globais no editar robô e aqui fica apenas o vínculo do login correto à empresa.
                  </p>
                </div>
                <CompanyRobotsEditor
                  robots={robotsForPaths}
                  accountants={accountants}
                  selectedRobotTechnicalId={selectedRobotTechnicalId}
                  onSelectedRobotTechnicalIdChange={setSelectedRobotTechnicalId}
                  configsByRobot={editRobotConfigs}
                  onConfigChange={(robotTechnicalId, next) =>
                    setEditRobotConfigs((current) => ({
                      ...current,
                      [robotTechnicalId]: next,
                    }))
                  }
                  contadorCpf={editContadorCpf}
                  stateRegistration={editStateRegistration}
                  disabled={editSaving}
                />
              </TabsContent>
            </Tabs>
            {editError && <p className="text-sm text-destructive">{editError}</p>}
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => setEditingCompany(null)} disabled={editSaving}>Cancelar</Button>
              <Button type="submit" disabled={editSaving}>{editSaving ? "Salvando..." : "Salvar"}</Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog open={!!editingUser} onOpenChange={(open) => !open && setEditingUser(null)}>
        <DialogContent className="max-h-[90vh] flex flex-col p-0 gap-0">
          <DialogHeader className="shrink-0 px-6 pt-6 pb-2">
            <DialogTitle>Editar usuário — {editingUser?.username || "Usuário"}</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleSaveUserAccess} className="flex flex-col flex-1 min-h-0">
            <div className="flex-1 min-h-0 overflow-y-auto px-6 pb-4 space-y-4">
              <div className="space-y-2">
              <Label htmlFor="edit-username">Usuário (nome)</Label>
              <Input
                id="edit-username"
                value={editUsername}
                onChange={(e) => setEditUsername(e.target.value)}
                placeholder="Nome de exibição"
                required
                disabled={editUserSaving}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="edit-email">Email</Label>
              <Input
                id="edit-email"
                type="email"
                value={editEmail}
                onChange={(e) => setEditEmail(e.target.value)}
                placeholder="email@exemplo.com"
                disabled={editUserSaving}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="edit-password">Nova senha</Label>
              <p className="text-xs text-muted-foreground">
                A senha atual não pode ser visualizada nem recuperada (segurança do Supabase). Para redefinir, digite uma nova senha abaixo e clique em Salvar. Após salvar, a nova senha será exibida uma vez para você copiar e informar o usuário.
              </p>
              <Input
                id="edit-password"
                type="password"
                value={editPassword}
                onChange={(e) => setEditPassword(e.target.value)}
                placeholder="Deixe em branco para não alterar a senha"
                disabled={editUserSaving}
                minLength={6}
              />
            </div>
            <div className="space-y-2">
              <Label>Perfil</Label>
              <Select value={editRole} onValueChange={(v) => setEditRole(v as "user" | "super_admin")} disabled={editUserSaving}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="user">user</SelectItem>
                  <SelectItem value="super_admin">super_admin</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <p className="text-sm text-muted-foreground">
              {editRole === "super_admin"
                ? "Administradores têm acesso a todos os painéis."
                : "Ative ou desative os painéis que este usuário pode acessar:"}
            </p>
            {editRole === "user" && (
              <div className="space-y-3">
                {PANEL_KEYS.map((key) => (
                  <div key={key} className="flex items-center justify-between rounded-lg border border-border px-3 py-2">
                    <Label htmlFor={`panel-${key}`} className="text-sm font-medium">
                      {PANEL_LABELS[key]}
                    </Label>
                    <Switch
                      id={`panel-${key}`}
                      checked={editPanelAccess[key] !== false}
                      onCheckedChange={(checked) => setEditPanelAccess((prev) => ({ ...prev, [key]: checked }))}
                      disabled={editUserSaving}
                    />
                  </div>
                ))}
              </div>
            )}
            {editUserError && <p className="text-sm text-destructive">{editUserError}</p>}
            </div>
            <DialogFooter className="shrink-0 px-6 py-4 border-t bg-muted/30">
              <Button type="button" variant="outline" onClick={() => setEditingUser(null)} disabled={editUserSaving}>
                Fechar
              </Button>
              <Button type="submit" disabled={editUserSaving}>
                {editUserSaving ? "Salvando..." : "Salvar"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog open={!!passwordJustSet} onOpenChange={(open) => !open && setPasswordJustSet(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Senha alterada</DialogTitle>
          </DialogHeader>
          <p className="text-sm text-muted-foreground">
            Copie a nova senha e informe o usuário. Por segurança, ela não será exibida novamente.
          </p>
          <div className="flex items-center gap-2 rounded-lg border border-border bg-muted/30 px-3 py-2 font-mono text-sm">
            <span className="flex-1 break-all">{passwordJustSet ?? ""}</span>
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={() => {
                if (passwordJustSet) navigator.clipboard.writeText(passwordJustSet)
                toast.success("Senha copiada.")
              }}
            >
              Copiar
            </Button>
          </div>
          <DialogFooter>
            <Button type="button" onClick={() => setPasswordJustSet(null)}>Fechar</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog open={modalOpen} onOpenChange={setModalOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Cadastrar novo usuário</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleCreateUser} className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="new-username">Usuário</Label>
              <Input id="new-username" value={username} onChange={(e) => setUsername(e.target.value)} placeholder="username" required disabled={submitting} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="new-email">Email</Label>
              <Input id="new-email" type="email" value={email} onChange={(e) => setEmail(e.target.value)} placeholder="email@exemplo.com" required disabled={submitting} />
            </div>
            <div className="space-y-2">
              <Label htmlFor="new-password">Senha</Label>
              <Input id="new-password" type="password" value={password} onChange={(e) => setPassword(e.target.value)} placeholder="••••••••" required disabled={submitting} minLength={6} />
            </div>
            <div className="space-y-2">
              <Label>Perfil</Label>
              <Select value={role} onValueChange={(v) => setRole(v as "user" | "super_admin")} disabled={submitting}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="user">user</SelectItem>
                  <SelectItem value="super_admin">super_admin</SelectItem>
                </SelectContent>
              </Select>
            </div>
            {submitError && <p className="text-sm text-destructive">{submitError}</p>}
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => setModalOpen(false)} disabled={submitting}>Cancelar</Button>
              <Button type="submit" disabled={submitting}>{submitting ? "Criando..." : "Criar"}</Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </div>
  )
}
