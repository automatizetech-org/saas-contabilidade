import { useEffect, useMemo, useState, type Dispatch, type SetStateAction } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { Loader2, Plus, Shield, Building2, ServerCog, KeyRound, Users, Trash2, PauseCircle, PlayCircle } from "lucide-react"
import { toast } from "sonner"
import { GlassCard } from "@/components/dashboard/GlassCard"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Badge } from "@/components/ui/badge"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { supabase } from "@/services/supabaseClient"
import { useProfile } from "@/hooks/useProfile"
import { getUsersForAdmin, type AdminUser } from "@/services/profilesService"
import { getCurrentOfficeContext } from "@/services/officeContextService"
import {
  createPrimeiroEscritorio,
  getCurrentOfficeServer,
  updateCurrentOfficeServer,
  setOfficeStatus,
  deleteOffice,
  type PrimeiroEscritorioInput,
} from "@/services/officeAdminService"
import { getCompaniesForUser } from "@/services/companiesService"
import { AdminFileRetention } from "@/components/admin/AdminFileRetention"
import { AdminFolderStructure } from "@/components/admin/AdminFolderStructure"
import { AdminRobotsList } from "@/components/admin/AdminRobotsList"
import { AdminScheduler } from "@/components/admin/AdminScheduler"
import { AdminBrandingBlock } from "@/components/admin/branding"
import { PANEL_KEYS, PANEL_LABELS } from "@/lib/panelAccess"

const SUPABASE_URL = import.meta.env.SUPABASE_URL ?? ""
const SUPABASE_ANON_KEY = import.meta.env.SUPABASE_ANON_KEY ?? ""

const defaultPanelAccess: Record<string, boolean> = {
  dashboard: true,
  fiscal: true,
  dp: true,
  contabil: true,
  inteligencia_tributaria: true,
  ir: true,
  paralegal: false,
  financeiro: true,
  operacoes: true,
  documentos: true,
  empresas: true,
  alteracao_empresarial: false,
  sync: false,
}

type UserFormState = {
  username: string
  email: string
  password: string
  role: "user" | "super_admin"
  office_role: "owner" | "viewer"
  panel_access: Record<string, boolean>
}

const emptyUserForm = (): UserFormState => ({
  username: "",
  email: "",
  password: "",
  role: "user",
  office_role: "viewer",
  panel_access: { ...defaultPanelAccess },
})

const emptyOfficeForm = (): PrimeiroEscritorioInput => ({
  office_name: "",
  office_slug: "",
  admin_email: "",
  admin_password: "",
  admin_username: "",
  public_base_url: "",
  base_path: "",
  connector_version: "",
  min_supported_connector_version: "",
})

async function callAdminFunction(name: string, payload: Record<string, unknown>) {
  const {
    data: { session },
  } = await supabase.auth.getSession()
  if (!session?.access_token) throw new Error("Não autenticado.")

  const response = await fetch(`${SUPABASE_URL}/functions/v1/${name}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${session.access_token}`,
      apikey: SUPABASE_ANON_KEY,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  })

  const body = await response.json().catch(() => ({}))
  if (!response.ok) throw new Error(body?.error || body?.detail || `Falha ao executar ${name}`)
  return body
}

function StatusBadge({ value }: { value: string | null | undefined }) {
  const normalized = String(value ?? "").trim() || "indefinido"
  const variant =
    normalized === "active"
      ? "default"
      : normalized === "pending"
        ? "secondary"
        : normalized === "offline"
          ? "destructive"
          : "outline"

  return <Badge variant={variant}>{normalized}</Badge>
}

function normalizeOfficeRole(role: string | null | undefined): UserFormState["office_role"] {
  return role === "owner" ? "owner" : "viewer"
}

function getOfficeRoleLabel(role: string | null | undefined): string {
  return normalizeOfficeRole(role) === "owner" ? "Owner" : "User"
}

function UserDialog({
  open,
  onOpenChange,
  title,
  form,
  setForm,
  onSubmit,
  submitting,
  allowPlatformRole,
  requirePassword,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  title: string
  form: UserFormState
  setForm: Dispatch<SetStateAction<UserFormState>>
  onSubmit: () => Promise<void>
  submitting: boolean
  allowPlatformRole: boolean
  requirePassword: boolean
}) {
  return (
    <Dialog open={open} onOpenChange={(next) => !submitting && onOpenChange(next)}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>
            O acesso operacional fica em `office_memberships`; `profiles.role` controla apenas o nível de plataforma.
          </DialogDescription>
        </DialogHeader>

        <div className="grid gap-4 md:grid-cols-2">
          <div className="space-y-2">
            <Label>Nome de exibição</Label>
            <Input
              value={form.username}
              onChange={(event) => setForm((current) => ({ ...current, username: event.target.value }))}
              placeholder="Ex.: Maria Silva"
              disabled={submitting}
            />
          </div>
          <div className="space-y-2">
            <Label>Email</Label>
            <Input
              type="email"
              value={form.email}
              onChange={(event) => setForm((current) => ({ ...current, email: event.target.value }))}
              placeholder="maria@escritorio.com"
              disabled={submitting}
            />
          </div>
          <div className="space-y-2">
            <Label>{requirePassword ? "Senha" : "Nova senha"}</Label>
            <Input
              type="password"
              value={form.password}
              onChange={(event) => setForm((current) => ({ ...current, password: event.target.value }))}
              placeholder={requirePassword ? "Obrigatória" : "Opcional"}
              disabled={submitting}
            />
          </div>
          <div className="space-y-2">
            <Label>Papel no escritório</Label>
            <Select
              value={form.office_role}
              onValueChange={(value) =>
                setForm((current) => ({
                  ...current,
                  office_role: value as UserFormState["office_role"],
                }))
              }
              disabled={submitting}
            >
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="owner">Owner</SelectItem>
                <SelectItem value="viewer">User</SelectItem>
              </SelectContent>
            </Select>
          </div>
          {allowPlatformRole ? (
            <div className="space-y-2 md:col-span-2">
              <Label>Papel de plataforma</Label>
              <Select
                value={form.role}
                onValueChange={(value) =>
                  setForm((current) => ({
                    ...current,
                    role: value as UserFormState["role"],
                  }))
                }
                disabled={submitting}
              >
                <SelectTrigger className="max-w-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="user">User</SelectItem>
                  <SelectItem value="super_admin">Super Admin</SelectItem>
                </SelectContent>
              </Select>
            </div>
          ) : null}
        </div>

        <div className="space-y-3">
          <div>
            <h3 className="text-sm font-medium">Acesso aos painéis</h3>
            <p className="text-xs text-muted-foreground">
              Ajuste o que esse usuário enxerga no menu lateral do escritório.
            </p>
          </div>
          <div className="grid gap-3 md:grid-cols-2">
            {PANEL_KEYS.map((panelKey) => (
              <div key={panelKey} className="flex items-center justify-between rounded-lg border border-border px-3 py-2">
                <span className="text-sm">{PANEL_LABELS[panelKey]}</span>
                <Switch
                  checked={Boolean(form.panel_access[panelKey])}
                  onCheckedChange={(checked) =>
                    setForm((current) => ({
                      ...current,
                      panel_access: {
                        ...current.panel_access,
                        [panelKey]: checked,
                      },
                    }))
                  }
                  disabled={submitting}
                />
              </div>
            ))}
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={submitting}>
            Cancelar
          </Button>
          <Button onClick={() => void onSubmit()} disabled={submitting}>
            {submitting ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
            Salvar
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

export default function AdminPage() {
  const queryClient = useQueryClient()
  const { profile, isSuperAdmin, canAccessAdmin, officeId, officeRole } = useProfile()
  const canManageOffice = isSuperAdmin || officeRole === "owner" || officeRole === "admin"

  const [officeForm, setOfficeForm] = useState<PrimeiroEscritorioInput>(emptyOfficeForm)
  const [creatingOffice, setCreatingOffice] = useState(false)
  const [lastConnectorSecret, setLastConnectorSecret] = useState<string | null>(null)

  const [createUserOpen, setCreateUserOpen] = useState(false)
  const [editUserOpen, setEditUserOpen] = useState(false)
  const [userForm, setUserForm] = useState<UserFormState>(emptyUserForm)
  const [editingUser, setEditingUser] = useState<AdminUser | null>(null)
  const [savingUser, setSavingUser] = useState(false)

  const [serverForm, setServerForm] = useState({
    public_base_url: "",
    base_path: "",
    connector_version: "",
    min_supported_connector_version: "",
    status: "pending",
  })
  const [savingServer, setSavingServer] = useState(false)

  type OfficeRow = { id: string; name: string; slug: string; status: string; created_at?: string }
  const [officeToDelete, setOfficeToDelete] = useState<OfficeRow | null>(null)
  const [deleteConfirmSlug, setDeleteConfirmSlug] = useState("")
  const [deleteLoading, setDeleteLoading] = useState(false)
  const [officeStatusLoading, setOfficeStatusLoading] = useState<string | null>(null)

  const { data: officeContext, isLoading: officeLoading } = useQuery({
    queryKey: ["admin", "office-context", officeId, officeRole, profile?.office_name ?? null],
    queryFn: getCurrentOfficeContext,
    enabled: canAccessAdmin && !!officeId,
  })

  const { data: officeServer, isLoading: serverLoading } = useQuery({
    queryKey: ["admin", "office-server", officeId],
    queryFn: getCurrentOfficeServer,
    enabled: !!officeId,
  })

  const { data: companies = [] } = useQuery({
    queryKey: ["admin", "companies-summary", officeId],
    queryFn: () => getCompaniesForUser("all"),
    enabled: !!officeId,
  })

  const { data: officeUsers = [], isLoading: usersLoading } = useQuery({
    queryKey: ["admin", "users", officeId ?? "platform"],
    queryFn: getUsersForAdmin,
    enabled: canAccessAdmin,
  })

  const { data: offices = [] } = useQuery({
    queryKey: ["admin", "offices"],
    queryFn: async () => {
      if (!isSuperAdmin) return []
      const { data, error } = await supabase
        .from("offices")
        .select("id, name, slug, status, created_at")
        .order("created_at", { ascending: false })
      if (error) throw error
      return data ?? []
    },
    enabled: isSuperAdmin,
  })

  useEffect(() => {
    if (!officeServer) return
    setServerForm({
      public_base_url: officeServer.public_base_url ?? "",
      base_path: officeServer.base_path ?? "",
      connector_version: officeServer.connector_version ?? "",
      min_supported_connector_version: officeServer.min_supported_connector_version ?? "",
      status: officeServer.status ?? "pending",
    })
  }, [officeServer])

  const userCountLabel = useMemo(() => {
    if (usersLoading) return "Carregando usuários"
    return `${officeUsers.length} usuário(s)`
  }, [officeUsers.length, usersLoading])

  const isBootstrappingAdmin =
    canAccessAdmin &&
    !!officeId &&
    (officeLoading || !officeContext)

  const openCreateUserDialog = () => {
    setEditingUser(null)
    setUserForm(emptyUserForm())
    setCreateUserOpen(true)
  }

  const openEditUserDialog = (user: AdminUser) => {
    setEditingUser(user)
    setUserForm({
      username: user.username ?? "",
      email: user.email ?? "",
      password: "",
      role: user.role ?? "user",
      office_role: normalizeOfficeRole(user.office_role),
      panel_access: { ...defaultPanelAccess, ...(user.panel_access ?? {}) },
    })
    setEditUserOpen(true)
  }

  const handleCreateOffice = async () => {
    setCreatingOffice(true)
    try {
      const result = await createPrimeiroEscritorio(officeForm)
      setLastConnectorSecret(result.connector_secret)
      setOfficeForm(emptyOfficeForm())
      toast.success("Primeiro escritório criado.")
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["admin", "offices"] }),
        queryClient.invalidateQueries({ queryKey: ["admin", "users"] }),
        queryClient.invalidateQueries({ queryKey: ["admin", "office-context"] }),
      ])
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Erro ao criar escritório")
    } finally {
      setCreatingOffice(false)
    }
  }

  const handleCreateUser = async () => {
    setSavingUser(true)
    try {
      await callAdminFunction("create-user", {
        username: userForm.username.trim(),
        email: userForm.email.trim(),
        password: userForm.password,
        role: isSuperAdmin ? userForm.role : "user",
        office_role: userForm.office_role,
        panel_access: userForm.panel_access,
        office_id: officeId,
      })
      setCreateUserOpen(false)
      setUserForm(emptyUserForm())
      toast.success("Usuário criado.")
      await queryClient.invalidateQueries({ queryKey: ["admin", "users"] })
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Erro ao criar usuário")
    } finally {
      setSavingUser(false)
    }
  }

  const handleUpdateUser = async () => {
    if (!editingUser) return
    setSavingUser(true)
    try {
      await callAdminFunction("update-user", {
        user_id: editingUser.id,
        username: userForm.username.trim(),
        email: userForm.email.trim(),
        password: userForm.password || undefined,
        role: isSuperAdmin ? userForm.role : undefined,
        office_role: userForm.office_role,
        panel_access: userForm.panel_access,
        office_id: editingUser.office_id ?? officeId,
      })
      setEditUserOpen(false)
      setEditingUser(null)
      toast.success("Usuário atualizado.")
      await queryClient.invalidateQueries({ queryKey: ["admin", "users"] })
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Erro ao atualizar usuário")
    } finally {
      setSavingUser(false)
    }
  }

  const handleSaveServer = async () => {
    setSavingServer(true)
    try {
      await updateCurrentOfficeServer({
        public_base_url: serverForm.public_base_url.trim(),
        base_path: serverForm.base_path.trim(),
        connector_version: serverForm.connector_version.trim() || null,
        min_supported_connector_version: serverForm.min_supported_connector_version.trim() || null,
        status: serverForm.status,
      })
      toast.success("Servidor do escritório atualizado.")
      await queryClient.invalidateQueries({ queryKey: ["admin", "office-server"] })
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Erro ao atualizar servidor")
    } finally {
      setSavingServer(false)
    }
  }

  const handleSetOfficeStatus = async (office: OfficeRow, status: "active" | "inactive") => {
    setOfficeStatusLoading(office.id)
    try {
      await setOfficeStatus(office.id, status)
      toast.success(status === "inactive" ? "Escritório inativado. Os usuários não poderão fazer login." : "Escritório reativado.")
      await queryClient.invalidateQueries({ queryKey: ["admin", "offices"] })
      await queryClient.invalidateQueries({ queryKey: ["admin", "office-context"] })
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Erro ao alterar status")
    } finally {
      setOfficeStatusLoading(null)
    }
  }

  const handleDeleteOffice = async () => {
    if (!officeToDelete) return
    const slug = deleteConfirmSlug.trim().toLowerCase()
    if (slug !== (officeToDelete.slug ?? "").trim().toLowerCase()) {
      toast.error("Digite o slug do escritório exatamente como exibido para confirmar.")
      return
    }
    setDeleteLoading(true)
    try {
      await deleteOffice(officeToDelete.id, slug)
      toast.success("Escritório excluído permanentemente.")
      setOfficeToDelete(null)
      setDeleteConfirmSlug("")
      await queryClient.invalidateQueries({ queryKey: ["admin", "offices"] })
      await queryClient.invalidateQueries({ queryKey: ["admin", "office-context"] })
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Erro ao excluir escritório")
    } finally {
      setDeleteLoading(false)
    }
  }

  return (
    <div className="space-y-6">
      <section className="grid gap-4 xl:grid-cols-3">
        <GlassCard className="p-5 xl:col-span-2">
          <div className="flex items-start justify-between gap-4">
            <div className="space-y-2">
              <div className="inline-flex items-center gap-2 rounded-full border border-border px-3 py-1 text-xs text-muted-foreground">
                <Shield className="h-3.5 w-3.5" />
                Modelo multi-tenant por escritório
              </div>
              <h1 className="text-2xl font-semibold font-display">Administração</h1>
              <p className="max-w-2xl text-sm text-muted-foreground">
                Auth por email, isolamento por `office_id`, branding privado, servidor por escritório e onboarding
                pelo wizard `Primeiro Escritório`.
              </p>
            </div>
            <div className="text-right text-sm">
              <p className="font-medium">{profile?.username ?? "Usuário"}</p>
              <p className="text-muted-foreground">{isSuperAdmin ? "super_admin" : officeRole ?? "sem escritório"}</p>
            </div>
          </div>
        </GlassCard>

        <GlassCard className="p-5">
          <div className="space-y-3">
            <div className="flex items-center gap-2 text-sm font-medium">
              <Building2 className="h-4 w-4 text-primary-icon" />
              Contexto atual
            </div>
            {isBootstrappingAdmin ? (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                Carregando escritório...
              </div>
            ) : officeContext ? (
              <>
                <div>
                  <p className="text-lg font-semibold">{officeContext.officeName}</p>
                  <p className="text-xs text-muted-foreground">{officeContext.officeSlug}</p>
                </div>
                <div className="flex items-center gap-2">
                  <StatusBadge value={officeContext.officeStatus} />
                  <Badge variant="outline">{getOfficeRoleLabel(officeContext.membershipRole)}</Badge>
                </div>
                <p className="text-xs text-muted-foreground">{userCountLabel}</p>
              </>
            ) : (
              <p className="text-sm text-muted-foreground">
                Nenhum escritório ativo no contexto atual. O super admin ainda consegue executar o wizard inicial.
              </p>
            )}
          </div>
        </GlassCard>
      </section>

      {isSuperAdmin ? (
        <GlassCard className="p-5">
          <div className="mb-4 flex items-center gap-2 text-sm font-medium">
            <KeyRound className="h-4 w-4 text-primary-icon" />
            Primeiro Escritório
          </div>
          <div className="grid gap-4 lg:grid-cols-2">
            <div className="space-y-4">
              <div className="grid gap-4 md:grid-cols-2">
                <div className="space-y-2">
                  <Label>Nome do escritório</Label>
                  <Input
                    value={officeForm.office_name}
                    onChange={(event) => setOfficeForm((current) => ({ ...current, office_name: event.target.value }))}
                    placeholder="Ex.: Escritório Central"
                    disabled={creatingOffice}
                  />
                </div>
                <div className="space-y-2">
                  <Label>Slug</Label>
                  <Input
                    value={officeForm.office_slug}
                    onChange={(event) =>
                      setOfficeForm((current) => ({
                        ...current,
                        office_slug: event.target.value.toLowerCase().replace(/\s+/g, "-"),
                      }))
                    }
                    placeholder="escritorio-central"
                    disabled={creatingOffice}
                  />
                </div>
                <div className="space-y-2">
                  <Label>Admin do escritório</Label>
                  <Input
                    value={officeForm.admin_username}
                    onChange={(event) => setOfficeForm((current) => ({ ...current, admin_username: event.target.value }))}
                    placeholder="Ex.: Paula Martins"
                    disabled={creatingOffice}
                  />
                </div>
                <div className="space-y-2">
                  <Label>Email do admin</Label>
                  <Input
                    type="email"
                    value={officeForm.admin_email}
                    onChange={(event) => setOfficeForm((current) => ({ ...current, admin_email: event.target.value }))}
                    placeholder="paula@escritorio.com"
                    disabled={creatingOffice}
                  />
                </div>
                <div className="space-y-2">
                  <Label>Senha inicial</Label>
                  <Input
                    type="password"
                    value={officeForm.admin_password}
                    onChange={(event) => setOfficeForm((current) => ({ ...current, admin_password: event.target.value }))}
                    placeholder="Obrigatória"
                    disabled={creatingOffice}
                  />
                </div>
                <div className="space-y-2">
                  <Label>URL pública da VM</Label>
                  <Input
                    value={officeForm.public_base_url}
                    onChange={(event) => setOfficeForm((current) => ({ ...current, public_base_url: event.target.value }))}
                    placeholder="https://vm-01.suaempresa.com"
                    disabled={creatingOffice}
                  />
                </div>
                <div className="space-y-2 md:col-span-2">
                  <Label>Base path</Label>
                  <Input
                    value={officeForm.base_path}
                    onChange={(event) => setOfficeForm((current) => ({ ...current, base_path: event.target.value }))}
                    placeholder="C:\\Users\\ROBO\\Documents\\EMPRESAS"
                    disabled={creatingOffice}
                  />
                </div>
                <div className="space-y-2">
                  <Label>Versão do conector</Label>
                  <Input
                    value={officeForm.connector_version ?? ""}
                    onChange={(event) => setOfficeForm((current) => ({ ...current, connector_version: event.target.value }))}
                    placeholder="1.0.0"
                    disabled={creatingOffice}
                  />
                </div>
                <div className="space-y-2">
                  <Label>Versão mínima suportada</Label>
                  <Input
                    value={officeForm.min_supported_connector_version ?? ""}
                    onChange={(event) =>
                      setOfficeForm((current) => ({
                        ...current,
                        min_supported_connector_version: event.target.value,
                      }))
                    }
                    placeholder="1.0.0"
                    disabled={creatingOffice}
                  />
                </div>
              </div>
              <Button onClick={() => void handleCreateOffice()} disabled={creatingOffice}>
                {creatingOffice ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Plus className="mr-2 h-4 w-4" />}
                Criar escritório inicial
              </Button>
            </div>

            <div className="space-y-4">
              <div className="rounded-xl border border-border bg-muted/20 p-4">
                <p className="text-sm font-medium">Segredo do conector</p>
                <p className="mt-1 text-xs text-muted-foreground">
                  O segredo é exibido uma única vez. Armazene no conector da VM depois de executar o wizard.
                </p>
                <div className="mt-3 rounded-lg bg-background p-3 font-mono text-xs break-all">
                  {lastConnectorSecret ?? "Ainda não gerado."}
                </div>
              </div>

              <div className="rounded-xl border border-border bg-muted/20 p-4">
                <p className="text-sm font-medium">Escritórios criados</p>
                <div className="mt-3 space-y-2">
                  {offices.length === 0 ? (
                    <p className="text-sm text-muted-foreground">Nenhum escritório criado ainda.</p>
                  ) : (
                    offices.map((office) => (
                      <div key={office.id} className="flex flex-wrap items-center justify-between gap-2 rounded-lg border border-border px-3 py-2">
                        <div>
                          <p className="text-sm font-medium">{office.name}</p>
                          <p className="text-xs text-muted-foreground font-mono">{office.slug}</p>
                        </div>
                        <div className="flex items-center gap-2">
                          <StatusBadge value={office.status} />
                          {office.status === "active" ? (
                            <Button
                              type="button"
                              variant="outline"
                              size="sm"
                              onClick={() => {
                                if (window.confirm(`Inativar o escritório "${office.name}"? Os usuários não poderão fazer login até reativar.`)) {
                                  void handleSetOfficeStatus(office as OfficeRow, "inactive")
                                }
                              }}
                              disabled={!!officeStatusLoading}
                            >
                              {officeStatusLoading === office.id ? (
                                <Loader2 className="h-4 w-4 animate-spin" />
                              ) : (
                                <PauseCircle className="h-4 w-4" />
                              )}
                              <span className="ml-1">Inativar</span>
                            </Button>
                          ) : office.status === "inactive" ? (
                            <Button
                              type="button"
                              variant="outline"
                              size="sm"
                              onClick={() => void handleSetOfficeStatus(office as OfficeRow, "active")}
                              disabled={!!officeStatusLoading}
                            >
                              {officeStatusLoading === office.id ? (
                                <Loader2 className="h-4 w-4 animate-spin" />
                              ) : (
                                <PlayCircle className="h-4 w-4" />
                              )}
                              <span className="ml-1">Reativar</span>
                            </Button>
                          ) : null}
                          <Button
                            type="button"
                            variant="destructive"
                            size="sm"
                            onClick={() => {
                              setOfficeToDelete(office as OfficeRow)
                              setDeleteConfirmSlug("")
                            }}
                          >
                            <Trash2 className="h-4 w-4" />
                            <span className="ml-1">Excluir</span>
                          </Button>
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </div>

              <Dialog open={!!officeToDelete} onOpenChange={(open) => !open && setOfficeToDelete(null)}>
                <DialogContent className="sm:max-w-md">
                  <DialogHeader>
                    <DialogTitle>Excluir escritório permanentemente?</DialogTitle>
                    <DialogDescription>
                      Esta ação não pode ser desfeita. Serão excluídos em cascata: o escritório, membros do escritório,
                      empresas, documentos fiscais, arquivos e todos os dados relacionados. Para confirmar, digite o
                      slug do escritório abaixo.
                    </DialogDescription>
                  </DialogHeader>
                  {officeToDelete && (
                    <>
                      <p className="text-sm font-medium text-destructive">
                        Escritório: {officeToDelete.name} — slug: <span className="font-mono">{officeToDelete.slug}</span>
                      </p>
                      <Input
                        placeholder="Digite o slug para confirmar"
                        value={deleteConfirmSlug}
                        onChange={(e) => setDeleteConfirmSlug(e.target.value)}
                        className="font-mono"
                        autoComplete="off"
                      />
                    </>
                  )}
                  <DialogFooter>
                    <Button type="button" variant="outline" onClick={() => setOfficeToDelete(null)}>
                      Cancelar
                    </Button>
                    <Button
                      type="button"
                      variant="destructive"
                      disabled={
                        !officeToDelete ||
                        deleteConfirmSlug.trim().toLowerCase() !== (officeToDelete?.slug ?? "").trim().toLowerCase() ||
                        deleteLoading
                      }
                      onClick={() => void handleDeleteOffice()}
                    >
                      {deleteLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}
                      <span className="ml-2">Excluir permanentemente</span>
                    </Button>
                  </DialogFooter>
                </DialogContent>
              </Dialog>
            </div>
          </div>
        </GlassCard>
      ) : null}

      {isBootstrappingAdmin ? (
        <GlassCard className="p-5">
          <div className="flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Carregando administração do escritório...
          </div>
        </GlassCard>
      ) : officeContext && canManageOffice ? (
        <>
          <section className="grid gap-4 xl:grid-cols-3">
            <GlassCard className="p-5 xl:col-span-2">
              <div className="mb-4 flex items-center gap-2 text-sm font-medium">
                <Users className="h-4 w-4 text-primary-icon" />
                Usuários do Escritório
              </div>
              <div className="mb-4 flex items-center justify-between gap-3">
                <p className="text-sm text-muted-foreground">
                  Usuários vinculados ao escritório atual com `office_memberships`.
                </p>
                <Button onClick={openCreateUserDialog}>
                  <Plus className="mr-2 h-4 w-4" />
                  Novo usuário
                </Button>
              </div>
              <div className="space-y-3">
                {usersLoading ? (
                  <div className="flex items-center gap-2 text-sm text-muted-foreground">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Carregando usuários...
                  </div>
                ) : officeUsers.length === 0 ? (
                  <p className="text-sm text-muted-foreground">Nenhum usuário vinculado ao escritório.</p>
                ) : (
                  officeUsers.map((user) => (
                    <div key={user.id} className="rounded-xl border border-border p-4">
                      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                        <div>
                          <p className="font-medium">{user.username || user.email || user.id}</p>
                          <p className="text-sm text-muted-foreground">{user.email || "sem email"}</p>
                          <div className="mt-2 flex flex-wrap gap-2">
                            <Badge variant="outline">{getOfficeRoleLabel(user.office_role)}</Badge>
                            <Badge variant={user.role === "super_admin" ? "default" : "secondary"}>{user.role}</Badge>
                          </div>
                        </div>
                        <Button variant="outline" onClick={() => openEditUserDialog(user)}>
                          Editar
                        </Button>
                      </div>
                    </div>
                  ))
                )}
              </div>
            </GlassCard>

            <GlassCard className="p-5">
              <div className="mb-4 flex items-center gap-2 text-sm font-medium">
                <Building2 className="h-4 w-4 text-primary-icon" />
                Resumo do Escritório
              </div>
              <div className="space-y-3 text-sm">
                <div>
                  <p className="text-muted-foreground">Escritório</p>
                  <p className="font-medium">{officeContext.officeName}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Slug</p>
                  <p className="font-mono text-xs">{officeContext.officeSlug}</p>
                </div>
                <div>
                  <p className="text-muted-foreground">Status</p>
                  <StatusBadge value={officeContext.officeStatus} />
                </div>
                <div>
                  <p className="text-muted-foreground">Empresas</p>
                  <p className="font-medium">{companies.length}</p>
                </div>
              </div>
            </GlassCard>
          </section>

          <section className="grid gap-4 xl:grid-cols-3">
            <GlassCard className="p-5 xl:col-span-2">
              <div className="mb-4 flex items-center gap-2 text-sm font-medium">
                <ServerCog className="h-4 w-4 text-primary-icon" />
                Servidor Ativo do Escritório
              </div>
              {serverLoading ? (
                <div className="flex items-center gap-2 text-sm text-muted-foreground">
                  <Loader2 className="h-4 w-4 animate-spin" />
                  Carregando servidor...
                </div>
              ) : officeServer ? (
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="space-y-2 md:col-span-2">
                    <Label>URL pública</Label>
                    <Input
                      value={serverForm.public_base_url}
                      onChange={(event) => setServerForm((current) => ({ ...current, public_base_url: event.target.value }))}
                      disabled={savingServer}
                    />
                  </div>
                  <div className="space-y-2 md:col-span-2">
                    <Label>Base path</Label>
                    <Input
                      value={serverForm.base_path}
                      onChange={(event) => setServerForm((current) => ({ ...current, base_path: event.target.value }))}
                      disabled={savingServer}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>Versão do conector</Label>
                    <Input
                      value={serverForm.connector_version}
                      onChange={(event) => setServerForm((current) => ({ ...current, connector_version: event.target.value }))}
                      disabled={savingServer}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>Versão mínima</Label>
                    <Input
                      value={serverForm.min_supported_connector_version}
                      onChange={(event) =>
                        setServerForm((current) => ({
                          ...current,
                          min_supported_connector_version: event.target.value,
                        }))
                      }
                      disabled={savingServer}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>Status</Label>
                    <Select
                      value={serverForm.status}
                      onValueChange={(value) => setServerForm((current) => ({ ...current, status: value }))}
                      disabled={savingServer}
                    >
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="pending">pending</SelectItem>
                        <SelectItem value="online">online</SelectItem>
                        <SelectItem value="offline">offline</SelectItem>
                        <SelectItem value="error">error</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  <div className="space-y-2">
                    <Label>Último heartbeat</Label>
                    <div className="rounded-md border border-border px-3 py-2 text-sm">
                      {officeServer.last_seen_at ? new Date(officeServer.last_seen_at).toLocaleString("pt-BR") : "Nunca"}
                    </div>
                  </div>
                  <div className="md:col-span-2">
                    <Button onClick={() => void handleSaveServer()} disabled={savingServer}>
                      {savingServer ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                      Salvar servidor
                    </Button>
                  </div>
                </div>
              ) : (
                <p className="text-sm text-muted-foreground">Nenhum servidor ativo encontrado para o escritório.</p>
              )}
            </GlassCard>

            <GlassCard className="p-5">
              <div className="mb-4 text-sm font-medium">Leituras rápidas</div>
              <div className="space-y-3 text-sm text-muted-foreground">
                <p>Login do app já usa email + senha nativo do Supabase.</p>
                <p>Branding agora é privado em `branding-assets` com URLs assinadas.</p>
                <p>Downloads passam pela function `office-server`, sem `SERVER_API_URL` no browser.</p>
              </div>
            </GlassCard>
          </section>

          <section className="grid gap-4 xl:grid-cols-2">
            <AdminBrandingBlock />
            <AdminFolderStructure isSuperAdmin={canManageOffice} />
          </section>

          <section className="grid gap-4">
            <AdminFileRetention isSuperAdmin={canManageOffice} />
          </section>

          <section className="grid gap-4 xl:grid-cols-2">
            <AdminRobotsList isSuperAdmin={canManageOffice} />
            <AdminScheduler isSuperAdmin={canManageOffice} />
          </section>
        </>
      ) : !isSuperAdmin ? (
        <GlassCard className="p-5">
          <p className="text-sm text-muted-foreground">
            Seu usuário precisa ser `owner` ou `admin` do escritório para acessar a operação administrativa.
          </p>
        </GlassCard>
      ) : null}

      <UserDialog
        open={createUserOpen}
        onOpenChange={setCreateUserOpen}
        title="Novo usuário do escritório"
        form={userForm}
        setForm={setUserForm}
        onSubmit={handleCreateUser}
        submitting={savingUser}
        allowPlatformRole={isSuperAdmin}
        requirePassword
      />

      <UserDialog
        open={editUserOpen}
        onOpenChange={setEditUserOpen}
        title="Editar usuário do escritório"
        form={userForm}
        setForm={setUserForm}
        onSubmit={handleUpdateUser}
        submitting={savingUser}
        allowPlatformRole={isSuperAdmin}
        requirePassword={false}
      />
    </div>
  )
}
