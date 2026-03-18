import { useState, useEffect } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { getRobots, updateRobot } from "@/services/robotsService"
import { getRobotGoianiaSkipIss, setRobotGoianiaSkipIss } from "@/services/adminSettingsService"
import type { Robot } from "@/services/robotsService"
import {
  getFolderStructureFlat,
  buildFolderTree,
} from "@/services/folderStructureService"
import type { FolderStructureNodeTree } from "@/types/folderStructure"
import { pathSegmentsToNode } from "@/types/folderStructure"
import type { FolderStructureNodeRow } from "@/types/folderStructure"
import { GlassCard } from "@/components/dashboard/GlassCard"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { Bot, Pencil, Loader2, Circle, Folder, FolderOpen, ChevronRight } from "lucide-react"
import { toast } from "sonner"
import { format } from "date-fns"
import { ptBR } from "date-fns/locale"
import { getDefaultNotesMode, getNotesModeOptions, isNotesModeCompatible } from "@/lib/robotNotes"
import type { FiscalNotesKind, RobotNotesMode } from "@/types/database"
import type { CompanySefazLogin } from "@/services/companiesService"
import { SefazLoginsField, sanitizeSefazLogins } from "@/components/companies/SefazLoginsField"

function statusLabel(s: Robot["status"]): string {
  switch (s) {
    case "active":
      return "Ativo"
    case "inactive":
      return "Inativo"
    case "processing":
      return "Executando"
    default:
      return s
  }
}

function statusClass(s: Robot["status"]): string {
  switch (s) {
    case "active":
      return "bg-success/20 text-success"
    case "inactive":
      return "bg-muted text-muted-foreground"
    case "processing":
      return "bg-amber-500/20 text-amber-600 dark:text-amber-400"
    default:
      return "bg-muted text-muted-foreground"
  }
}

function DepartmentTreeItem({
  node,
  depth,
  flatNodes,
  selectedPath,
  onSelect,
}: {
  node: FolderStructureNodeTree
  depth: number
  flatNodes: FolderStructureNodeRow[]
  selectedPath: string
  onSelect: (path: string) => void
}) {
  const [open, setOpen] = useState(depth < 2)
  const hasChildren = node.children.length > 0
  const path = pathSegmentsToNode(flatNodes, node.id).join("/")
  const isSelected = path === selectedPath

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <div className="flex items-center gap-1 py-0.5 rounded group" style={{ paddingLeft: `${depth * 14}px` }}>
        <CollapsibleTrigger asChild>
          <button
            type="button"
            className="flex items-center gap-1 min-w-0 flex-1 text-left hover:bg-muted/50 rounded px-1 -mx-1 py-0.5"
          >
            {hasChildren ? (
              <ChevronRight className={`h-3.5 w-3.5 shrink-0 transition-transform ${open ? "rotate-90" : ""}`} />
            ) : (
              <span className="w-3.5 shrink-0" />
            )}
            {hasChildren ? (
              <FolderOpen className="h-3.5 w-3.5 shrink-0 text-amber-600 dark:text-amber-400" />
            ) : (
              <Folder className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
            )}
            <span className={`text-xs truncate ${isSelected ? "font-semibold text-primary-icon" : ""}`}>{node.name}</span>
          </button>
        </CollapsibleTrigger>
        <Button
          type="button"
          variant="ghost"
          size="sm"
          className="h-6 text-[10px] shrink-0"
          onClick={(e) => {
            e.stopPropagation()
            onSelect(path)
          }}
        >
          Selecionar
        </Button>
      </div>
      <CollapsibleContent>
        {node.children.map((child) => (
          <DepartmentTreeItem
            key={child.id}
            node={child}
            depth={depth + 1}
            flatNodes={flatNodes}
            selectedPath={selectedPath}
            onSelect={onSelect}
          />
        ))}
      </CollapsibleContent>
    </Collapsible>
  )
}

export function AdminRobotsList({
  isSuperAdmin,
  robots: robotsProp,
}: {
  isSuperAdmin: boolean
  robots?: Robot[]
}) {
  const queryClient = useQueryClient()
  const [editing, setEditing] = useState<Robot | null>(null)
  const [displayName, setDisplayName] = useState("")
  const [segmentPath, setSegmentPath] = useState("")
  const [isFiscalNotesRobot, setIsFiscalNotesRobot] = useState(false)
  const [fiscalNotesKind, setFiscalNotesKind] = useState<FiscalNotesKind>("nfs")
  const [notesMode, setNotesMode] = useState<RobotNotesMode>("recebidas")
  const [dateExecutionMode, setDateExecutionMode] = useState<"competencia" | "interval">("interval")
  const [initialPeriodStart, setInitialPeriodStart] = useState("")
  const [initialPeriodEnd, setInitialPeriodEnd] = useState("")
  const [globalLogins, setGlobalLogins] = useState<CompanySefazLogin[]>([])
  const [useGoianiaPortalLogin, setUseGoianiaPortalLogin] = useState(false)
  const [skipIssDebts, setSkipIssDebts] = useState(false)
  const [saving, setSaving] = useState(false)

  const isGoianiaTaxasImpostos = editing?.technical_id === "goiania_taxas_impostos"

  useEffect(() => {
    if (!isGoianiaTaxasImpostos) {
      setSkipIssDebts(false)
      return
    }
    getRobotGoianiaSkipIss().then(setSkipIssDebts).catch(() => setSkipIssDebts(false))
  }, [isGoianiaTaxasImpostos, editing?.id])

  const { data: queriedRobots = [], isLoading } = useQuery({
    queryKey: ["admin-robots"],
    queryFn: getRobots,
    refetchOnWindowFocus: true,
    refetchInterval: 5000,
    enabled: !robotsProp,
    staleTime: 5000,
  })
  const robots = robotsProp ?? queriedRobots

  const { data: flatNodes = [] } = useQuery({
    queryKey: ["folder-structure-flat"],
    queryFn: getFolderStructureFlat,
  })
  const folderTree = flatNodes.length > 0 ? buildFolderTree(flatNodes) : []

  const handleRename = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!editing || !displayName.trim()) return
    setSaving(true)
    try {
      await updateRobot(editing.id, {
        display_name: displayName.trim(),
        segment_path: segmentPath.trim() || null,
        is_fiscal_notes_robot: isFiscalNotesRobot,
        fiscal_notes_kind: isFiscalNotesRobot ? fiscalNotesKind : null,
        notes_mode: isFiscalNotesRobot ? notesMode : null,
        date_execution_mode: dateExecutionMode,
        initial_period_start: dateExecutionMode === "interval" && initialPeriodStart ? initialPeriodStart : null,
        initial_period_end: dateExecutionMode === "interval" && initialPeriodEnd ? initialPeriodEnd : null,
        global_logins: !useGoianiaPortalLogin ? [] : sanitizeSefazLogins(globalLogins),
      })
      if (isGoianiaTaxasImpostos) {
        await setRobotGoianiaSkipIss(skipIssDebts)
      }
      queryClient.invalidateQueries({ queryKey: ["admin-robots"] })
      setEditing(null)
      toast.success("Robô atualizado")
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Falha ao salvar")
    } finally {
      setSaving(false)
    }
  }

  const openRename = (r: Robot) => {
    const kind: FiscalNotesKind = r.fiscal_notes_kind ?? (r.notes_mode === "modelo_55" || r.notes_mode === "modelo_65" || r.notes_mode === "modelos_55_65" ? "nfe_nfc" : "nfs")
    setEditing(r)
    setDisplayName(r.display_name)
    setSegmentPath(r.segment_path ?? "")
    setIsFiscalNotesRobot(Boolean(r.is_fiscal_notes_robot))
    setFiscalNotesKind(kind)
    setNotesMode(isNotesModeCompatible(kind, r.notes_mode) ? r.notes_mode : getDefaultNotesMode(kind))
    setDateExecutionMode((r.date_execution_mode === "competencia" ? "competencia" : "interval") as "competencia" | "interval")
    setInitialPeriodStart(r.initial_period_start ?? "")
    setInitialPeriodEnd(r.initial_period_end ?? "")
    const nextLogins = Array.isArray(r.global_logins) ? (r.global_logins as CompanySefazLogin[]) : []
    setGlobalLogins(nextLogins)
    setUseGoianiaPortalLogin(nextLogins.length > 0)
  }

  const notesModeOptions = getNotesModeOptions(fiscalNotesKind)
  const isSefazXmlRobot = editing?.technical_id === "sefaz_xml"

  return (
    <>
      <GlassCard className="overflow-hidden">
        <div className="p-4 border-b border-border">
          <h3 className="text-sm font-semibold font-display">Robôs vinculados</h3>
          <p className="text-xs text-muted-foreground mt-0.5">
            Nome de exibição, departamento (estrutura de pastas) e modo de execução de datas.
          </p>
        </div>
        <div className="divide-y divide-border">
          {isLoading ? (
            <div className="p-4 flex items-center gap-2 text-muted-foreground text-sm">
              <Loader2 className="h-4 w-4 animate-spin" />
              Carregando...
            </div>
          ) : robots.length === 0 ? (
            <div className="p-6 text-center text-muted-foreground text-sm">
              Nenhum robô vinculado. Abra um robô configurado com .env (Supabase) para ele aparecer aqui.
            </div>
          ) : (
            robots.map((r) => (
              <div
                key={r.id}
                className="px-4 py-3 flex items-center justify-between gap-3 hover:bg-muted/30 transition-colors"
              >
                <div className="flex items-center gap-3 min-w-0">
                  <div className="rounded-lg bg-primary/10 p-2 shrink-0">
                    <Bot className="h-4 w-4 text-primary-icon" />
                  </div>
                  <div className="min-w-0">
                    <p className="text-sm font-medium truncate">{r.display_name}</p>
                    <p className="text-[10px] text-muted-foreground font-mono truncate">{r.technical_id}</p>
                    {r.segment_path && (
                      <p className="text-[10px] text-muted-foreground truncate">Departamento: {r.segment_path}</p>
                    )}
                    {r.status === "inactive" && r.last_heartbeat_at && (
                      <p className="text-[10px] text-muted-foreground mt-0.5">
                        Última vez ativo: {format(new Date(r.last_heartbeat_at), "dd/MM/yyyy 'às' HH:mm", { locale: ptBR })}
                      </p>
                    )}
                  </div>
                </div>
                <div className="flex items-center gap-2 shrink-0">
                  <span className={`inline-flex items-center gap-1 rounded px-2 py-0.5 text-[10px] font-medium ${statusClass(r.status)}`}>
                    <Circle className="h-1.5 w-1.5 fill-current" />
                    {statusLabel(r.status)}
                  </span>
                  {isSuperAdmin && (
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      className="h-8 w-8"
                      onClick={() => openRename(r)}
                      aria-label="Editar"
                    >
                      <Pencil className="h-3.5 w-3.5" />
                    </Button>
                  )}
                </div>
              </div>
            ))
          )}
        </div>
      </GlassCard>

      <Dialog open={!!editing} onOpenChange={(open) => !open && !saving && setEditing(null)}>
        <DialogContent aria-describedby={undefined} className="max-w-lg max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Editar robô</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleRename} className="space-y-4">
            <p className="text-xs text-muted-foreground">
              Identificador técnico: <code className="bg-muted px-1 rounded">{editing?.technical_id}</code>
            </p>
            <div className="space-y-2">
              <Label>Nome de exibição</Label>
              <Input
                value={displayName}
                onChange={(e) => setDisplayName(e.target.value)}
                placeholder="Ex.: NFS Padrão - VM"
                required
                disabled={saving}
              />
            </div>
            <div className="space-y-2">
              <Label>Departamento</Label>
              <p className="text-[10px] text-muted-foreground">
                Selecione na estrutura de pastas onde os arquivos do robô serão salvos (ex.: FISCAL / NFS).
              </p>
              <div className="rounded border border-input bg-muted/20 p-2 max-h-48 overflow-y-auto">
                {folderTree.length === 0 ? (
                  <p className="text-xs text-muted-foreground">Nenhuma pasta na estrutura. Configure em Estrutura de pastas.</p>
                ) : (
                  folderTree.map((node) => (
                    <DepartmentTreeItem
                      key={node.id}
                      node={node}
                      depth={0}
                      flatNodes={flatNodes}
                      selectedPath={segmentPath}
                      onSelect={setSegmentPath}
                    />
                  ))
                )}
              </div>
              {segmentPath && (
                <p className="text-xs text-primary-icon font-medium">Selecionado: {segmentPath}</p>
              )}
            </div>
            <div className="space-y-2">
              <Label>Modo de execução de datas</Label>
              <select
                value={dateExecutionMode}
                onChange={(e) => setDateExecutionMode(e.target.value as "competencia" | "interval")}
                disabled={saving}
                className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm"
              >
                <option value="competencia">Por competência</option>
                <option value="interval">Por intervalo de datas</option>
              </select>
              <p className="text-[10px] text-muted-foreground">
                {dateExecutionMode === "competencia"
                  ? "Executa para uma competência mensal (ex.: 03/2026)."
                  : "Primeira execução usa o intervalo abaixo; depois o sistema usa apenas o dia anterior (incremental)."}
              </p>
            </div>
            {dateExecutionMode === "interval" && (
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1">
                  <Label className="text-[10px]">Data inicial (primeiro intervalo)</Label>
                  <input
                    type="date"
                    value={initialPeriodStart}
                    onChange={(e) => setInitialPeriodStart(e.target.value)}
                    disabled={saving}
                    className="w-full rounded border border-input bg-background px-2 py-1.5 text-xs"
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-[10px]">Data final (primeiro intervalo)</Label>
                  <input
                    type="date"
                    value={initialPeriodEnd}
                    onChange={(e) => setInitialPeriodEnd(e.target.value)}
                    disabled={saving}
                    className="w-full rounded border border-input bg-background px-2 py-1.5 text-xs"
                  />
                </div>
              </div>
            )}
            <div className="space-y-3 rounded-lg border border-border bg-muted/20 p-3">
              <div className="flex items-start gap-3">
                <Checkbox
                  checked={isFiscalNotesRobot}
                  onCheckedChange={(checked) => {
                    const enabled = checked === true
                    setIsFiscalNotesRobot(enabled)
                    if (enabled && !isNotesModeCompatible(fiscalNotesKind, notesMode)) {
                      setNotesMode(getDefaultNotesMode(fiscalNotesKind))
                    }
                  }}
                  disabled={saving}
                />
                <div className="space-y-1">
                  <Label className="text-sm">Usa modos de notas fiscais</Label>
                  <p className="text-[10px] text-muted-foreground">
                    Ative apenas para robôs fiscais. Se desligado, o editor não exibe configuração de modo de notas.
                  </p>
                </div>
              </div>

              {isFiscalNotesRobot ? (
                <>
                  <div className="space-y-2">
                    <Label>Família fiscal</Label>
                    <select
                      value={fiscalNotesKind}
                      onChange={(e) => {
                        const nextKind = e.target.value as FiscalNotesKind
                        setFiscalNotesKind(nextKind)
                        setNotesMode(getDefaultNotesMode(nextKind))
                      }}
                      disabled={saving}
                      className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm"
                    >
                      <option value="nfs">NFS</option>
                      <option value="nfe_nfc">NFE / NFC</option>
                    </select>
                    <p className="text-[10px] text-muted-foreground">
                      NFS usa recebidas/emitidas. NFE/NFC usa modelos 55 e 65.
                    </p>
                  </div>

                  <div className="space-y-2">
                    <Label>Modo de notas</Label>
                    <select
                      value={notesMode}
                      onChange={(e) => setNotesMode(e.target.value as RobotNotesMode)}
                      disabled={saving}
                      className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm"
                    >
                      {notesModeOptions.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                    <p className="text-[10px] text-muted-foreground">
                      {notesModeOptions.find((option) => option.value === notesMode)?.description ?? "Selecione como o robô deve classificar as notas."}
                    </p>
                  </div>
                </>
              ) : (
                <p className="text-[10px] text-muted-foreground">
                  Robôs que não baixam notas fiscais não devem carregar `notes_mode`.
                </p>
              )}
            </div>
            {
              <div className="space-y-3 rounded-lg border border-border bg-muted/20 p-3">
                <div className="flex items-start gap-3">
                  <Checkbox
                    checked={useGoianiaPortalLogin}
                    onCheckedChange={(checked) => setUseGoianiaPortalLogin(checked === true)}
                    disabled={saving}
                    id="use-goiania-portal-login"
                  />
                  <div className="space-y-1">
                    <Label htmlFor="use-goiania-portal-login">Usar login da Prefeitura de Goiânia</Label>
                    <p className="text-[10px] text-muted-foreground">
                      Ative para qualquer robô que precise autenticar no portal da Prefeitura de Goiânia com CPF e senha globais.
                    </p>
                  </div>
                </div>
                {isGoianiaTaxasImpostos && (
                  <div className="flex items-start gap-3 pt-1 border-t border-border/50">
                    <Checkbox
                      checked={skipIssDebts}
                      onCheckedChange={(checked) => setSkipIssDebts(checked === true)}
                      disabled={saving}
                      id="goiania-skip-iss"
                    />
                    <div className="space-y-1">
                      <Label htmlFor="goiania-skip-iss">Não capturar débitos de ISS</Label>
                      <p className="text-[10px] text-muted-foreground">
                        Se marcado, o robô não captura débitos de ISS e não os seleciona para baixar guias. Desmarque para manter o comportamento atual (captura todos).
                      </p>
                    </div>
                  </div>
                )}
              </div>
            }
            {(isSefazXmlRobot || useGoianiaPortalLogin) && (
              <SefazLoginsField
                value={globalLogins}
                onChange={setGlobalLogins}
                disabled={saving}
                title={useGoianiaPortalLogin ? "Login da Prefeitura de Goiânia" : "Logins globais do robô"}
                description={useGoianiaPortalLogin
                  ? "Cadastre aqui o CPF e a senha do portal da Prefeitura de Goiânia que o robô vai usar na inicialização."
                  : "Cadastre os logins CPF/senha que este robô pode usar. Depois, no editar empresa, você escolhe qual login cada empresa usa."}
                defaultLabel={useGoianiaPortalLogin ? "Login padrão da Prefeitura de Goiânia" : "Login padrão global do robô"}
              />
            )}
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => setEditing(null)} disabled={saving}>
                Cancelar
              </Button>
              <Button type="submit" disabled={saving || !displayName.trim()}>
                {saving ? "Salvando..." : "Salvar"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>
    </>
  )
}
