import { useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { getCompaniesForUser, type CompanySefazLogin } from "@/services/companiesService";
import { getRobots, updateRobot } from "@/services/robotsService";
import type { Robot } from "@/services/robotsService";
import {
  getFolderStructureFlat,
  buildFolderTree,
} from "@/services/folderStructureService";
import type { FolderStructureNodeTree, FolderStructureNodeRow } from "@/types/folderStructure";
import { pathSegmentsToNode } from "@/types/folderStructure";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { RobotConfigFieldGroup } from "@/components/robots/RobotConfigFieldGroup";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Bot, ChevronRight, Circle, Folder, FolderOpen, Loader2, Pencil } from "lucide-react";
import { toast } from "sonner";
import { format } from "date-fns";
import { ptBR } from "date-fns/locale";
import { getDefaultNotesMode, getNotesModeOptions, isNotesModeCompatible } from "@/lib/robotNotes";
import {
  getRobotAdminFormSchema,
  getRobotConfigRecord,
  getRobotGlobalLogins,
  type RobotConfigTarget,
} from "@/lib/robotConfigSchemas";
import type { FiscalNotesKind, Json, RobotNotesMode } from "@/types/database";

function statusLabel(s: Robot["status"]): string {
  switch (s) {
    case "active":
      return "Ativo";
    case "inactive":
      return "Inativo";
    case "processing":
      return "Executando";
    default:
      return s;
  }
}

function statusClass(s: Robot["status"]): string {
  switch (s) {
    case "active":
      return "bg-success/20 text-success";
    case "inactive":
      return "bg-muted text-muted-foreground";
    case "processing":
      return "bg-amber-500/20 text-amber-600 dark:text-amber-400";
    default:
      return "bg-muted text-muted-foreground";
  }
}

function DepartmentTreeItem({
  node,
  depth,
  flatNodes,
  selectedPath,
  onSelect,
}: {
  node: FolderStructureNodeTree;
  depth: number;
  flatNodes: FolderStructureNodeRow[];
  selectedPath: string;
  onSelect: (path: string) => void;
}) {
  const [open, setOpen] = useState(depth < 2);
  const hasChildren = node.children.length > 0;
  const path = pathSegmentsToNode(flatNodes, node.id).join("/");
  const isSelected = path === selectedPath;

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <div className="group flex items-center gap-1 rounded py-0.5" style={{ paddingLeft: `${depth * 14}px` }}>
        <CollapsibleTrigger asChild>
          <button
            type="button"
            className="flex min-w-0 flex-1 items-center gap-1 rounded px-1 py-0.5 text-left hover:bg-muted/50"
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
            <span className={`truncate text-xs ${isSelected ? "font-semibold text-primary-icon" : ""}`}>{node.name}</span>
          </button>
        </CollapsibleTrigger>
        <Button
          type="button"
          variant="ghost"
          size="sm"
          className="h-6 shrink-0 text-[10px]"
          onClick={(event) => {
            event.stopPropagation();
            onSelect(path);
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
  );
}

export function AdminRobotsList({
  isSuperAdmin,
  robots: robotsProp,
}: {
  isSuperAdmin: boolean;
  robots?: Robot[];
}) {
  const queryClient = useQueryClient();
  const [editing, setEditing] = useState<Robot | null>(null);
  const [displayName, setDisplayName] = useState("");
  const [segmentPath, setSegmentPath] = useState("");
  const [isFiscalNotesRobot, setIsFiscalNotesRobot] = useState(false);
  const [fiscalNotesKind, setFiscalNotesKind] = useState<FiscalNotesKind>("nfs");
  const [notesMode, setNotesMode] = useState<RobotNotesMode>("recebidas");
  const [dateExecutionMode, setDateExecutionMode] = useState<"competencia" | "interval">("interval");
  const [initialPeriodStart, setInitialPeriodStart] = useState("");
  const [initialPeriodEnd, setInitialPeriodEnd] = useState("");
  const [globalLogins, setGlobalLogins] = useState<CompanySefazLogin[]>([]);
  const [adminSettingsDraft, setAdminSettingsDraft] = useState<Record<string, Json>>({});
  const [executionDefaultsDraft, setExecutionDefaultsDraft] = useState<Record<string, Json>>({});
  const [saving, setSaving] = useState(false);

  const { data: queriedRobots = [], isLoading } = useQuery({
    queryKey: ["admin-robots"],
    queryFn: getRobots,
    refetchOnWindowFocus: true,
    refetchInterval: 5000,
    enabled: !robotsProp,
    staleTime: 5000,
  });
  const robots = robotsProp ?? queriedRobots;

  const { data: flatNodes = [] } = useQuery({
    queryKey: ["folder-structure-flat"],
    queryFn: getFolderStructureFlat,
  });

  const { data: companies = [] } = useQuery({
    queryKey: ["admin-robots-companies"],
    queryFn: () => getCompaniesForUser("all"),
  });

  const folderTree = flatNodes.length > 0 ? buildFolderTree(flatNodes) : [];
  const adminFields = editing ? getRobotAdminFormSchema(editing) : [];

  const handleDynamicFieldChange = (target: RobotConfigTarget, key: string, value: Json) => {
    if (target === "execution_defaults") {
      setExecutionDefaultsDraft((prev) => ({ ...prev, [key]: value }));
      return;
    }
    if (target === "admin_settings") {
      setAdminSettingsDraft((prev) => ({ ...prev, [key]: value }));
    }
  };

  const handleRename = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!editing || !displayName.trim()) return;

    setSaving(true);
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
        global_logins: globalLogins,
        admin_settings: adminSettingsDraft,
        execution_defaults: executionDefaultsDraft,
      });
      queryClient.invalidateQueries({ queryKey: ["admin-robots"] });
      setEditing(null);
      toast.success("Robo atualizado");
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "Falha ao salvar");
    } finally {
      setSaving(false);
    }
  };

  const openRename = (robot: Robot) => {
    const kind: FiscalNotesKind =
      robot.fiscal_notes_kind ??
      (robot.notes_mode === "modelo_55" ||
      robot.notes_mode === "modelo_65" ||
      robot.notes_mode === "modelos_55_65"
        ? "nfe_nfc"
        : "nfs");

    setEditing(robot);
    setDisplayName(robot.display_name);
    setSegmentPath(robot.segment_path ?? "");
    setIsFiscalNotesRobot(Boolean(robot.is_fiscal_notes_robot));
    setFiscalNotesKind(kind);
    setNotesMode(isNotesModeCompatible(kind, robot.notes_mode) ? robot.notes_mode : getDefaultNotesMode(kind));
    setDateExecutionMode((robot.date_execution_mode === "competencia" ? "competencia" : "interval") as "competencia" | "interval");
    setInitialPeriodStart(robot.initial_period_start ?? "");
    setInitialPeriodEnd(robot.initial_period_end ?? "");
    setGlobalLogins(getRobotGlobalLogins(robot.global_logins));
    setAdminSettingsDraft(getRobotConfigRecord(robot.admin_settings));
    setExecutionDefaultsDraft(getRobotConfigRecord(robot.execution_defaults));
  };

  const notesModeOptions = getNotesModeOptions(fiscalNotesKind);

  return (
    <>
      <GlassCard className="overflow-hidden">
        <div className="border-b border-border p-4">
          <h3 className="text-sm font-semibold font-display">Robos vinculados</h3>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Nome de exibicao, pasta de salvamento, modo de datas e configuracoes genericas do robo.
          </p>
        </div>
        <div className="divide-y divide-border">
          {isLoading ? (
            <div className="flex items-center gap-2 p-4 text-sm text-muted-foreground">
              <Loader2 className="h-4 w-4 animate-spin" />
              Carregando...
            </div>
          ) : robots.length === 0 ? (
            <div className="p-6 text-center text-sm text-muted-foreground">
              Nenhum robo vinculado. Abra um robo configurado com .env para ele aparecer aqui.
            </div>
          ) : (
            robots.map((robot) => (
              <div
                key={robot.id}
                className="flex items-center justify-between gap-3 px-4 py-3 transition-colors hover:bg-muted/30"
              >
                <div className="flex min-w-0 items-center gap-3">
                  <div className="rounded-lg bg-primary/10 p-2 shrink-0">
                    <Bot className="h-4 w-4 text-primary-icon" />
                  </div>
                  <div className="min-w-0">
                    <p className="truncate text-sm font-medium">{robot.display_name}</p>
                    <p className="truncate font-mono text-[10px] text-muted-foreground">{robot.technical_id}</p>
                    {robot.segment_path ? (
                      <p className="truncate text-[10px] text-muted-foreground">Departamento: {robot.segment_path}</p>
                    ) : null}
                    {robot.status === "inactive" && robot.last_heartbeat_at ? (
                      <p className="mt-0.5 text-[10px] text-muted-foreground">
                        Ultima vez ativo: {format(new Date(robot.last_heartbeat_at), "dd/MM/yyyy 'as' HH:mm", { locale: ptBR })}
                      </p>
                    ) : null}
                  </div>
                </div>
                <div className="flex shrink-0 items-center gap-2">
                  <span className={`inline-flex items-center gap-1 rounded px-2 py-0.5 text-[10px] font-medium ${statusClass(robot.status)}`}>
                    <Circle className="h-1.5 w-1.5 fill-current" />
                    {statusLabel(robot.status)}
                  </span>
                  {isSuperAdmin ? (
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      className="h-8 w-8"
                      onClick={() => openRename(robot)}
                      aria-label="Editar"
                    >
                      <Pencil className="h-3.5 w-3.5" />
                    </Button>
                  ) : null}
                </div>
              </div>
            ))
          )}
        </div>
      </GlassCard>

      <Dialog open={Boolean(editing)} onOpenChange={(open) => !open && !saving && setEditing(null)}>
        <DialogContent aria-describedby={undefined} className="max-h-[90vh] max-w-lg overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Editar robo</DialogTitle>
          </DialogHeader>

          <form onSubmit={handleRename} className="space-y-4">
            <p className="text-xs text-muted-foreground">
              Identificador tecnico: <code className="rounded bg-muted px-1">{editing?.technical_id}</code>
            </p>

            <div className="space-y-2">
              <Label>Nome de exibicao</Label>
              <Input
                value={displayName}
                onChange={(event) => setDisplayName(event.target.value)}
                placeholder="Ex.: NFS Padrao - VM"
                required
                disabled={saving}
              />
            </div>

            <div className="space-y-2">
              <Label>Departamento</Label>
              <p className="text-[10px] text-muted-foreground">
                Selecione a estrutura de pastas onde os arquivos do robo serao salvos.
              </p>
              <div className="max-h-48 overflow-y-auto rounded border border-input bg-muted/20 p-2">
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
              {segmentPath ? <p className="text-xs font-medium text-primary-icon">Selecionado: {segmentPath}</p> : null}
            </div>

            <div className="space-y-2">
              <Label>Modo de execucao de datas</Label>
              <select
                value={dateExecutionMode}
                onChange={(event) => setDateExecutionMode(event.target.value as "competencia" | "interval")}
                disabled={saving}
                className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm"
              >
                <option value="competencia">Por competencia</option>
                <option value="interval">Por intervalo de datas</option>
              </select>
              <p className="text-[10px] text-muted-foreground">
                {dateExecutionMode === "competencia"
                  ? "Executa para uma competencia mensal."
                  : "Primeira execucao usa o intervalo abaixo; depois o sistema segue incremental."}
              </p>
            </div>

            {dateExecutionMode === "interval" ? (
              <div className="grid grid-cols-2 gap-2">
                <div className="space-y-1">
                  <Label className="text-[10px]">Data inicial</Label>
                  <input
                    type="date"
                    value={initialPeriodStart}
                    onChange={(event) => setInitialPeriodStart(event.target.value)}
                    disabled={saving}
                    className="w-full rounded border border-input bg-background px-2 py-1.5 text-xs"
                  />
                </div>
                <div className="space-y-1">
                  <Label className="text-[10px]">Data final</Label>
                  <input
                    type="date"
                    value={initialPeriodEnd}
                    onChange={(event) => setInitialPeriodEnd(event.target.value)}
                    disabled={saving}
                    className="w-full rounded border border-input bg-background px-2 py-1.5 text-xs"
                  />
                </div>
              </div>
            ) : null}

            <div className="space-y-3 rounded-lg border border-border bg-muted/20 p-3">
              <div className="flex items-start gap-3">
                <Checkbox
                  checked={isFiscalNotesRobot}
                  onCheckedChange={(checked) => {
                    const enabled = checked === true;
                    setIsFiscalNotesRobot(enabled);
                    if (enabled && !isNotesModeCompatible(fiscalNotesKind, notesMode)) {
                      setNotesMode(getDefaultNotesMode(fiscalNotesKind));
                    }
                  }}
                  disabled={saving}
                />
                <div className="space-y-1">
                  <Label className="text-sm">Usa modos de notas fiscais</Label>
                  <p className="text-[10px] text-muted-foreground">
                    Ative apenas para robos fiscais. Se desligado, o editor nao envia configuracao de notas.
                  </p>
                </div>
              </div>

              {isFiscalNotesRobot ? (
                <>
                  <div className="space-y-2">
                    <Label>Familia fiscal</Label>
                    <select
                      value={fiscalNotesKind}
                      onChange={(event) => {
                        const nextKind = event.target.value as FiscalNotesKind;
                        setFiscalNotesKind(nextKind);
                        setNotesMode(getDefaultNotesMode(nextKind));
                      }}
                      disabled={saving}
                      className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm"
                    >
                      <option value="nfs">NFS</option>
                      <option value="nfe_nfc">NFE / NFC</option>
                    </select>
                  </div>

                  <div className="space-y-2">
                    <Label>Modo de notas</Label>
                    <select
                      value={notesMode}
                      onChange={(event) => setNotesMode(event.target.value as RobotNotesMode)}
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
                      {notesModeOptions.find((option) => option.value === notesMode)?.description ?? "Selecione como o robo deve classificar as notas."}
                    </p>
                  </div>
                </>
              ) : (
                <p className="text-[10px] text-muted-foreground">
                  Robos que nao baixam notas fiscais nao devem carregar <code>notes_mode</code>.
                </p>
              )}
            </div>

            {editing ? (
              <div className="space-y-3 rounded-lg border border-border bg-muted/20 p-3">
                <div className="space-y-1">
                  <Label className="text-sm">Configuracoes dinamicas</Label>
                  <p className="text-[10px] text-muted-foreground">
                    Campos definidos no catalogo do robo. Novos robos entram aqui por schema, sem novo hardcode no front.
                  </p>
                </div>
                <RobotConfigFieldGroup
                  fields={adminFields}
                  valuesByTarget={{
                    admin_settings: adminSettingsDraft,
                    execution_defaults: executionDefaultsDraft,
                  }}
                  onChangeField={handleDynamicFieldChange}
                  globalLogins={globalLogins}
                  onChangeGlobalLogins={setGlobalLogins}
                  cityNames={companies.map((company) => company.city_name)}
                  disabled={saving}
                />
              </div>
            ) : null}

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
  );
}
