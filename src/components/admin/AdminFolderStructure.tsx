import { useState } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import {
  getFolderStructureTree,
  createFolderNode,
  updateFolderNode,
  deleteFolderNode,
} from "@/services/folderStructureService"
import type { FolderStructureNodeTree } from "@/types/folderStructure"
import type { DateRule } from "@/types/folderStructure"
import { GlassCard } from "@/components/dashboard/GlassCard"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Folder,
  FolderOpen,
  ChevronRight,
  Plus,
  Pencil,
  Trash2,
  Loader2,
} from "lucide-react"
import { toast } from "sonner"

const DATE_RULE_OPTIONS: { value: DateRule; label: string }[] = [
  { value: null, label: "Nenhuma" },
  { value: "year", label: "Ano" },
  { value: "year_month", label: "Ano / Mês" },
  { value: "year_month_day", label: "Ano / Mês / Dia" },
]

function FolderTreeItem({
  node,
  depth,
  onAddChild,
  onEdit,
  onDelete,
  isSuperAdmin,
}: {
  node: FolderStructureNodeTree
  depth: number
  onAddChild: (parentId: string) => void
  onEdit: (node: FolderStructureNodeTree) => void
  onDelete: (node: FolderStructureNodeTree) => void
  isSuperAdmin: boolean
}) {
  const [open, setOpen] = useState(depth < 1)
  const hasChildren = node.children.length > 0

  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <div className="flex items-center gap-1 py-0.5 rounded group" style={{ paddingLeft: `${depth * 16}px` }}>
        <CollapsibleTrigger asChild>
          <button
            type="button"
            className="flex items-center gap-1 min-w-0 flex-1 text-left hover:bg-muted/50 rounded px-1 -mx-1 py-0.5"
          >
            {hasChildren ? (
              <ChevronRight className={`h-4 w-4 shrink-0 transition-transform ${open ? "rotate-90" : ""}`} />
            ) : (
              <span className="w-4 shrink-0" />
            )}
            {hasChildren ? (
              <FolderOpen className="h-4 w-4 shrink-0 text-amber-600 dark:text-amber-400" />
            ) : (
              <Folder className="h-4 w-4 shrink-0 text-muted-foreground" />
            )}
            <span className="text-sm font-medium truncate">{node.name}</span>
            {node.date_rule && (
              <span className="text-[10px] bg-primary/15 text-primary-icon rounded px-1.5 py-0.5 shrink-0">
                {node.date_rule === "year" ? "Ano" : node.date_rule === "year_month" ? "Ano/Mês" : "Ano/Mês/Dia"}
              </span>
            )}
          </button>
        </CollapsibleTrigger>
        {isSuperAdmin && (
          <div className="flex items-center gap-0.5 opacity-0 group-hover:opacity-100 transition-opacity">
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="h-7 w-7"
              onClick={() => onAddChild(node.id)}
              aria-label="Adicionar subpasta"
            >
              <Plus className="h-3.5 w-3.5" />
            </Button>
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="h-7 w-7"
              onClick={() => onEdit(node)}
              aria-label="Editar"
            >
              <Pencil className="h-3.5 w-3.5" />
            </Button>
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="h-7 w-7 text-destructive hover:text-destructive"
              onClick={() => onDelete(node)}
              aria-label="Excluir"
            >
              <Trash2 className="h-3.5 w-3.5" />
            </Button>
          </div>
        )}
      </div>
      <CollapsibleContent>
        {node.children.map((child) => (
          <FolderTreeItem
            key={child.id}
            node={child}
            depth={depth + 1}
            onAddChild={onAddChild}
            onEdit={onEdit}
            onDelete={onDelete}
            isSuperAdmin={isSuperAdmin}
          />
        ))}
      </CollapsibleContent>
    </Collapsible>
  )
}

export function AdminFolderStructure({ isSuperAdmin }: { isSuperAdmin: boolean }) {
  const queryClient = useQueryClient()
  const [addParentId, setAddParentId] = useState<string | null | undefined>(undefined)
  // undefined = dialog closed; null = add root; string = add child under that id
  const [editingNode, setEditingNode] = useState<FolderStructureNodeTree | null>(null)
  const [deletingNode, setDeletingNode] = useState<FolderStructureNodeTree | null>(null)
  const [newName, setNewName] = useState("")
  const [newSlug, setNewSlug] = useState("")
  const [newDateRule, setNewDateRule] = useState<DateRule>(null)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState("")

  const { data: tree = [], isLoading } = useQuery({
    queryKey: ["folder-structure-tree"],
    queryFn: getFolderStructureTree,
  })

  const invalidate = () => queryClient.invalidateQueries({ queryKey: ["folder-structure-tree"] })

  const openAdd = (parentId: string | null) => {
    setAddParentId(parentId)
    setNewName("")
    setNewSlug("")
    setNewDateRule(null)
    setError("")
  }

  const openEdit = (node: FolderStructureNodeTree) => {
    setEditingNode(node)
    setNewName(node.name)
    setNewSlug(node.slug ?? "")
    setNewDateRule(node.date_rule)
    setError("")
  }

  const handleAdd = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!newName.trim()) {
      setError("Nome é obrigatório")
      return
    }
    setSaving(true)
    setError("")
    try {
      await createFolderNode({
        parent_id: addParentId === null ? undefined : addParentId ?? undefined,
        name: newName.trim(),
        slug: newSlug.trim() || null,
        date_rule: newDateRule,
        position: 0,
      })
      invalidate()
      setAddParentId(undefined)
      toast.success("Pasta adicionada")
    } catch (err) {
      setError(err instanceof Error ? err.message : "Erro ao criar")
      toast.error("Falha ao criar pasta")
    } finally {
      setSaving(false)
    }
  }

  const handleEdit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!editingNode || !newName.trim()) return
    setSaving(true)
    setError("")
    try {
      await updateFolderNode(editingNode.id, {
        name: newName.trim(),
        slug: newSlug.trim() || null,
        date_rule: newDateRule,
      })
      invalidate()
      setEditingNode(null)
      toast.success("Pasta atualizada")
    } catch (err) {
      setError(err instanceof Error ? err.message : "Erro ao salvar")
      toast.error("Falha ao atualizar")
    } finally {
      setSaving(false)
    }
  }

  const handleDelete = async () => {
    if (!deletingNode) return
    setSaving(true)
    try {
      await deleteFolderNode(deletingNode.id)
      invalidate()
      setDeletingNode(null)
      toast.success("Pasta removida")
    } catch (err) {
      toast.error(err instanceof Error ? err.message : "Falha ao excluir")
    } finally {
      setSaving(false)
    }
  }

  return (
    <>
      <GlassCard className="overflow-hidden">
        <div className="p-4 border-b border-border flex items-center justify-between">
          <h3 className="text-sm font-semibold font-display">Estrutura de pastas</h3>
          {isSuperAdmin && (
            <Button size="sm" variant="outline" onClick={() => openAdd(null)}>
              <Plus className="h-3.5 w-3.5 mr-1" />
              Adicionar raiz
            </Button>
          )}
        </div>
        <div className="p-3 text-xs text-muted-foreground border-b border-border">
          Base por empresa: <code className="bg-muted px-1 rounded">EMPRESAS/&#123;nome_empresa&#125;/</code> + segmentos abaixo. Robôs usam esta árvore para salvar arquivos.
        </div>
        <div className="p-3 max-h-[400px] overflow-y-auto">
          {isLoading ? (
            <div className="flex items-center gap-2 py-4 text-muted-foreground text-sm">
              <Loader2 className="h-4 w-4 animate-spin" />
              Carregando...
            </div>
          ) : tree.length === 0 ? (
            <div className="py-6 text-center text-muted-foreground text-sm">
              Nenhuma pasta. Execute a migration 00000005 para criar a estrutura padrão.
            </div>
          ) : (
            tree.map((node) => (
              <FolderTreeItem
                key={node.id}
                node={node}
                depth={0}
                onAddChild={(id) => openAdd(id)}
                onEdit={openEdit}
                onDelete={(n) => setDeletingNode(n)}
                isSuperAdmin={isSuperAdmin}
              />
            ))
          )}
        </div>
      </GlassCard>

      <Dialog open={addParentId !== undefined} onOpenChange={(open) => !open && !saving && setAddParentId(undefined)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{addParentId !== null ? "Nova subpasta" : "Nova pasta raiz"}</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleAdd} className="space-y-4">
            <div className="space-y-2">
              <Label>Nome</Label>
              <Input value={newName} onChange={(e) => setNewName(e.target.value)} placeholder="Ex.: FISCAL" required disabled={saving} />
            </div>
            <div className="space-y-2">
              <Label>Slug (opcional — usado no path)</Label>
              <Input value={newSlug} onChange={(e) => setNewSlug(e.target.value)} placeholder="Igual ao nome se vazio" disabled={saving} />
            </div>
            <div className="space-y-2">
              <Label>Segmentação por data</Label>
              <Select value={newDateRule ?? "none"} onValueChange={(v) => setNewDateRule(v === "none" ? null : (v as DateRule))} disabled={saving}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {DATE_RULE_OPTIONS.map((o) => (
                    <SelectItem key={String(o.value)} value={o.value ?? "none"}>{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            {error && <p className="text-sm text-destructive">{error}</p>}
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => setAddParentId(undefined)} disabled={saving}>Cancelar</Button>
              <Button type="submit" disabled={saving}>{saving ? "Salvando..." : "Criar"}</Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog open={!!editingNode} onOpenChange={(open) => !open && !saving && setEditingNode(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Editar pasta</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleEdit} className="space-y-4">
            <div className="space-y-2">
              <Label>Nome</Label>
              <Input value={newName} onChange={(e) => setNewName(e.target.value)} required disabled={saving} />
            </div>
            <div className="space-y-2">
              <Label>Slug (opcional)</Label>
              <Input value={newSlug} onChange={(e) => setNewSlug(e.target.value)} disabled={saving} />
            </div>
            <div className="space-y-2">
              <Label>Segmentação por data</Label>
              <Select value={newDateRule ?? "none"} onValueChange={(v) => setNewDateRule(v === "none" ? null : (v as DateRule))} disabled={saving}>
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {DATE_RULE_OPTIONS.map((o) => (
                    <SelectItem key={String(o.value)} value={o.value ?? "none"}>{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            {error && <p className="text-sm text-destructive">{error}</p>}
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => setEditingNode(null)} disabled={saving}>Cancelar</Button>
              <Button type="submit" disabled={saving}>{saving ? "Salvando..." : "Salvar"}</Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog open={!!deletingNode} onOpenChange={(open) => !open && setDeletingNode(null)}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Excluir pasta</DialogTitle>
          </DialogHeader>
          <p className="text-sm text-muted-foreground">
            Excluir &quot;{deletingNode?.name}&quot; e todas as subpastas? Esta ação não pode ser desfeita.
          </p>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setDeletingNode(null)} disabled={saving}>Cancelar</Button>
            <Button type="button" variant="destructive" onClick={handleDelete} disabled={saving}>
              {saving ? "Excluindo..." : "Excluir"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}
