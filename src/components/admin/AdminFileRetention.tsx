import { useState } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import {
  getFileRetentionDays,
  setFileRetentionDays,
  runFileRetentionCleanup,
  type FileRetentionDays,
} from "@/services/adminSettingsService"
import { GlassCard } from "@/components/dashboard/GlassCard"
import { Button } from "@/components/ui/button"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Trash2, Loader2, HardDrive } from "lucide-react"
import { toast } from "sonner"

const RETENTION_OPTIONS: { value: FileRetentionDays; label: string }[] = [
  { value: 0, label: "Nunca excluir" },
  { value: 30, label: "30 dias" },
  { value: 60, label: "60 dias" },
  { value: 90, label: "90 dias" },
  { value: 120, label: "120 dias" },
]

export function AdminFileRetention({ isSuperAdmin }: { isSuperAdmin: boolean }) {
  const queryClient = useQueryClient()
  const [saving, setSaving] = useState(false)
  const [cleaning, setCleaning] = useState(false)
  const [confirmCleanupOpen, setConfirmCleanupOpen] = useState(false)

  const { data: retentionDays = 60, isLoading } = useQuery({
    queryKey: ["admin-file-retention"],
    queryFn: getFileRetentionDays,
    enabled: isSuperAdmin,
  })

  const handleSave = async (value: FileRetentionDays) => {
    setSaving(true)
    try {
      await setFileRetentionDays(value)
      queryClient.invalidateQueries({ queryKey: ["admin-file-retention"] })
      toast.success("Configuração salva. Documentos baixados há mais tempo que isso poderão ser excluídos ao rodar a limpeza.")
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Erro ao salvar")
    } finally {
      setSaving(false)
    }
  }

  const handleCleanup = async () => {
    setCleaning(true)
    try {
      const { deleted } = await runFileRetentionCleanup()
      queryClient.invalidateQueries({ queryKey: ["fiscal-documents"] })
      queryClient.invalidateQueries({ queryKey: ["fiscal-summary"] })
      queryClient.invalidateQueries({ queryKey: ["fiscal-documents-all"] })
      toast.success(deleted > 0 ? `${deleted} registro(s) excluído(s).` : "Nenhum registro antigo para excluir.")
      setConfirmCleanupOpen(false)
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Erro ao excluir")
    } finally {
      setCleaning(false)
    }
  }

  if (!isSuperAdmin) return null

  return (
    <GlassCard className="overflow-hidden">
      <div className="p-4 border-b border-border flex items-center gap-2">
        <HardDrive className="h-4 w-4 text-primary-icon" />
        <h3 className="text-sm font-semibold font-display">Retenção de arquivos</h3>
      </div>
      <div className="p-4 space-y-4">
        <p className="text-xs text-muted-foreground">
          Controle por quantos dias manter os documentos após o último download. Arquivos que não foram baixados não são excluídos. Ao clicar em &quot;Excluir agora&quot;, são removidos apenas os registros cujo último download foi há mais tempo que o período escolhido.
        </p>
        <div className="space-y-2">
          <Label className="text-xs font-medium">Manter arquivos (por data do último download)</Label>
          <Select
            value={String(retentionDays)}
            onValueChange={(v) => handleSave(Number(v) as FileRetentionDays)}
            disabled={isLoading || saving}
          >
            <SelectTrigger className="w-full max-w-[200px]">
              <SelectValue placeholder="Selecione" />
            </SelectTrigger>
            <SelectContent>
              {RETENTION_OPTIONS.map((opt) => (
                <SelectItem key={opt.value} value={String(opt.value)}>
                  {opt.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>
        <Button
          type="button"
          variant="outline"
          size="sm"
          onClick={() => setConfirmCleanupOpen(true)}
          disabled={cleaning || retentionDays === 0}
          className="gap-2"
        >
          {cleaning ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : <Trash2 className="h-3.5 w-3.5" />}
          {cleaning ? "Excluindo..." : "Excluir arquivos antigos agora"}
        </Button>
        {retentionDays === 0 && (
          <p className="text-[10px] text-muted-foreground">Com &quot;Nunca excluir&quot; a limpeza não remove nenhum registro.</p>
        )}
      </div>

      <AlertDialog open={confirmCleanupOpen} onOpenChange={setConfirmCleanupOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Excluir arquivos antigos?</AlertDialogTitle>
            <AlertDialogDescription>
              Serão removidos apenas os registros de documentos cujo <strong>último download</strong> foi há mais de <strong>{retentionDays} dias</strong>. Documentos que nunca foram baixados não são excluídos. Esta ação não pode ser desfeita.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={cleaning}>Cancelar</AlertDialogCancel>
            <AlertDialogAction
              onClick={(e) => {
                e.preventDefault()
                handleCleanup()
              }}
              disabled={cleaning}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              {cleaning ? "Excluindo..." : "Excluir"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </GlassCard>
  )
}
