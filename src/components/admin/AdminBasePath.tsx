import { useState, useEffect } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { getBasePath, setBasePath } from "@/services/adminSettingsService"
import { GlassCard } from "@/components/dashboard/GlassCard"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { FolderRoot, Loader2 } from "lucide-react"
import { toast } from "sonner"

export function AdminBasePath({ isSuperAdmin }: { isSuperAdmin: boolean }) {
  const queryClient = useQueryClient()
  const [value, setValue] = useState("")
  const [saving, setSaving] = useState(false)

  const { data: basePath = "", isLoading } = useQuery({
    queryKey: ["admin-base-path"],
    queryFn: getBasePath,
    enabled: isSuperAdmin,
  })

  useEffect(() => {
    if (basePath) setValue(basePath)
  }, [basePath])

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault()
    setSaving(true)
    try {
      await setBasePath(value.trim())
      queryClient.invalidateQueries({ queryKey: ["admin-base-path"] })
      toast.success("Pasta base salva. A VM e os robôs passam a usar esse valor (via Supabase).")
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Erro ao salvar")
    } finally {
      setSaving(false)
    }
  }

  if (!isSuperAdmin) return null

  const current = value

  return (
    <GlassCard className="overflow-hidden">
      <div className="p-4 border-b border-border flex items-center gap-2">
        <FolderRoot className="h-4 w-4 text-primary-icon" />
        <h3 className="text-sm font-semibold font-display">Pasta base na VM</h3>
      </div>
      <div className="p-4 space-y-4">
        <p className="text-xs text-muted-foreground">
          Raiz onde ficam as pastas EMPRESAS na VM (ex.: <code className="bg-muted px-1 rounded">C:\Users\ROBO\Documents</code>).
          Os robôs e o server-api leem esse valor do Supabase; não é mais necessário definir BASE_PATH no .env de cada robô.
        </p>
        <form onSubmit={handleSave} className="space-y-2">
          <Label className="text-xs font-medium">Caminho base (global para todos os robôs)</Label>
          <Input
            value={current}
            onChange={(e) => setValue(e.target.value)}
            placeholder="C:\Users\ROBO\Documents"
            disabled={isLoading}
            className="font-mono text-xs"
          />
          <Button type="submit" size="sm" disabled={isLoading || saving || !value.trim()}>
            {saving ? <Loader2 className="h-3.5 w-3.5 animate-spin" /> : null}
            {saving ? " Salvando..." : "Salvar"}
          </Button>
        </form>
      </div>
    </GlassCard>
  )
}
