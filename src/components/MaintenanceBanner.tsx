import { AlertTriangle } from "lucide-react"

export function MaintenanceBanner() {
  return (
    <div
      role="status"
      aria-live="polite"
      className="maintenance-banner maintenance-banner__shimmer fixed inset-x-0 top-0 z-[70] h-[52px] border-b border-amber-500/25 bg-slate-950/92 text-amber-100 shadow-lg backdrop-blur"
    >
      <div className="mx-auto flex h-full max-w-7xl items-center justify-center gap-2 px-3 text-center text-xs font-medium sm:px-4 sm:text-sm">
        <AlertTriangle className="h-4 w-4 shrink-0 text-amber-300" />
        <span>Instabilidade na conexão com o Supabase. Alguns dados podem não carregar temporariamente.</span>
      </div>
    </div>
  )
}
