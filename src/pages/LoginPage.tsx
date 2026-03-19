import { useState, useEffect } from "react"
import { useNavigate, useLocation } from "react-router-dom"
import { Lock, Loader2, AlertCircle } from "lucide-react"
import defaultLogoUrl from "@/assets/images/logo.png"
import { supabase } from "@/services/supabaseClient"
import { getProfile } from "@/services/profilesService"
import { resetBrandingInDocument } from "@/contexts/BrandingContext"

export default function LoginPage() {
  const navigate = useNavigate()
  const location = useLocation()
  const loginSubtitle = "Dashboard Web"
  const from = (location.state as { from?: string } | null)?.from
  const loginMessage = (location.state as { message?: string } | null)?.message
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [error, setError] = useState(loginMessage ?? "")
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (loginMessage) setError(loginMessage)
  }, [loginMessage])

  useEffect(() => {
    resetBrandingInDocument()
    const stored = localStorage.getItem("theme")
    const isDark = stored !== "light"
    document.documentElement.classList.toggle("dark", isDark)
    if (!stored) localStorage.setItem("theme", "dark")
  }, [])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError("")
    setLoading(true)
    try {
      const { data, error: signInError } = await supabase.auth.signInWithPassword({
        email: email.trim(),
        password,
      })
      if (signInError) throw signInError

      const userId = data.user?.id
      if (!userId) throw new Error("Usuário não retornado")

      const profile = await getProfile(userId)
      setLoading(false)

      if (!profile) {
        navigate(from && from !== "/login" ? from : "/dashboard", { replace: true })
        return
      }

      if (profile.office_id && profile.office_status === "inactive") {
        await supabase.auth.signOut()
        setError(`O escritório "${profile.office_name ?? "deste usuário"}" está inativado. Entre em contato com o suporte.`)
        return
      }

      if (profile.role === "super_admin" || profile.office_role === "owner") {
        navigate("/admin", { replace: true })
      } else {
        navigate(from && from !== "/login" ? from : "/dashboard", { replace: true })
      }
    } catch (err: unknown) {
      setLoading(false)
      let message = "Falha no login"
      if (err && typeof err === "object" && "message" in err) {
        const msg = String((err as { message: string }).message)
        if (msg === "Failed to fetch" || msg.includes("fetch")) {
          message = "Não foi possível contactar o Supabase. Confira SUPABASE_URL e SUPABASE_ANON_KEY."
        } else if (msg.toLowerCase().includes("invalid login")) {
          message = "E-mail ou senha inválidos."
        } else {
          message = msg
        }
      }
      setError(message)
    }
  }

  return (
    <div className="min-h-[100dvh] flex items-center justify-center relative overflow-hidden p-4 bg-background">
      <div className="absolute inset-0 bg-gradient-to-br from-primary/5 via-transparent to-accent/5" aria-hidden />
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        <div className="absolute top-1/4 left-1/4 w-72 h-72 bg-primary/10 rounded-full blur-3xl animate-pulse" />
        <div className="absolute bottom-1/4 right-1/4 w-96 h-96 bg-primary/10 rounded-full blur-3xl animate-pulse delay-1000" />
        <div className="absolute top-1/2 right-1/3 w-64 h-64 bg-accent/10 rounded-full blur-3xl animate-pulse delay-2000" />
      </div>

      <div className="relative w-full max-w-md bg-card/90 backdrop-blur-xl rounded-2xl md:rounded-3xl shadow-2xl border border-border p-6 md:p-8 animate-in slide-in-from-bottom-4">
        <div className="flex flex-col sm:flex-row items-center gap-2 md:gap-3 mb-6 md:mb-8 min-w-0">
          <div className="h-12 w-12 md:h-14 md:w-14 rounded-xl md:rounded-2xl overflow-hidden ring-2 ring-primary-icon/20 zoom-in-anim animate-in flex-shrink-0 flex items-center justify-center bg-card/80">
            <img src={defaultLogoUrl} alt="Logo" className="w-full h-full object-contain" />
          </div>
          <div className="text-center sm:text-left min-w-0">
            <div className="text-xs md:text-sm text-muted-foreground font-medium">{loginSubtitle}</div>
            <h1 className="text-xl md:text-2xl font-extrabold text-foreground truncate">Login</h1>
          </div>
        </div>

        <form onSubmit={handleSubmit} className="space-y-3 md:space-y-4">
          <div className="space-y-2.5 md:space-y-3">
            <input
              id="email"
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="E-mail"
              autoFocus
              autoComplete="email"
              className="w-full px-4 py-3 md:py-3.5 rounded-lg md:rounded-xl border-2 border-border bg-background/50 text-foreground placeholder-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring focus:border-primary-icon transition-all text-base touch-manipulation"
              disabled={loading}
            />
            <input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Senha"
              autoComplete="current-password"
              className="w-full px-4 py-3 md:py-3.5 rounded-lg md:rounded-xl border-2 border-border bg-background/50 text-foreground placeholder-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring focus:border-primary-icon transition-all text-base touch-manipulation"
              disabled={loading}
            />
          </div>

          {error && (
            <div className="flex items-center gap-2 text-xs md:text-sm text-red-700 dark:text-red-300 bg-red-50 dark:bg-red-900/30 rounded-lg md:rounded-xl px-3 md:px-4 py-2.5 md:py-3 border border-red-200 dark:border-red-800 slide-in-from-top-2 animate-in">
              <AlertCircle size={16} className="md:w-[18px] md:h-[18px] flex-shrink-0" />
              <span className="break-words">{error}</span>
            </div>
          )}

          <button
            type="submit"
            disabled={loading}
            className="w-full py-3 md:py-3.5 px-4 bg-primary hover:opacity-90 text-primary-foreground font-semibold rounded-lg md:rounded-xl transition-all disabled:opacity-50 flex items-center justify-center gap-2 shadow-lg shadow-primary/25 transform hover:scale-[1.02] active:scale-[0.98] touch-manipulation text-base"
          >
            {loading ? (
              <>
                <Loader2 size={18} className="md:w-5 md:h-5 animate-spin" />
                Entrando...
              </>
            ) : (
              <>
                <Lock size={16} className="md:w-[18px] md:h-[18px]" />
                Entrar
              </>
            )}
          </button>
        </form>
      </div>
    </div>
  )
}
