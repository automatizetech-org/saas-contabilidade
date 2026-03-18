import { Navigate, useLocation } from "react-router-dom"
import { useQuery } from "@tanstack/react-query"
import { supabase } from "@/services/supabaseClient"

/**
 * Protege rotas que exigem autenticação. Se o usuário não estiver logado, redireciona para /login.
 * Impede acesso direto por URL sem sessão.
 */
export function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const location = useLocation()
  const { data: session, isLoading } = useQuery({
    queryKey: ["auth-session"],
    queryFn: async () => {
      const { data } = await supabase.auth.getSession()
      return data.session
    },
  })

  if (isLoading) return null
  if (!session) return <Navigate to="/login" replace state={{ from: location.pathname }} />
  return <>{children}</>
}
