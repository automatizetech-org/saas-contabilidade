import { Navigate, useLocation } from "react-router-dom"
import { useProfile } from "@/hooks/useProfile"

export function ProtectedAdminRoute({ children }: { children: React.ReactNode }) {
  const { canAccessAdmin, isLoading } = useProfile()
  const location = useLocation()

  if (isLoading) return null
  if (!canAccessAdmin) return <Navigate to="/dashboard" replace state={{ from: location.pathname }} />
  return <>{children}</>
}
