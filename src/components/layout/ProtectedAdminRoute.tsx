import { Navigate, useLocation } from "react-router-dom"
import { useProfile } from "@/hooks/useProfile"

export function ProtectedAdminRoute({ children }: { children: React.ReactNode }) {
  const { isSuperAdmin, isLoading } = useProfile()
  const location = useLocation()

  if (isLoading) return null
  if (!isSuperAdmin) return <Navigate to="/dashboard" replace state={{ from: location.pathname }} />
  return <>{children}</>
}
