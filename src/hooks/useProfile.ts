import { useQuery } from "@tanstack/react-query"
import { supabase } from "@/services/supabaseClient"
import { getProfile } from "@/services/profilesService"

export function useProfile() {
  const { data: session } = useQuery({
    queryKey: ["auth-session"],
    queryFn: async () => {
      const { data } = await supabase.auth.getSession()
      return data.session
    },
  })
  const userId = session?.user?.id
  const { data: profile, isLoading } = useQuery({
    queryKey: ["profile", userId],
    queryFn: () => getProfile(userId!),
    enabled: !!userId,
  })
  const isSuperAdmin = profile?.role === "super_admin"
  return { profile, isLoading, isSuperAdmin, userId }
}
