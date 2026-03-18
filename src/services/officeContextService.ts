import { supabase } from "./supabaseClient"
import type { OfficeRole } from "@/types/database"

export type CurrentOfficeContext = {
  officeId: string
  officeName: string
  officeSlug: string
  officeStatus: string
  membershipId: string
  membershipRole: OfficeRole
  panelAccess: Record<string, boolean>
}

export async function getCurrentOfficeContext(): Promise<CurrentOfficeContext | null> {
  const {
    data: { session },
  } = await supabase.auth.getSession()
  const userId = session?.user?.id
  if (!userId) return null

  const { data, error } = await supabase
    .from("office_memberships")
    .select(`
      id,
      office_id,
      role,
      panel_access,
      is_default,
      offices:offices!office_memberships_office_id_fkey (
        id,
        name,
        slug,
        status
      )
    `)
    .eq("user_id", userId)
    .order("is_default", { ascending: false })
    .limit(1)

  if (error) throw error
  const row = Array.isArray(data) ? data[0] : null
  if (!row) return null

  const office = Array.isArray(row.offices) ? row.offices[0] : row.offices
  if (!office?.id) return null

  return {
    officeId: office.id,
    officeName: office.name ?? "",
    officeSlug: office.slug ?? "",
    officeStatus: office.status ?? "draft",
    membershipId: row.id,
    membershipRole: row.role,
    panelAccess: (row.panel_access ?? {}) as Record<string, boolean>,
  }
}
