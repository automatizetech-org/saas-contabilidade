import { supabase } from "./supabaseClient"
import type { Database, OfficeRole, PlatformRole } from "@/types/database"
import { getCurrentOfficeContext, type CurrentOfficeContext } from "./officeContextService"

export type BaseProfile = Database["public"]["Tables"]["profiles"]["Row"]
export type Company = Database["public"]["Tables"]["companies"]["Row"]

export type Profile = BaseProfile & {
  office_id: string | null
  office_name: string | null
  office_slug: string | null
  office_status: string | null
  office_membership_id: string | null
  office_role: OfficeRole | null
  panel_access: Record<string, boolean>
}

export type AdminUser = {
  id: string
  email: string | null
  username: string
  role: PlatformRole
  created_at: string
  office_id: string | null
  office_role: OfficeRole | null
  panel_access: Record<string, boolean>
}

const SUPABASE_URL = import.meta.env.SUPABASE_URL ?? ""
const ANON_KEY = import.meta.env.SUPABASE_ANON_KEY ?? ""

function mergeProfileWithOfficeContext(
  profile: BaseProfile,
  context: CurrentOfficeContext | null
): Profile {
  return {
    ...profile,
    office_id: context?.officeId ?? null,
    office_name: context?.officeName ?? null,
    office_slug: context?.officeSlug ?? null,
    office_status: context?.officeStatus ?? null,
    office_membership_id: context?.membershipId ?? null,
    office_role: context?.membershipRole ?? null,
    panel_access: context?.panelAccess ?? {},
  }
}

export async function getProfile(userId: string): Promise<Profile | null> {
  const { data, error } = await supabase
    .from("profiles")
    .select("*")
    .eq("id", userId)
    .maybeSingle()

  if (error) throw error
  if (!data) return null

  const context = await getCurrentOfficeContext().catch(() => null)
  return mergeProfileWithOfficeContext(data as BaseProfile, context)
}

export async function getProfilesForAdmin(): Promise<AdminUser[]> {
  return getUsersForAdmin()
}

export async function updateProfile(
  id: string,
  updates: { username?: string }
) {
  const { data, error } = await supabase.from("profiles").update(updates).eq("id", id).select().single()
  if (error) throw error
  return data as BaseProfile
}

export async function getUsersForAdmin(): Promise<AdminUser[]> {
  const { data: { session } } = await supabase.auth.getSession()
  if (!session?.access_token) throw new Error("Não autenticado")

  const res = await fetch(`${SUPABASE_URL}/functions/v1/get-users-admin`, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${session.access_token}`,
      apikey: ANON_KEY,
    },
    cache: "no-store",
  })

  const data = await res.json().catch(() => ({}))
  if (!res.ok) throw new Error(data?.error || data?.detail || "Falha ao listar usuários")

  return (Array.isArray(data) ? data : []).map((row) => ({
    id: String(row.id ?? ""),
    email: row.email ?? null,
    username: String(row.username ?? "").trim(),
    role: (row.role ?? "user") as PlatformRole,
    created_at: String(row.created_at ?? ""),
    office_id: row.office_id ?? null,
    office_role: (row.office_role ?? null) as OfficeRole | null,
    panel_access: (row.panel_access ?? {}) as Record<string, boolean>,
  }))
}
