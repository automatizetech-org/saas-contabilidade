import { supabase } from "./supabaseClient"
import type { Database } from "@/types/database"

export type Profile = Database["public"]["Tables"]["profiles"]["Row"]
export type Company = Database["public"]["Tables"]["companies"]["Row"]

export async function getProfile(userId: string): Promise<Profile | null> {
  const { data, error } = await supabase
    .from("profiles")
    .select("id, username, role")
    .eq("id", userId)
    .maybeSingle()
  if (error) throw error
  if (!data) return null
  const row = data as { id: string; username: string; role: string }
  return {
    ...row,
    created_at: "",
    panel_access: {},
  } as Profile
}

export async function getProfilesForAdmin() {
  const { data, error } = await supabase
    .from("profiles")
    .select("id, username, role")
    .order("id", { ascending: true })
  if (error) throw error
  return (data ?? []).map((row) => ({
    ...row,
    created_at: "",
    panel_access: {},
  })) as Profile[]
}

export async function updateProfile(
  id: string,
  updates: { username?: string; role?: string; panel_access?: Record<string, boolean> }
) {
  const { data, error } = await supabase.from("profiles").update(updates).eq("id", id).select().single()
  if (error) throw error
  return data as Profile
}

export type AdminUser = Profile & { email: string | null }

const SUPABASE_URL = import.meta.env.SUPABASE_URL ?? ""
const ANON_KEY = import.meta.env.SUPABASE_ANON_KEY ?? ""

export async function getUsersForAdmin(): Promise<AdminUser[]> {
  const { data: { session } } = await supabase.auth.getSession()
  if (!session?.access_token) throw new Error("Não autenticado")
  const res = await fetch(`${SUPABASE_URL}/functions/v1/get-users-admin`, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${ANON_KEY}`,
      apikey: ANON_KEY,
      "X-User-Token": session.access_token,
    },
    cache: "no-store",
  })
  const data = await res.json().catch(() => ({}))
  if (!res.ok) throw new Error(data?.error || data?.detail || "Falha ao listar usuários")
  const raw = Array.isArray(data) ? data : []
  return raw.map((u: { id?: string; email?: string | null; username?: string | null; role?: string; panel_access?: Record<string, boolean>; created_at?: string }) => {
    const username = (u.username != null && u.username !== "") ? String(u.username).trim() : ""
    return {
      id: u.id ?? "",
      email: u.email ?? null,
      username,
      role: u.role ?? "user",
      panel_access: u.panel_access ?? {},
      created_at: u.created_at ?? "",
    }
  }) as AdminUser[]
}
