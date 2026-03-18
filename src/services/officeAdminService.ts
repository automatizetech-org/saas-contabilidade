import { supabase } from "./supabaseClient"
import { getCurrentOfficeContext } from "./officeContextService"

const SUPABASE_URL = import.meta.env.SUPABASE_URL ?? ""
const ANON_KEY = import.meta.env.SUPABASE_ANON_KEY ?? ""

export type OfficeServerRecord = {
  id: string
  office_id: string
  public_base_url: string
  base_path: string
  status: string
  is_active: boolean
  connector_version: string | null
  min_supported_connector_version: string | null
  last_seen_at: string | null
  last_job_at: string | null
  created_at: string
  updated_at: string
}

export type PrimeiroEscritorioInput = {
  office_name: string
  office_slug: string
  admin_email: string
  admin_password: string
  admin_username: string
  public_base_url: string
  base_path: string
  connector_version?: string | null
  min_supported_connector_version?: string | null
}

async function getSessionToken() {
  const {
    data: { session },
  } = await supabase.auth.getSession()
  if (!session?.access_token) throw new Error("Não autenticado.")
  return session.access_token
}

async function invokeFunction<T>(name: string, body: Record<string, unknown>): Promise<T> {
  const token = await getSessionToken()
  const response = await fetch(`${SUPABASE_URL}/functions/v1/${name}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      apikey: ANON_KEY,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(body),
  })

  const payload = await response.json().catch(() => ({}))
  if (!response.ok) {
    throw new Error(payload?.error || payload?.detail || `Falha ao executar ${name}`)
  }
  return payload as T
}

export async function createPrimeiroEscritorio(input: PrimeiroEscritorioInput) {
  return invokeFunction<{
    message: string
    office: { id: string; name: string; slug: string; status: string }
    admin_user_id: string
    connector_secret: string
  }>("primeiro-escritorio", input)
}

export async function getCurrentOfficeServer(): Promise<OfficeServerRecord | null> {
  const context = await getCurrentOfficeContext()
  if (!context?.officeId) return null

  const { data, error } = await supabase
    .from("office_servers")
    .select("*")
    .eq("office_id", context.officeId)
    .eq("is_active", true)
    .maybeSingle()
  if (error) throw error
  return (data ?? null) as OfficeServerRecord | null
}

export async function updateCurrentOfficeServer(
  updates: Partial<Pick<OfficeServerRecord, "public_base_url" | "base_path" | "connector_version" | "min_supported_connector_version" | "status">>
): Promise<OfficeServerRecord> {
  const context = await getCurrentOfficeContext()
  if (!context?.officeId) throw new Error("Nenhum escritório ativo encontrado.")

  const current = await getCurrentOfficeServer()
  if (!current) throw new Error("Nenhum servidor ativo encontrado.")

  const payload = {
    ...updates,
    updated_at: new Date().toISOString(),
  }

  const { data, error } = await supabase
    .from("office_servers")
    .update(payload)
    .eq("id", current.id)
    .eq("office_id", context.officeId)
    .select("*")
    .single()
  if (error) throw error
  return data as OfficeServerRecord
}
