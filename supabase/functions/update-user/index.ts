import { createClient } from "https://esm.sh/@supabase/supabase-js@2"

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
}

const defaultPanelAccess = {
  dashboard: true,
  fiscal: true,
  dp: true,
  contabil: true,
  inteligencia_tributaria: true,
  ir: true,
  paralegal: false,
  financeiro: true,
  operacoes: true,
  documentos: true,
  empresas: true,
  alteracao_empresarial: false,
  sync: false,
}

const allowedPanelKeys = Object.keys(defaultPanelAccess)

type CallerContext = {
  userId: string
  platformRole: "super_admin" | "user"
  officeId: string | null
  officeRole: "owner" | "admin" | "operator" | "viewer" | null
}

function normalizeOfficeRole(role: unknown): "owner" | "viewer" {
  return role === "owner" ? "owner" : "viewer"
}

function sanitizePanelAccess(input: unknown) {
  const source = input && typeof input === "object" ? input as Record<string, unknown> : {}
  return allowedPanelKeys.reduce<Record<string, boolean>>((acc, key) => {
    acc[key] = typeof source[key] === "boolean" ? source[key] : defaultPanelAccess[key as keyof typeof defaultPanelAccess]
    return acc
  }, {})
}

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
}

async function getCallerContext(req: Request) {
  const userToken = req.headers.get("Authorization")?.replace(/^Bearer\s+/i, "")
  if (!userToken) return { error: json({ error: "Missing authorization" }, 401) }

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!
  const anonKey = Deno.env.get("SUPABASE_ANON_KEY")!
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!

  const supabaseAuth = createClient(supabaseUrl, anonKey, {
    global: { headers: { Authorization: `Bearer ${userToken}` } },
    auth: { autoRefreshToken: false, persistSession: false },
  })

  const {
    data: { user },
    error: authError,
  } = await supabaseAuth.auth.getUser()
  if (authError || !user) return { error: json({ error: "Unauthorized", detail: authError?.message }, 401) }

  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  })

  const { data: profile } = await admin.from("profiles").select("role").eq("id", user.id).single()
  const { data: membership } = await admin
    .from("office_memberships")
    .select("office_id, role")
    .eq("user_id", user.id)
    .order("is_default", { ascending: false })
    .limit(1)
    .maybeSingle()

  if (!profile) return { error: json({ error: "Profile not found" }, 403) }

  return {
    admin,
    caller: {
      userId: user.id,
      platformRole: profile.role,
      officeId: membership?.office_id ?? null,
      officeRole: membership?.role ?? null,
    } as CallerContext,
  }
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders })
  if (req.method !== "POST") return json({ error: "Method not allowed" }, 405)

  const context = await getCallerContext(req)
  if ("error" in context) return context.error

  const { admin, caller } = context
  const canManageOffice =
    caller.platformRole === "super_admin" ||
    caller.officeRole === "owner"
  if (!canManageOffice) return json({ error: "Forbidden" }, 403)

  let body: {
    user_id?: string
    email?: string
    password?: string
    username?: string
    role?: "super_admin" | "user"
    office_role?: "owner" | "viewer"
    office_id?: string | null
    panel_access?: Record<string, boolean>
  }
  try {
    body = await req.json()
  } catch {
    return json({ error: "Invalid JSON body" }, 400)
  }

  const userId = String(body.user_id ?? "").trim()
  if (!userId) return json({ error: "user_id required" }, 400)

  const requestedOfficeId = String(body.office_id ?? "").trim() || null
  if (caller.platformRole !== "super_admin" && body.role) {
    return json({ error: "Apenas super_admin pode alterar o papel de plataforma." }, 403)
  }
  if (caller.platformRole !== "super_admin" && requestedOfficeId && requestedOfficeId !== caller.officeId) {
    return json({ error: "Não é permitido alterar usuários fora do escritório atual." }, 403)
  }

  const officeId =
    caller.platformRole === "super_admin"
      ? requestedOfficeId ?? caller.officeId
      : caller.officeId

  if (caller.platformRole !== "super_admin") {
    const { data: targetMembership } = await admin
      .from("office_memberships")
      .select("office_id")
      .eq("user_id", userId)
      .eq("office_id", officeId ?? "")
      .maybeSingle()
    if (!targetMembership) return json({ error: "Target user is outside the current office." }, 403)
  }

  const authUpdates: Record<string, unknown> = {}
  const email = String(body.email ?? "").trim().toLowerCase()
  const password = String(body.password ?? "")
  const username = String(body.username ?? "").trim()
  if (email) authUpdates.email = email
  if (password) authUpdates.password = password
  if (username) {
    const { data: targetUser } = await admin.auth.admin.getUserById(userId)
    const existing = (targetUser?.user?.user_metadata ?? {}) as Record<string, unknown>
    authUpdates.user_metadata = {
      ...existing,
      username,
      display_name: username,
      full_name: username,
    }
  }

  if (Object.keys(authUpdates).length > 0) {
    const { error } = await admin.auth.admin.updateUserById(userId, authUpdates)
    if (error) return json({ error: "Update auth failed", detail: error.message }, 400)
  }

  const profileUpdates: Record<string, unknown> = {}
  if (username) profileUpdates.username = username
  if (caller.platformRole === "super_admin" && (body.role === "super_admin" || body.role === "user")) {
    profileUpdates.role = body.role
  }
  if (Object.keys(profileUpdates).length > 0) {
    const { error } = await admin.from("profiles").update(profileUpdates).eq("id", userId)
    if (error) return json({ error: "Update profile failed", detail: error.message }, 400)
  }

  if (officeId && (body.office_role || body.panel_access)) {
    const membershipPatch: Record<string, unknown> = {
      office_id: officeId,
      user_id: userId,
      updated_at: new Date().toISOString(),
    }
    if (body.office_role) membershipPatch.role = normalizeOfficeRole(body.office_role)
    if (body.panel_access) membershipPatch.panel_access = sanitizePanelAccess(body.panel_access)

    const { error } = await admin
      .from("office_memberships")
      .upsert(membershipPatch, { onConflict: "office_id,user_id" })
    if (error) return json({ error: "Update membership failed", detail: error.message }, 400)
  }

  return json({ message: "User updated" })
})
