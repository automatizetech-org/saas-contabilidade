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
const functionVersion = "create-user-hardening-v2"

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
    headers: {
      ...corsHeaders,
      "Content-Type": "application/json",
      "X-Function-Version": functionVersion,
    },
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

  const { data: profile, error: profileError } = await admin
    .from("profiles")
    .select("role")
    .eq("id", user.id)
    .single()
  if (profileError || !profile) return { error: json({ error: "Profile not found" }, 403) }

  const { data: membership } = await admin
    .from("office_memberships")
    .select("office_id, role")
    .eq("user_id", user.id)
    .order("is_default", { ascending: false })
    .limit(1)
    .maybeSingle()

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

  const email = String(body.email ?? "").trim().toLowerCase()
  const password = String(body.password ?? "")
  const username = String(body.username ?? "").trim()
  const requestedPlatformRole = body.role === "super_admin" ? "super_admin" : "user"
  const targetOfficeRole = normalizeOfficeRole(body.office_role)
  const requestedOfficeId = String(body.office_id ?? "").trim() || null

  if (caller.platformRole !== "super_admin" && requestedPlatformRole === "super_admin") {
    return json(
      {
        error: "Apenas super_admin pode criar usuários com papel de plataforma elevado.",
        code: "CREATE_USER_PLATFORM_ROLE_FORBIDDEN",
      },
      403,
    )
  }
  if (caller.platformRole !== "super_admin" && requestedOfficeId && requestedOfficeId !== caller.officeId) {
    return json(
      {
        error: "Não é permitido vincular usuários a outro escritório.",
        code: "CREATE_USER_FOREIGN_OFFICE_FORBIDDEN",
      },
      403,
    )
  }

  const targetPlatformRole =
    requestedPlatformRole === "super_admin" && caller.platformRole === "super_admin" ? "super_admin" : "user"
  const officeId =
    caller.platformRole === "super_admin"
      ? requestedOfficeId ?? caller.officeId
      : caller.officeId

  if (!email || !password || !username) {
    return json({ error: "email, password and username required" }, 400)
  }
  if (!officeId && targetPlatformRole !== "super_admin") {
    return json({ error: "Nenhum escritório ativo encontrado para vincular o usuário." }, 400)
  }

  const panelAccess = sanitizePanelAccess(body.panel_access)

  const { data: createdUser, error: createError } = await admin.auth.admin.createUser({
    email,
    password,
    email_confirm: true,
    user_metadata: {
      username,
      display_name: username,
      full_name: username,
    },
  })
  if (createError || !createdUser.user?.id) {
    return json({ error: "Create user failed", detail: createError?.message ?? "Unknown error" }, 400)
  }

  const userId = createdUser.user.id

  const { error: profileError } = await admin.from("profiles").upsert(
    {
      id: userId,
      username,
      role: targetPlatformRole,
      updated_at: new Date().toISOString(),
    },
    { onConflict: "id" }
  )
  if (profileError) return json({ error: "Profile upsert failed", detail: profileError.message }, 400)

  if (officeId) {
    const { error: membershipError } = await admin.from("office_memberships").upsert(
      {
        office_id: officeId,
        user_id: userId,
        role: targetOfficeRole,
        panel_access: panelAccess,
        is_default: true,
        updated_at: new Date().toISOString(),
      },
      { onConflict: "office_id,user_id" }
    )
    if (membershipError) {
      return json({ error: "Membership upsert failed", detail: membershipError.message }, 400)
    }
  }

  return json({
    message: "User created",
    user_id: userId,
    office_id: officeId,
    office_role: officeId ? targetOfficeRole : null,
  })
})
