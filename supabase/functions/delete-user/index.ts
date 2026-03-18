import { createClient } from "https://esm.sh/@supabase/supabase-js@2"

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
}

type CallerContext = {
  userId: string
  platformRole: "super_admin" | "user"
  officeId: string | null
  officeRole: "owner" | "admin" | "operator" | "viewer" | null
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

  const [{ data: profile }, { data: membership }] = await Promise.all([
    admin.from("profiles").select("role").eq("id", user.id).single(),
    admin
      .from("office_memberships")
      .select("office_id, role")
      .eq("user_id", user.id)
      .order("is_default", { ascending: false })
      .limit(1)
      .maybeSingle(),
  ])

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
  const canManageOffice = caller.platformRole === "super_admin" || caller.officeRole === "owner"
  if (!canManageOffice) return json({ error: "Forbidden" }, 403)

  let body: { user_id?: string; office_id?: string | null }
  try {
    body = await req.json()
  } catch {
    return json({ error: "Invalid JSON body" }, 400)
  }

  const targetUserId = String(body.user_id ?? "").trim()
  if (!targetUserId) return json({ error: "user_id required" }, 400)
  if (targetUserId === caller.userId) return json({ error: "Você não pode excluir seu próprio usuário." }, 400)

  const requestedOfficeId = String(body.office_id ?? "").trim() || null
  if (caller.platformRole !== "super_admin" && requestedOfficeId && requestedOfficeId !== caller.officeId) {
    return json({ error: "Não é permitido excluir usuários de outro escritório." }, 403)
  }
  const officeId = caller.platformRole === "super_admin" ? requestedOfficeId ?? caller.officeId : caller.officeId

  const [{ data: targetProfile }, { data: targetMembership }] = await Promise.all([
    admin.from("profiles").select("role").eq("id", targetUserId).maybeSingle(),
    officeId
      ? admin
          .from("office_memberships")
          .select("office_id, role")
          .eq("user_id", targetUserId)
          .eq("office_id", officeId)
          .maybeSingle()
      : Promise.resolve({ data: null, error: null }),
  ])

  if (!targetProfile) return json({ error: "Usuário alvo não encontrado." }, 404)
  if (caller.platformRole !== "super_admin" && !targetMembership) {
    return json({ error: "Target user is outside the current office." }, 403)
  }
  if (caller.platformRole !== "super_admin" && targetProfile.role === "super_admin") {
    return json({ error: "Apenas super_admin pode excluir outro super_admin." }, 403)
  }

  if (officeId && targetMembership?.role === "owner") {
    const { count: ownerCount, error: ownerCountError } = await admin
      .from("office_memberships")
      .select("id", { count: "exact", head: true })
      .eq("office_id", officeId)
      .eq("role", "owner")
    if (ownerCountError) return json({ error: ownerCountError.message }, 500)
    if ((ownerCount ?? 0) <= 1) {
      return json({ error: "Não é permitido remover o último owner do escritório." }, 400)
    }
  }

  const { error: deleteError } = await admin.auth.admin.deleteUser(targetUserId)
  if (deleteError) return json({ error: "Delete user failed", detail: deleteError.message }, 400)

  return json({ message: "User deleted", user_id: targetUserId })
})
