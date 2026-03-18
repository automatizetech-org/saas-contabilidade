import { createClient } from "https://esm.sh/@supabase/supabase-js@2"

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Max-Age": "86400",
}
const functionVersion = "get-users-admin-hardening-v2"

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

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response(null, { status: 204, headers: corsHeaders })
  if (req.method !== "GET") return json({ error: "Method not allowed" }, 405)

  try {
    const userToken = req.headers.get("Authorization")?.replace(/^Bearer\s+/i, "")
    if (!userToken) return json({ error: "Missing authorization" }, 401)

    const supabaseUrl = Deno.env.get("SUPABASE_URL")!
    const anonKey = Deno.env.get("SUPABASE_ANON_KEY")!
    const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!

    const authClient = createClient(supabaseUrl, anonKey, {
      global: { headers: { Authorization: `Bearer ${userToken}` } },
      auth: { autoRefreshToken: false, persistSession: false },
    })
    const {
      data: { user },
      error: authError,
    } = await authClient.auth.getUser()
    if (authError || !user) return json({ error: "Unauthorized" }, 401)

    const admin = createClient(supabaseUrl, serviceRoleKey, {
      auth: { autoRefreshToken: false, persistSession: false },
    })

    const { data: callerProfile } = await admin
      .from("profiles")
      .select("role")
      .eq("id", user.id)
      .single()
    const { data: callerMembership } = await admin
      .from("office_memberships")
      .select("office_id, role")
      .eq("user_id", user.id)
      .order("is_default", { ascending: false })
      .limit(1)
      .maybeSingle()

    const isSuperAdmin = callerProfile?.role === "super_admin"
    const canManageOffice =
      isSuperAdmin ||
      callerMembership?.role === "owner"
    if (!canManageOffice) return json({ error: "Forbidden" }, 403)

    const requestedOfficeId = new URL(req.url).searchParams.get("office_id")
    if (!isSuperAdmin && requestedOfficeId?.trim() && requestedOfficeId.trim() !== callerMembership?.office_id) {
      return json({ error: "Forbidden", code: "GET_USERS_ADMIN_FOREIGN_OFFICE_FORBIDDEN" }, 403)
    }
    const officeId = isSuperAdmin
      ? requestedOfficeId?.trim() || callerMembership?.office_id || null
      : callerMembership?.office_id || null

    let membershipsQuery = admin
      .from("office_memberships")
      .select("user_id, office_id, role, panel_access, created_at")
      .order("created_at", { ascending: true })

    if (officeId) membershipsQuery = membershipsQuery.eq("office_id", officeId)

    const { data: memberships, error: membershipsError } = await membershipsQuery
    if (membershipsError) return json({ error: membershipsError.message }, 500)

    const userIds = [...new Set((memberships ?? []).map((row) => row.user_id).filter(Boolean))]
    if (userIds.length === 0) return json([], 200)

    const [{ data: profiles, error: profilesError }, authList] = await Promise.all([
      admin.from("profiles").select("id, username, role, created_at").in("id", userIds),
      admin.auth.admin.listUsers({ page: 1, perPage: 1000 }),
    ])
    if (profilesError) return json({ error: profilesError.message }, 500)
    if (authList.error) return json({ error: authList.error.message }, 500)

    const emailById = new Map(
      (authList.data?.users ?? []).map((authUser) => [authUser.id, authUser.email ?? null])
    )
    const profileById = new Map((profiles ?? []).map((row) => [row.id, row]))

    const rows = (memberships ?? []).map((membership) => {
      const profile = profileById.get(membership.user_id)
      return {
        id: membership.user_id,
        email: emailById.get(membership.user_id) ?? null,
        username: profile?.username ?? "",
        role: profile?.role ?? "user",
        created_at: profile?.created_at ?? membership.created_at,
        office_id: membership.office_id,
        office_role: membership.role,
        panel_access: membership.panel_access ?? {},
      }
    })

    return json(rows, 200)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    return json({ error: "Internal error", detail: message }, 500)
  }
})
