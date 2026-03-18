import { createClient } from "https://esm.sh/@supabase/supabase-js@2"

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-user-token",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Max-Age": "86400",
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: corsHeaders })
  }

  const json = (body: unknown, status: number) =>
    new Response(JSON.stringify(body), { status, headers: { ...corsHeaders, "Content-Type": "application/json" } })

  try {
  const userToken = req.headers.get("X-User-Token") ?? req.headers.get("Authorization")?.replace(/^Bearer\s+/i, "")
  if (!userToken) {
    return json({ error: "Missing authorization" }, 401)
  }

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!
  const anonKey = Deno.env.get("SUPABASE_ANON_KEY")!
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!

  const supabaseAuth = createClient(supabaseUrl, anonKey, {
    global: { headers: { Authorization: `Bearer ${userToken}` } },
    auth: { autoRefreshToken: false, persistSession: false },
  })
  const { data: { user }, error: authError } = await supabaseAuth.auth.getUser()
  if (authError || !user) {
    return json({ error: "Unauthorized" }, 401)
  }

  const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey, { auth: { autoRefreshToken: false, persistSession: false } })
  const { data: callerProfile } = await supabaseAdmin.from("profiles").select("role").eq("id", user.id).single()
  if (callerProfile?.role !== "super_admin") {
    return json({ error: "Forbidden" }, 403)
  }

  const allUsers: Array<{ id: string; email?: string }> = []
  let page = 1
  let hasMore = true
  while (hasMore) {
    const { data: { users }, error: listError } = await supabaseAdmin.auth.admin.listUsers({ page, perPage: 100 })
    if (listError) {
      return json({ error: "List users failed", detail: listError.message }, 500)
    }
    const list = users ?? []
    allUsers.push(...list)
    hasMore = list.length === 100
    if (hasMore) page += 1
  }

  const userIds = allUsers.map((u) => u.id).filter(Boolean)
  const profiles: unknown[] = []
  const chunkSize = 100
  for (let i = 0; i < userIds.length; i += chunkSize) {
    const chunk = userIds.slice(i, i + chunkSize)
    const { data } = await supabaseAdmin.from("profiles").select("id, username, role, created_at").in("id", chunk)
    if (data?.length) profiles.push(...data)
  }
  const profileMap = new Map(profiles.map((p) => [String((p as { id: string }).id), p]))

  const list = allUsers.map((u) => {
    const profileId = String(u.id)
    const p = profileMap.get(profileId) as Record<string, unknown> | undefined
    const rawUsername = p && (p.username != null ? String(p.username) : (p as { Username?: string }).Username != null ? String((p as { Username: string }).Username) : "")
    const username = typeof rawUsername === "string" && rawUsername.trim() !== "" ? rawUsername.trim() : ""
    return {
      id: u.id,
      email: u.email ?? null,
      username,
      role: (p && (p as { role?: string }).role) ?? "user",
      panel_access: (p && (p as { panel_access?: Record<string, unknown> }).panel_access) ?? {},
      created_at: (p && (p as { created_at?: string }).created_at) ?? null,
    }
  }).filter((r) => r.email)

  return json(list, 200)
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    return json({ error: "Internal error", detail: message }, 500)
  }
})
