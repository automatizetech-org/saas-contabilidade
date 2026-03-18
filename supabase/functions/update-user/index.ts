import { createClient } from "https://esm.sh/@supabase/supabase-js@2"

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-user-token",
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders })

  const userToken = req.headers.get("X-User-Token") ?? req.headers.get("Authorization")?.replace(/^Bearer\s+/i, "")
  if (!userToken) {
    return new Response(JSON.stringify({ error: "Missing authorization" }), { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } })
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
    return new Response(JSON.stringify({ error: "Unauthorized", detail: authError?.message }), { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } })
  }

  const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey, { auth: { autoRefreshToken: false, persistSession: false } })
  const { data: profile } = await supabaseAdmin.from("profiles").select("role").eq("id", user.id).single()
  if (profile?.role !== "super_admin") {
    return new Response(JSON.stringify({ error: "Forbidden: super_admin only" }), { status: 403, headers: { ...corsHeaders, "Content-Type": "application/json" } })
  }

  let body: { user_id: string; email?: string; password?: string; username?: string; role?: string; panel_access?: Record<string, boolean> }
  try {
    body = await req.json()
  } catch {
    return new Response(JSON.stringify({ error: "Invalid JSON body" }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } })
  }

  const { user_id, email, password, username, role, panel_access } = body
  if (!user_id) {
    return new Response(JSON.stringify({ error: "user_id required" }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } })
  }

  const updates: Record<string, unknown> = {}
  if (typeof email === "string" && email.trim()) updates.email = email.trim()
  if (typeof password === "string" && password.length > 0) updates.password = password
  if (typeof username === "string" && username.trim()) {
    const { data: targetUser } = await supabaseAdmin.auth.admin.getUserById(user_id)
    const existing = (targetUser?.user?.user_metadata ?? {}) as Record<string, unknown>
    updates.user_metadata = {
      ...existing,
      full_name: username.trim(),
      display_name: username.trim(),
      username: username.trim(),
    }
  }

  if (Object.keys(updates).length > 0) {
    const { error: updateAuthError } = await supabaseAdmin.auth.admin.updateUserById(user_id, updates)
    if (updateAuthError) {
      return new Response(JSON.stringify({ error: "Update auth failed", detail: updateAuthError.message }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } })
    }
  }

  const profileUpdates: Record<string, unknown> = {}
  if (typeof username === "string" && username.trim()) profileUpdates.username = username.trim()
  if (role === "super_admin" || role === "user") profileUpdates.role = role
  // panel_access não é atualizado aqui para não quebrar quando a coluna não existir na tabela profiles

  if (Object.keys(profileUpdates).length > 0) {
    const { error: profileError } = await supabaseAdmin.from("profiles").update(profileUpdates).eq("id", user_id)
    if (profileError) {
      return new Response(JSON.stringify({ error: "Update profile failed", detail: profileError.message }), { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } })
    }
  }

  return new Response(JSON.stringify({ message: "User updated" }), { headers: { ...corsHeaders, "Content-Type": "application/json" } })
})
