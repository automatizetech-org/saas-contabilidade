import { createClient } from "https://esm.sh/@supabase/supabase-js@2"

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-user-token",
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders })
  }

  const authHeader = req.headers.get("Authorization")
  const userToken = req.headers.get("X-User-Token") ?? authHeader?.replace(/^Bearer\s+/i, "")
  if (!userToken) {
    return new Response(
      JSON.stringify({ error: "Missing authorization", detail: "Envie o token em Authorization ou X-User-Token." }),
      { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
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
    return new Response(
      JSON.stringify({
        error: "Unauthorized",
        detail: authError?.message === "jwt expired" ? "Token expirado. Faça login novamente." : authError?.message || "Token inválido ou ausente.",
      }),
      { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  const supabaseAdmin = createClient(supabaseUrl, serviceRoleKey, { auth: { autoRefreshToken: false, persistSession: false } })
  const { data: profile } = await supabaseAdmin.from("profiles").select("role").eq("id", user.id).single()
  if (profile?.role !== "super_admin") {
    return new Response(
      JSON.stringify({ error: "Forbidden: super_admin only" }),
      { status: 403, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  let body: { email: string; password: string; username: string; role?: string }
  try {
    body = await req.json()
  } catch {
    return new Response(
      JSON.stringify({ error: "Invalid JSON body" }),
      { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  const { email, password, username, role = "user" } = body
  if (!email || !password || !username) {
    return new Response(
      JSON.stringify({ error: "email, password and username required" }),
      { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  const { data: newUser, error: createError } = await supabaseAdmin.auth.admin.createUser({
    email,
    password,
    email_confirm: true,
    user_metadata: { username, full_name: username, display_name: username, role },
  })

  if (createError) {
    return new Response(
      JSON.stringify({ error: "Create user failed", detail: createError.message }),
      { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  const uid = newUser.user?.id
  if (uid) {
    await supabaseAdmin.from("profiles").upsert(
      { id: uid, username, role: role === "super_admin" ? "super_admin" : "user" },
      { onConflict: "id" }
    )
  }

  return new Response(
    JSON.stringify({ message: "User created", user_id: uid }),
    { headers: { ...corsHeaders, "Content-Type": "application/json" } }
  )
})
