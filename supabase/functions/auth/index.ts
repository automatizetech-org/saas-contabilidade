import { createClient } from "https://esm.sh/@supabase/supabase-js@2"

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders })
  }

  if (req.method !== "POST") {
    return new Response(
      JSON.stringify({ error: "Method not allowed" }),
      { status: 405, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  let body: { username?: string; password?: string; bootstrap_secret?: string }
  try {
    body = await req.json()
  } catch {
    return new Response(
      JSON.stringify({ error: "Corpo inválido (JSON esperado)" }),
      { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!
  const anonKey = Deno.env.get("SUPABASE_ANON_KEY")!
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!

  // --- Bootstrap admin: body.bootstrap_secret + Authorization ---
  if (body.bootstrap_secret !== undefined) {
    const authHeader = req.headers.get("Authorization")
    if (!authHeader) {
      return new Response(
        JSON.stringify({ error: "Não autenticado" }),
        { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      )
    }
    const secret = String(body.bootstrap_secret).trim()
    const expectedSecret = Deno.env.get("BOOTSTRAP_SECRET")
    if (!expectedSecret || secret !== expectedSecret) {
      return new Response(
        JSON.stringify({ error: "Segredo inválido" }),
        { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      )
    }
    const supabaseAuth = createClient(supabaseUrl, anonKey, {
      global: { headers: { Authorization: authHeader } },
      auth: { autoRefreshToken: false, persistSession: false },
    })
    const { data: { user }, error: authError } = await supabaseAuth.auth.getUser()
    if (authError || !user) {
      return new Response(
        JSON.stringify({ error: "Não autenticado" }),
        { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      )
    }
    const admin = createClient(supabaseUrl, serviceRoleKey, {
      auth: { autoRefreshToken: false, persistSession: false },
    })
    const { count, error: countError } = await admin
      .from("profiles")
      .select("id", { count: "exact", head: true })
      .eq("role", "super_admin")
    if (countError || (count ?? 0) > 0) {
      return new Response(
        JSON.stringify({ error: "Já existe um administrador. Use o painel Admin para gerenciar perfis." }),
        { status: 403, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      )
    }
    const { error: updateError } = await admin
      .from("profiles")
      .update({ username: "admin", role: "super_admin" })
      .eq("id", user.id)
    if (updateError) {
      return new Response(
        JSON.stringify({ error: "Falha ao atualizar perfil", detail: updateError.message }),
        { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
      )
    }
    return new Response(
      JSON.stringify({ message: "Administrador definido com sucesso. Faça login com usuário admin e sua senha." }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  // --- Login com username + password ---
  const { username, password } = body
  const usernameTrim = typeof username === "string" ? username.trim() : ""
  if (!usernameTrim || typeof password !== "string" || !password) {
    return new Response(
      JSON.stringify({ error: "Nome de usuário e senha são obrigatórios" }),
      { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  })

  // Fallback: login admin/admin123 usando email configurado (ADMIN_EMAIL) — atualiza o perfil para username admin
  const adminEmail = Deno.env.get("ADMIN_EMAIL")
  if (usernameTrim === "admin" && adminEmail?.trim()) {
    const tokenRes = await fetch(`${supabaseUrl}/auth/v1/token?grant_type=password`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "apikey": anonKey },
      body: JSON.stringify({ email: adminEmail.trim(), password }),
    })
    if (tokenRes.ok) {
      const session = await tokenRes.json()
      const payload = JSON.parse(atob(session.access_token.split(".")[1]))
      const userId = payload.sub
      if (userId) {
        await admin.from("profiles").update({ username: "admin", role: "super_admin" }).eq("id", userId)
      }
      return new Response(
        JSON.stringify({
          access_token: session.access_token,
          refresh_token: session.refresh_token,
          expires_in: session.expires_in,
        }),
        { headers: { ...corsHeaders, "Content-Type": "application/json" } }
      )
    }
  }

  const { data: profile, error: profileError } = await admin
    .from("profiles")
    .select("id")
    .eq("username", usernameTrim)
    .maybeSingle()

  if (profileError || !profile?.id) {
    return new Response(
      JSON.stringify({ error: "Usuário ou senha inválidos" }),
      { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  const { data: userData, error: userError } = await admin.auth.admin.getUserById(profile.id)
  if (userError || !userData?.user?.email) {
    return new Response(
      JSON.stringify({ error: "Usuário ou senha inválidos" }),
      { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  const tokenRes = await fetch(`${supabaseUrl}/auth/v1/token?grant_type=password`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "apikey": anonKey,
    },
    body: JSON.stringify({
      email: userData.user.email,
      password,
    }),
  })

  if (!tokenRes.ok) {
    const errBody = await tokenRes.text()
    let message = "Usuário ou senha inválidos"
    try {
      const parsed = JSON.parse(errBody)
      if (parsed.error_description) message = parsed.error_description
    } catch {
      // keep default
    }
    return new Response(
      JSON.stringify({ error: message }),
      { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  const session = await tokenRes.json()
  return new Response(
    JSON.stringify({
      access_token: session.access_token,
      refresh_token: session.refresh_token,
      expires_in: session.expires_in,
    }),
    { headers: { ...corsHeaders, "Content-Type": "application/json" } }
  )
})
