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

  let body: { username?: string; password?: string }
  try {
    body = await req.json()
  } catch {
    return new Response(
      JSON.stringify({ error: "Corpo inválido (JSON esperado)" }),
      { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  const { username, password } = body
  const usernameTrim = typeof username === "string" ? username.trim() : ""
  if (!usernameTrim || typeof password !== "string" || !password) {
    return new Response(
      JSON.stringify({ error: "Nome de usuário e senha são obrigatórios" }),
      { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!
  const anonKey = Deno.env.get("SUPABASE_ANON_KEY")!
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  })

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
      // keep default message
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
