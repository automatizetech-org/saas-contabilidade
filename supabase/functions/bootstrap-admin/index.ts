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

  const authHeader = req.headers.get("Authorization")
  if (!authHeader) {
    return new Response(
      JSON.stringify({ error: "Não autenticado" }),
      { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  let body: { bootstrap_secret?: string }
  try {
    body = await req.json()
  } catch {
    return new Response(
      JSON.stringify({ error: "Corpo inválido" }),
      { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  const secret = body.bootstrap_secret?.trim()
  const expectedSecret = Deno.env.get("BOOTSTRAP_SECRET")
  if (!expectedSecret || secret !== expectedSecret) {
    return new Response(
      JSON.stringify({ error: "Segredo inválido" }),
      { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!
  const anonKey = Deno.env.get("SUPABASE_ANON_KEY")!
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!

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
})
