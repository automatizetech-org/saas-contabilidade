import { createClient } from "https://esm.sh/@supabase/supabase-js@2"

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
}

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders })
  if (req.method !== "POST") return json({ error: "Method not allowed" }, 405)

  let body: { bootstrap_secret?: string; username?: string }
  try {
    body = await req.json()
  } catch {
    return json({ error: "Corpo inválido (JSON esperado)" }, 400)
  }

  if (body.bootstrap_secret === undefined) {
    return json(
      {
        error: "Login por username foi descontinuado.",
        detail: "Use email e senha pelo fluxo nativo do Supabase.",
      },
      410
    )
  }

  const authHeader = req.headers.get("Authorization")
  if (!authHeader) return json({ error: "Não autenticado" }, 401)

  const secret = String(body.bootstrap_secret ?? "").trim()
  const expectedSecret = Deno.env.get("BOOTSTRAP_SECRET")
  if (!expectedSecret || secret !== expectedSecret) return json({ error: "Segredo inválido" }, 401)

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!
  const anonKey = Deno.env.get("SUPABASE_ANON_KEY")!
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!

  const authClient = createClient(supabaseUrl, anonKey, {
    global: { headers: { Authorization: authHeader } },
    auth: { autoRefreshToken: false, persistSession: false },
  })
  const {
    data: { user },
    error: authError,
  } = await authClient.auth.getUser()
  if (authError || !user) return json({ error: "Não autenticado" }, 401)

  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  })
  const { count, error: countError } = await admin
    .from("profiles")
    .select("id", { count: "exact", head: true })
    .eq("role", "super_admin")
  if (countError) return json({ error: countError.message }, 500)
  if ((count ?? 0) > 0) {
    return json({ error: "Já existe um administrador. Use o painel Admin para gerenciar perfis." }, 403)
  }

  const username = String(body.username ?? user.user_metadata?.display_name ?? "admin").trim() || "admin"
  const { error: updateError } = await admin
    .from("profiles")
    .update({ username, role: "super_admin", updated_at: new Date().toISOString() })
    .eq("id", user.id)
  if (updateError) return json({ error: "Falha ao atualizar perfil", detail: updateError.message }, 500)

  return json({ message: "Administrador definido com sucesso.", user_id: user.id })
})
