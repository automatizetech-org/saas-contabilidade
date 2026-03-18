import { createClient } from "https://esm.sh/@supabase/supabase-js@2"

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-create-admin-secret",
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

  const secret = req.headers.get("x-create-admin-secret")
  const expectedSecret = Deno.env.get("CREATE_ADMIN_SECRET")
  if (!expectedSecret || secret !== expectedSecret) return json({ error: "Unauthorized" }, 401)

  let body: { email?: string; password?: string; username?: string }
  try {
    body = await req.json()
  } catch {
    body = {}
  }

  const email = String(body.email ?? "admin@fleury-insights.local").trim().toLowerCase()
  const password = String(body.password ?? "admin123")
  const username = String(body.username ?? "admin").trim() || "admin"

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  })

  const { count, error: countError } = await admin
    .from("profiles")
    .select("id", { count: "exact", head: true })
    .eq("role", "super_admin")
  if (countError) return json({ error: countError.message }, 500)
  if ((count ?? 0) > 0) return json({ error: "Já existe um super_admin cadastrado." }, 409)

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

  const { error: profileError } = await admin.from("profiles").upsert(
    {
      id: createdUser.user.id,
      username,
      role: "super_admin",
      updated_at: new Date().toISOString(),
    },
    { onConflict: "id" }
  )
  if (profileError) return json({ error: "Profile update failed", detail: profileError.message }, 500)

  return json({
    message: "Admin created.",
    user_id: createdUser.user.id,
    email,
  })
})
