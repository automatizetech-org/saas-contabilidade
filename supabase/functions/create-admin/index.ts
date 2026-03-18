import { createClient } from "https://esm.sh/@supabase/supabase-js@2"

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-create-admin-secret",
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders })
  }

  const secret = req.headers.get("x-create-admin-secret")
  const expectedSecret = Deno.env.get("CREATE_ADMIN_SECRET")
  if (!expectedSecret || secret !== expectedSecret) {
    return new Response(
      JSON.stringify({ error: "Unauthorized" }),
      { status: 401, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  const supabase = createClient(supabaseUrl, serviceRoleKey, { auth: { autoRefreshToken: false, persistSession: false } })

  const email = "admin@fleury-insights.local"
  const password = "admin123"
  const username = "admin"
  const role = "super_admin"

  const { data: user, error: createError } = await supabase.auth.admin.createUser({
    email,
    password,
    email_confirm: true,
    user_metadata: { username, role },
  })

  if (createError) {
    if (createError.message.includes("already been registered")) {
      const { data: list } = await supabase.auth.admin.listUsers()
      const existing = list?.users?.find((u) => u.email === email)
      if (existing) {
        const { error: updateError } = await supabase.from("profiles").upsert(
          { id: existing.id, username, role },
          { onConflict: "id" }
        )
        if (updateError) {
          return new Response(
            JSON.stringify({ error: "Profile update failed", detail: updateError.message }),
            { status: 500, headers: { ...corsHeaders, "Content-Type": "application/json" } }
          )
        }
        return new Response(
          JSON.stringify({ message: "Admin user already exists; profile updated to super_admin." }),
          { headers: { ...corsHeaders, "Content-Type": "application/json" } }
        )
      }
    }
    return new Response(
      JSON.stringify({ error: "Create user failed", detail: createError.message }),
      { status: 400, headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )
  }

  return new Response(
    JSON.stringify({ message: "Admin created.", user_id: user.user?.id }),
    { headers: { ...corsHeaders, "Content-Type": "application/json" } }
  )
})
