// manage-office: apenas super_admin pode inativar ou excluir escritório.
// set_status: office_id + status ('active'|'inactive'). delete: office_id + confirm_delete:true + confirm_slug.
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

function uuidRegex(): RegExp {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") return new Response("ok", { headers: corsHeaders })
  if (req.method !== "POST") return json({ error: "Method not allowed" }, 405)

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
  if (authError || !user) return json({ error: "Unauthorized", detail: authError?.message }, 401)

  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  })

  const { data: profile } = await admin.from("profiles").select("role").eq("id", user.id).single()
  if (profile?.role !== "super_admin") {
    return json({ error: "Apenas super_admin pode inativar ou excluir escritórios." }, 403)
  }

  let body: { action?: string; office_id?: string; status?: string; confirm_delete?: boolean; confirm_slug?: string }
  try {
    body = await req.json()
  } catch {
    return json({ error: "Invalid JSON body" }, 400)
  }

  const action = String(body.action ?? "").trim().toLowerCase()
  const officeId = String(body.office_id ?? "").trim()
  if (!officeId || !uuidRegex().test(officeId)) {
    return json({ error: "office_id inválido ou ausente." }, 400)
  }

  const { data: office, error: fetchError } = await admin
    .from("offices")
    .select("id, name, slug, status")
    .eq("id", officeId)
    .maybeSingle()
  if (fetchError) return json({ error: fetchError.message }, 500)
  if (!office) return json({ error: "Escritório não encontrado." }, 404)

  if (action === "set_status") {
    const status = String(body.status ?? "").trim().toLowerCase()
    if (status !== "active" && status !== "inactive") {
      return json({ error: "status deve ser 'active' ou 'inactive'." }, 400)
    }
    const { error: updateError } = await admin
      .from("offices")
      .update({ status: status as "active" | "inactive", updated_at: new Date().toISOString() })
      .eq("id", officeId)
    if (updateError) return json({ error: updateError.message }, 500)
    return json({ message: status === "inactive" ? "Escritório inativado." : "Escritório reativado.", office_id: officeId })
  }

  if (action === "delete") {
    if (body.confirm_delete !== true) {
      return json({ error: "Confirme a exclusão enviando confirm_delete: true." }, 400)
    }
    const confirmSlug = String(body.confirm_slug ?? "").trim().toLowerCase()
    const officeSlug = String(office.slug ?? "").trim().toLowerCase()
    if (confirmSlug !== officeSlug) {
      return json({
        error: "Confirmação incorreta. Digite o slug do escritório exatamente como exibido.",
        expected_slug: office.slug,
      }, 400)
    }
    // Usuários do escritório: excluir do Auth (profile cai em cascata) antes de excluir o escritório
    const { data: memberships } = await admin
      .from("office_memberships")
      .select("user_id")
      .eq("office_id", officeId)
    const callerId = user.id
    const userIds = (memberships ?? [])
      .map((m) => m.user_id)
      .filter((id): id is string => !!id && id !== callerId)
    for (const uid of userIds) {
      await admin.auth.admin.deleteUser(uid)
    }
    const { error: deleteError } = await admin.from("offices").delete().eq("id", officeId)
    if (deleteError) return json({ error: deleteError.message }, 500)
    return json({
      message: "Escritório excluído permanentemente (dados e usuários do escritório em cascata).",
      office_id: officeId,
      users_deleted: userIds.length,
    })
  }

  return json({ error: "action deve ser 'set_status' ou 'delete'." }, 400)
})
