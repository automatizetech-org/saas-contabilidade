import { createClient } from "https://esm.sh/@supabase/supabase-js@2"

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
}

const ownerPanelAccess = {
  dashboard: true,
  fiscal: true,
  dp: true,
  contabil: true,
  inteligencia_tributaria: true,
  ir: true,
  paralegal: false,
  financeiro: true,
  operacoes: true,
  documentos: true,
  empresas: true,
  alteracao_empresarial: true,
  sync: true,
}

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  })
}

async function sha256(text: string) {
  const bytes = new TextEncoder().encode(text)
  const hash = await crypto.subtle.digest("SHA-256", bytes)
  return Array.from(new Uint8Array(hash))
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("")
}

function generateSecret() {
  const bytes = crypto.getRandomValues(new Uint8Array(32))
  return Array.from(bytes)
    .map((value) => value.toString(16).padStart(2, "0"))
    .join("")
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
  if (authError || !user) return json({ error: "Unauthorized" }, 401)

  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  })

  const { data: callerProfile } = await admin
    .from("profiles")
    .select("role")
    .eq("id", user.id)
    .single()
  if (callerProfile?.role !== "super_admin") return json({ error: "Forbidden" }, 403)

  let body: {
    office_name?: string
    office_slug?: string
    admin_email?: string
    admin_password?: string
    admin_username?: string
    public_base_url?: string
    base_path?: string
    connector_version?: string | null
    min_supported_connector_version?: string | null
  }
  try {
    body = await req.json()
  } catch {
    return json({ error: "Invalid JSON body" }, 400)
  }

  const officeName = String(body.office_name ?? "").trim()
  const officeSlug = String(body.office_slug ?? "").trim().toLowerCase()
  const adminEmail = String(body.admin_email ?? "").trim().toLowerCase()
  const adminPassword = String(body.admin_password ?? "")
  const adminUsername = String(body.admin_username ?? "").trim()
  const publicBaseUrl = String(body.public_base_url ?? "").trim().replace(/\/+$/, "")
  const basePath = String(body.base_path ?? "").trim()
  const connectorVersion = String(body.connector_version ?? "").trim() || null
  const minSupportedConnectorVersion = String(body.min_supported_connector_version ?? "").trim() || null

  if (!officeName || !officeSlug || !adminEmail || !adminPassword || !adminUsername || !publicBaseUrl || !basePath) {
    return json({ error: "Campos obrigatórios ausentes." }, 400)
  }

  const { data: existingOffice } = await admin
    .from("offices")
    .select("id")
    .ilike("slug", officeSlug)
    .maybeSingle()
  if (existingOffice) return json({ error: "Já existe um escritório com esse slug." }, 409)

  const existingUsers = await admin.auth.admin.listUsers({ page: 1, perPage: 1000 })
  if (existingUsers.error) return json({ error: existingUsers.error.message }, 500)
  const existingByEmail = (existingUsers.data?.users ?? []).find((candidate) => candidate.email?.toLowerCase() === adminEmail)
  if (existingByEmail) return json({ error: "O e-mail do administrador já está em uso." }, 409)

  const { data: office, error: officeError } = await admin
    .from("offices")
    .insert({
      name: officeName,
      slug: officeSlug,
      status: "active",
    })
    .select("id, name, slug, status")
    .single()
  if (officeError || !office) return json({ error: "Office creation failed", detail: officeError?.message }, 400)

  const { data: createdUser, error: createUserError } = await admin.auth.admin.createUser({
    email: adminEmail,
    password: adminPassword,
    email_confirm: true,
    user_metadata: {
      username: adminUsername,
      display_name: adminUsername,
      full_name: adminUsername,
    },
  })
  if (createUserError || !createdUser.user?.id) {
    await admin.from("offices").delete().eq("id", office.id)
    return json({ error: "Create admin user failed", detail: createUserError?.message }, 400)
  }

  const adminUserId = createdUser.user.id
  const connectorSecret = generateSecret()
  const serverSecretHash = await sha256(connectorSecret)

  const [profileResult, membershipResult, brandingResult] = await Promise.all([
    admin.from("profiles").upsert(
      {
        id: adminUserId,
        username: adminUsername,
        role: "user",
        updated_at: new Date().toISOString(),
      },
      { onConflict: "id" }
    ),
    admin.from("office_memberships").insert({
      office_id: office.id,
      user_id: adminUserId,
      role: "owner",
      panel_access: ownerPanelAccess,
      is_default: true,
    }),
    admin.from("office_branding").insert({ office_id: office.id }),
  ])

  if (profileResult.error || membershipResult.error || brandingResult.error) {
    await admin.from("office_memberships").delete().eq("office_id", office.id)
    await admin.from("office_branding").delete().eq("office_id", office.id)
    await admin.from("office_servers").delete().eq("office_id", office.id)
    await admin.from("offices").delete().eq("id", office.id)
    await admin.auth.admin.deleteUser(adminUserId)
    return json(
      {
        error: "Falha ao concluir o onboarding do escritório.",
        detail:
          profileResult.error?.message ||
          membershipResult.error?.message ||
          brandingResult.error?.message ||
          "Unknown error",
      },
      400
    )
  }

  const { data: server, error: serverError } = await admin
    .from("office_servers")
    .insert({
      office_id: office.id,
      public_base_url: publicBaseUrl,
      base_path: basePath,
      connector_version: connectorVersion,
      min_supported_connector_version: minSupportedConnectorVersion,
      status: "pending",
      is_active: true,
    })
    .select("id")
    .single()

  if (serverError || !server?.id) {
    await admin.from("office_memberships").delete().eq("office_id", office.id)
    await admin.from("office_branding").delete().eq("office_id", office.id)
    await admin.from("office_servers").delete().eq("office_id", office.id)
    await admin.from("offices").delete().eq("id", office.id)
    await admin.auth.admin.deleteUser(adminUserId)
    return json(
      {
        error: "Falha ao registrar o servidor do escritÃ³rio.",
        detail: serverError?.message || "Unknown error",
      },
      400
    )
  }

  const { error: credentialError } = await admin.from("office_server_credentials").insert({
    office_server_id: server.id,
    secret_hash: serverSecretHash,
  })

  if (credentialError) {
    await admin.from("office_memberships").delete().eq("office_id", office.id)
    await admin.from("office_branding").delete().eq("office_id", office.id)
    await admin.from("office_server_credentials").delete().eq("office_server_id", server.id)
    await admin.from("office_servers").delete().eq("office_id", office.id)
    await admin.from("offices").delete().eq("id", office.id)
    await admin.auth.admin.deleteUser(adminUserId)
    return json(
      {
        error: "Falha ao registrar as credenciais do conector do escritÃ³rio.",
        detail: credentialError.message,
      },
      400
    )
  }

  const folderCopy = await admin.rpc("copy_default_folder_structure", {
    target_office_id: office.id,
  })
  if (folderCopy.error) {
    await admin.from("office_memberships").delete().eq("office_id", office.id)
    await admin.from("office_branding").delete().eq("office_id", office.id)
    await admin.from("office_servers").delete().eq("office_id", office.id)
    await admin.from("offices").delete().eq("id", office.id)
    await admin.auth.admin.deleteUser(adminUserId)
    return json(
      {
        error: "Falha ao copiar a estrutura padrão do escritório.",
        detail: folderCopy.error.message,
      },
      500
    )
  }

  await admin.from("office_audit_logs").insert({
    office_id: office.id,
    actor_user_id: user.id,
    action: "office.bootstrap",
    entity_type: "office",
    entity_id: office.id,
    payload: {
      admin_user_id: adminUserId,
      public_base_url: publicBaseUrl,
      base_path: basePath,
    },
  })

  return json({
    message: "Primeiro escritório criado com sucesso.",
    office,
    admin_user_id: adminUserId,
    connector_secret: connectorSecret,
  })
})
