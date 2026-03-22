import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers":
    "authorization, x-client-info, apikey, content-type",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
};

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

type OfficeServer = {
  id: string;
  office_id: string;
  public_base_url: string;
  base_path: string;
  status: string;
  is_active: boolean;
  connector_version: string | null;
  min_supported_connector_version: string | null;
  last_seen_at: string | null;
};

type MembershipRow = {
  office_id: string;
  role: string | null;
  panel_access: Record<string, boolean> | null;
};

type RequestContext = {
  admin: ReturnType<typeof createClient>;
  authClient: ReturnType<typeof createClient>;
  userToken: string;
  userId: string;
  officeId: string;
  officeRole: string | null;
  panelAccess: Record<string, boolean>;
  platformRole: string | null;
  server: OfficeServer;
  connectorSecretHash: string;
};

function isOwnerOrSuperAdmin(
  context: Pick<RequestContext, "officeRole" | "platformRole">,
) {
  return (
    context.platformRole === "super_admin" || context.officeRole === "owner"
  );
}

function hasAnyPanelAccess(
  context: Pick<RequestContext, "officeRole" | "platformRole" | "panelAccess">,
  panelKeys: string[],
) {
  if (isOwnerOrSuperAdmin(context)) return true;
  return panelKeys.some((panelKey) => context.panelAccess?.[panelKey] === true);
}

function validateRelativePath(input: string) {
  const trimmed = String(input ?? "").trim();
  if (!trimmed) {
    throw new Error("file_path is required");
  }

  const normalized = trimmed.replace(/\\/g, "/").replace(/^\/+/, "");
  if (!normalized) {
    throw new Error("file_path is required");
  }
  if (/^[a-zA-Z]:\//.test(normalized) || normalized.startsWith("//")) {
    throw new Error(
      "Apenas caminhos relativos ao diretório base são permitidos.",
    );
  }

  const parts = normalized.split("/").filter(Boolean);
  if (
    parts.length === 0 ||
    parts.some((part) => part === "." || part === "..")
  ) {
    throw new Error("Caminho inválido para download.");
  }

  return parts.join("/");
}

async function getContext(req: Request) {
  const userToken = req.headers
    .get("Authorization")
    ?.replace(/^Bearer\s+/i, "");
  if (!userToken)
    return { error: json({ error: "Missing authorization" }, 401) };

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
  const anonKey = Deno.env.get("SUPABASE_ANON_KEY")!;
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

  const authClient = createClient(supabaseUrl, anonKey, {
    global: { headers: { Authorization: `Bearer ${userToken}` } },
    auth: { autoRefreshToken: false, persistSession: false },
  });
  const {
    data: { user },
    error: authError,
  } = await authClient.auth.getUser();
  if (authError || !user)
    return {
      error: json({ error: "Unauthorized", detail: authError?.message }, 401),
    };

  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  const [
    { data: membership, error: membershipError },
    { data: profile, error: profileError },
  ] = await Promise.all([
    admin
      .from("office_memberships")
      .select("office_id, role, panel_access")
      .eq("user_id", user.id)
      .order("is_default", { ascending: false })
      .limit(1)
      .maybeSingle(),
    admin.from("profiles").select("role").eq("id", user.id).maybeSingle(),
  ]);

  if (membershipError)
    return { error: json({ error: membershipError.message }, 500) };
  if (profileError)
    return { error: json({ error: profileError.message }, 500) };
  if (!membership?.office_id)
    return {
      error: json({ error: "Nenhum escritório ativo encontrado." }, 403),
    };

  const { data: server, error: serverError } = await admin
    .from("office_servers")
    .select(
      "id, office_id, public_base_url, base_path, status, is_active, connector_version, min_supported_connector_version, last_seen_at",
    )
    .eq("office_id", membership.office_id)
    .eq("is_active", true)
    .maybeSingle();
  if (serverError) return { error: json({ error: serverError.message }, 500) };
  if (!server?.public_base_url) {
    return {
      error: json(
        { error: "Nenhum servidor ativo configurado para o escritório." },
        409,
      ),
    };
  }

  const { data: credential, error: credentialError } = await admin
    .from("office_server_credentials")
    .select("secret_hash")
    .eq("office_server_id", server.id)
    .maybeSingle();
  if (credentialError)
    return { error: json({ error: credentialError.message }, 500) };
  if (!credential?.secret_hash) {
    return {
      error: json(
        { error: "Credencial do conector não configurada para o escritório." },
        409,
      ),
    };
  }

  return {
    admin,
    authClient,
    userToken,
    userId: user.id,
    officeId: membership.office_id,
    officeRole: (membership as MembershipRow).role ?? null,
    panelAccess: (membership as MembershipRow).panel_access ?? {},
    platformRole: profile?.role ?? null,
    server: server as OfficeServer,
    connectorSecretHash: credential.secret_hash,
  } satisfies RequestContext;
}

async function getAuthorizedFilePaths(
  context: Pick<RequestContext, "authClient">,
  filePaths: string[],
) {
  const normalizedPaths = [
    ...new Set(filePaths.map((filePath) => validateRelativePath(filePath))),
  ];
  if (normalizedPaths.length === 0) return new Set<string>();

  const { data, error } = await context.authClient.rpc(
    "get_authorized_file_paths",
    {
      requested_paths: normalizedPaths,
    },
  );
  if (error) {
    throw new Error(
      error.message ||
        "Não foi possível validar os arquivos autorizados para download.",
    );
  }

  return new Set(
    (Array.isArray(data) ? data : [])
      .map((row) =>
        String((row as { file_path?: string | null })?.file_path ?? "").trim(),
      )
      .filter(Boolean),
  );
}

async function readError(response: Response) {
  const text = await response.text().catch(() => "");
  if (!text) return `Erro ${response.status}`;
  try {
    const payload = JSON.parse(text);
    return payload.error || payload.detail || text;
  } catch {
    return text;
  }
}

async function proxyJson(
  server: OfficeServer,
  connectorSecretHash: string,
  userToken: string,
  endpoint: string,
  init?: RequestInit,
) {
  const response = await fetch(`${server.public_base_url}${endpoint}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${connectorSecretHash}`,
      "X-Office-User-JWT": userToken,
      "Content-Type": "application/json",
      "ngrok-skip-browser-warning": "1",
      ...(init?.headers ?? {}),
    },
  });
  const text = await response.text().catch(() => "{}");
  return new Response(text, {
    status: response.status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

async function proxyBinary(
  server: OfficeServer,
  connectorSecretHash: string,
  userToken: string,
  endpoint: string,
  init?: RequestInit,
) {
  const response = await fetch(`${server.public_base_url}${endpoint}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${connectorSecretHash}`,
      "X-Office-User-JWT": userToken,
      "Content-Type": "application/json",
      "ngrok-skip-browser-warning": "1",
      ...(init?.headers ?? {}),
    },
  });
  if (!response.ok)
    return json({ error: await readError(response) }, response.status);

  const payload = await response.arrayBuffer();
  return new Response(payload, {
    status: 200,
    headers: {
      ...corsHeaders,
      "Content-Type":
        response.headers.get("Content-Type") ?? "application/octet-stream",
      "Content-Disposition":
        response.headers.get("Content-Disposition") ?? "attachment",
    },
  });
}

/** Repassa o stream do servidor para o cliente sem bufferar no edge (ZIP grande). */
async function proxyBinaryStream(
  server: OfficeServer,
  connectorSecretHash: string,
  userToken: string,
  endpoint: string,
  init?: RequestInit,
) {
  const response = await fetch(`${server.public_base_url}${endpoint}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${connectorSecretHash}`,
      "X-Office-User-JWT": userToken,
      "Content-Type": "application/json",
      "ngrok-skip-browser-warning": "1",
      ...(init?.headers ?? {}),
    },
  });
  if (!response.ok)
    return json({ error: await readError(response) }, response.status);
  if (!response.body)
    return json({ error: "Resposta vazia do servidor." }, 502);

  return new Response(response.body, {
    status: 200,
    headers: {
      ...corsHeaders,
      "Content-Type": response.headers.get("Content-Type") ?? "application/zip",
      "Content-Disposition":
        response.headers.get("Content-Disposition") ??
        'attachment; filename="documentos.zip"',
    },
  });
}

async function validateOfficeCompanies(
  admin: RequestContext["admin"],
  officeId: string,
  companyIds: string[],
) {
  const normalized = [
    ...new Set(companyIds.map((value) => String(value ?? "").trim()).filter(Boolean)),
  ];
  if (normalized.length === 0) return;

  const { data, error } = await admin
    .from("companies")
    .select("id")
    .eq("office_id", officeId)
    .in("id", normalized);
  if (error) throw error;
  if ((data?.length ?? 0) !== normalized.length) {
    throw new Error("Uma ou mais empresas nao pertencem ao escritorio atual.");
  }
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS")
    return new Response("ok", { headers: corsHeaders });

  const action = new URL(req.url).searchParams.get("action")?.trim() || "";
  const context = await getContext(req);
  if ("error" in context) return context.error;

  const { admin, userToken, officeId, server, connectorSecretHash } = context;

  if (action === "download-file") {
    if (!hasAnyPanelAccess(context, ["documentos", "fiscal", "paralegal"])) {
      return json(
        { error: "Sem permissão para baixar arquivos deste escritório." },
        403,
      );
    }

    const body = await req.json().catch(() => ({}));
    try {
      const filePath = validateRelativePath(String(body.file_path ?? ""));
      const authorizedPaths = await getAuthorizedFilePaths(context, [filePath]);
      if (!authorizedPaths.has(filePath)) {
        return json(
          { error: "Arquivo não autorizado para este escritório." },
          403,
        );
      }
      return proxyBinary(
        server,
        connectorSecretHash,
        userToken,
        `/api/files/download?path=${encodeURIComponent(filePath)}`,
        {
          method: "GET",
        },
      );
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : "Caminho inválido para download.";
      return json(
        { error: message },
        message.includes("autorizado") ? 403 : 400,
      );
    }
  }

  const MAX_FILES_ZIP_BY_PATHS = 50000;
  if (action === "download-zip-by-paths") {
    if (!hasAnyPanelAccess(context, ["documentos", "fiscal", "paralegal"])) {
      return json(
        { error: "Sem permissão para baixar arquivos deste escritório." },
        403,
      );
    }
    const body = await req.json().catch(() => ({}));
    const rawItems = Array.isArray(body.items) ? body.items : [];
    if (rawItems.length === 0)
      return json({ error: "Nenhum arquivo informado para o ZIP." }, 400);
    if (rawItems.length > MAX_FILES_ZIP_BY_PATHS) {
      return json(
        {
          error: `Limite de ${MAX_FILES_ZIP_BY_PATHS} arquivos por download. Selecione menos itens.`,
        },
        400,
      );
    }
    try {
      const items: Array<{
        file_path: string;
        company_name?: string;
        category?: string;
      }> = [];
      const requestedPaths: string[] = [];
      for (const it of rawItems) {
        try {
          const file_path = validateRelativePath(
            String(it?.file_path ?? "").trim(),
          );
          requestedPaths.push(file_path);
          items.push({
            file_path,
            company_name:
              typeof it?.company_name === "string"
                ? it.company_name.trim()
                : undefined,
            category:
              typeof it?.category === "string" ? it.category.trim() : undefined,
          });
        } catch {
          /* ignora item com path inválido */
        }
      }
      if (items.length === 0)
        return json({ error: "Nenhum caminho válido informado." }, 400);
      const authorizedPaths = await getAuthorizedFilePaths(
        context,
        requestedPaths,
      );
      if (authorizedPaths.size !== new Set(requestedPaths).size) {
        return json(
          {
            error:
              "Um ou mais arquivos solicitados não estão autorizados para este escritório.",
          },
          403,
        );
      }
      return proxyBinaryStream(
        server,
        connectorSecretHash,
        userToken,
        "/api/documents/download-zip-by-paths",
        {
          method: "POST",
          body: JSON.stringify({
            items: items.filter((item) => authorizedPaths.has(item.file_path)),
            filename_suffix: body.filename_suffix,
          }),
        },
      );
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : "Não foi possível validar os arquivos do ZIP.";
      return json(
        { error: message },
        message.includes("autorizado") ? 403 : 400,
      );
    }
  }

  if (action === "download-fiscal-document") {
    if (!hasAnyPanelAccess(context, ["fiscal"])) {
      return json(
        {
          error:
            "Sem permissão para baixar documentos fiscais deste escritório.",
        },
        403,
      );
    }

    const body = await req.json().catch(() => ({}));
    const documentId = String(body.document_id ?? "").trim();
    if (!documentId) return json({ error: "document_id is required" }, 400);

    const { data: document, error: documentError } = await admin
      .from("fiscal_documents")
      .select("id, file_path")
      .eq("office_id", officeId)
      .eq("id", documentId)
      .maybeSingle();
    if (documentError) return json({ error: documentError.message }, 500);
    if (!document?.file_path)
      return json({ error: "Documento não encontrado." }, 404);

    const result = await proxyBinary(
      server,
      connectorSecretHash,
      userToken,
      `/api/files/download?path=${encodeURIComponent(document.file_path)}`,
      { method: "GET" },
    );
    if (result.status === 200) {
      await admin
        .from("fiscal_documents")
        .update({ last_downloaded_at: new Date().toISOString() })
        .eq("office_id", officeId)
        .eq("id", documentId);
    }
    return result;
  }

  if (action === "list-declaration-artifacts") {
    if (!hasAnyPanelAccess(context, ["fiscal"])) {
      return json(
        { error: "Sem permissao para consultar documentos de declaracoes." },
        403,
      );
    }

    const body = await req.json().catch(() => ({}));
    try {
      await validateOfficeCompanies(
        admin,
        officeId,
        Array.isArray(body.company_ids) ? body.company_ids : [],
      );
    } catch (error) {
      return json(
        { error: error instanceof Error ? error.message : "Empresas invalidas." },
        403,
      );
    }

    return proxyJson(
      server,
      connectorSecretHash,
      userToken,
      "/api/declarations/artifacts/list",
      {
        method: "POST",
        body: JSON.stringify({
          action: body.action,
          company_ids: Array.isArray(body.company_ids) ? body.company_ids : [],
          competence: body.competence,
          limit: body.limit,
        }),
      },
    );
  }

  if (action === "download-declaration-artifact") {
    if (!hasAnyPanelAccess(context, ["fiscal"])) {
      return json(
        { error: "Sem permissao para baixar documentos de declaracoes." },
        403,
      );
    }

    const body = await req.json().catch(() => ({}));
    try {
      await validateOfficeCompanies(
        admin,
        officeId,
        body.company_id ? [body.company_id] : [],
      );
    } catch (error) {
      return json(
        { error: error instanceof Error ? error.message : "Empresa invalida." },
        403,
      );
    }

    return proxyBinary(
      server,
      connectorSecretHash,
      userToken,
      "/api/declarations/artifacts/download",
      {
        method: "POST",
        body: JSON.stringify({
          action: body.action,
          company_id: body.company_id,
          competence: body.competence,
          artifact_key: body.artifact_key,
        }),
      },
    );
  }

  const MAX_DOCS_PER_ZIP = 50000;

  if (action === "download-fiscal-documents-zip") {
    if (!hasAnyPanelAccess(context, ["fiscal"])) {
      return json(
        {
          error:
            "Sem permissão para baixar documentos fiscais deste escritório.",
        },
        403,
      );
    }

    const body = await req.json().catch(() => ({}));
    const ids = [
      ...new Set(
        Array.isArray(body.ids)
          ? body.ids.map((id) => String(id ?? "").trim()).filter(Boolean)
          : [],
      ),
    ];
    if (ids.length === 0)
      return json({ error: "Nenhum documento informado." }, 400);
    if (ids.length > MAX_DOCS_PER_ZIP)
      return json(
        {
          error: `Limite de ${MAX_DOCS_PER_ZIP} documentos por download. Selecione menos itens na lista.`,
        },
        400,
      );

    const { data: documents, error: documentsError } = await admin
      .from("fiscal_documents")
      .select("id")
      .eq("office_id", officeId)
      .in("id", ids);
    if (documentsError) return json({ error: documentsError.message }, 500);
    if ((documents?.length ?? 0) !== ids.length) {
      return json(
        { error: "Um ou mais documentos não pertencem ao escritório atual." },
        403,
      );
    }

    const result = await proxyBinary(
      server,
      connectorSecretHash,
      userToken,
      "/api/fiscal-documents/download-zip",
      {
        method: "POST",
        body: JSON.stringify({ ids }),
      },
    );
    if (result.status === 200) {
      await admin
        .from("fiscal_documents")
        .update({ last_downloaded_at: new Date().toISOString() })
        .eq("office_id", officeId)
        .in("id", ids);
    }
    return result;
  }

  if (action === "download-fiscal-companies-zip") {
    if (!hasAnyPanelAccess(context, ["fiscal"])) {
      return json(
        {
          error:
            "Sem permissão para baixar documentos fiscais deste escritório.",
        },
        403,
      );
    }

    const body = await req.json().catch(() => ({}));
    const companyIds = [
      ...new Set(
        Array.isArray(body.company_ids)
          ? body.company_ids
              .map((id) => String(id ?? "").trim())
              .filter(Boolean)
          : [],
      ),
    ];
    if (companyIds.length === 0)
      return json({ error: "Nenhuma empresa informada." }, 400);

    const { data: companies, error: companiesError } = await admin
      .from("companies")
      .select("id")
      .eq("office_id", officeId)
      .in("id", companyIds);
    if (companiesError) return json({ error: companiesError.message }, 500);
    if ((companies?.length ?? 0) !== companyIds.length) {
      return json(
        { error: "Uma ou mais empresas não pertencem ao escritório atual." },
        403,
      );
    }

    return proxyBinary(
      server,
      connectorSecretHash,
      userToken,
      "/api/fiscal-documents/download-zip",
      {
        method: "POST",
        body: JSON.stringify({
          company_ids: companyIds,
          types: Array.isArray(body.types) ? body.types : [],
        }),
      },
    );
  }

  if (action === "download-hub-companies-zip") {
    if (!hasAnyPanelAccess(context, ["documentos", "fiscal", "paralegal"])) {
      return json(
        { error: "Sem permissão para baixar documentos deste escritório." },
        403,
      );
    }

    const body = await req.json().catch(() => ({}));
    const companyIds = [
      ...new Set(
        Array.isArray(body.company_ids)
          ? body.company_ids
              .map((id) => String(id ?? "").trim())
              .filter(Boolean)
          : [],
      ),
    ];
    if (companyIds.length === 0)
      return json({ error: "Nenhuma empresa informada." }, 400);

    const { data: companies, error: companiesError } = await admin
      .from("companies")
      .select("id")
      .eq("office_id", officeId)
      .in("id", companyIds);
    if (companiesError) return json({ error: companiesError.message }, 500);
    if ((companies?.length ?? 0) !== companyIds.length) {
      return json(
        { error: "Uma ou mais empresas não pertencem ao escritório atual." },
        403,
      );
    }

    return proxyBinary(
      server,
      connectorSecretHash,
      userToken,
      "/api/hub-documents/download-zip",
      {
        method: "POST",
        body: JSON.stringify({
          company_ids: companyIds,
          categories: Array.isArray(body.categories) ? body.categories : [],
        }),
      },
    );
  }

  if (action === "fiscal-sync-all") {
    if (!isOwnerOrSuperAdmin(context)) {
      return json(
        { error: "Apenas owner pode iniciar a sincronização fiscal completa." },
        403,
      );
    }

    const response = await fetch(
      `${server.public_base_url}/api/fiscal-sync-all`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${connectorSecretHash}`,
          "X-Office-User-JWT": userToken,
          "Content-Type": "application/json",
          "ngrok-skip-browser-warning": "1",
        },
        body: JSON.stringify({}),
      },
    );
    const text = await response.text().catch(() => "{}");
    return new Response(text, {
      status: response.status,
      headers: { ...corsHeaders, "Content-Type": "application/json" },
    });
  }

  if (action === "status") {
    return json({ office_server: server });
  }

  return json({ error: "Unsupported action" }, 400);
});
