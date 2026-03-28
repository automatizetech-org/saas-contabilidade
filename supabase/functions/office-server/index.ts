import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { createRemoteJWKSet, jwtVerify } from "https://esm.sh/jose@5.9.6";

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

const jwksCache = new Map<string, ReturnType<typeof createRemoteJWKSet>>();

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

function sanitizeCompanyFolderName(input: string) {
  const normalized = String(input ?? "")
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "");
  return normalized.replace(/[^A-Za-z0-9 _.-]/g, "").trim() || "EMPRESA";
}

async function buildRequestedPathCandidates(
  context: Pick<RequestContext, "admin" | "officeId">,
  filePath: string,
) {
  const normalized = validateRelativePath(filePath);
  const parts = normalized.split("/").filter(Boolean);
  if (parts.length === 0) return [normalized];

  const [requestedCompanyFolder, ...restParts] = parts;
  const suffix = restParts.join("/");
  const candidates = new Set<string>([normalized]);
  const requestedSanitized = sanitizeCompanyFolderName(requestedCompanyFolder);

  if (suffix && requestedSanitized !== requestedCompanyFolder) {
    candidates.add(`${requestedSanitized}/${suffix}`);
  }

  const { data: companies, error } = await context.admin
    .from("companies")
    .select("name")
    .eq("office_id", context.officeId);
  if (error) {
    throw new Error(
      error.message || "Não foi possível resolver aliases de pastas da empresa.",
    );
  }

  for (const row of companies ?? []) {
    const rawName = String(row.name ?? "").trim();
    if (!rawName) continue;
    const sanitizedName = sanitizeCompanyFolderName(rawName);
    if (
      rawName === requestedCompanyFolder ||
      sanitizedName === requestedCompanyFolder ||
      sanitizedName === requestedSanitized
    ) {
      if (suffix) {
        candidates.add(`${rawName}/${suffix}`);
        candidates.add(`${sanitizedName}/${suffix}`);
      } else {
        candidates.add(rawName);
        candidates.add(sanitizedName);
      }
    }
  }

  return [...candidates];
}

/**
 * Sessão ativa no GoTrue. Usar **somente a anon key** no header `apikey`:
 * com `service_role` + JWT de usuário o GoTrue costuma responder 401/403 indevido.
 */
async function resolveUserIdFromGoTrue(
  supabaseUrl: string,
  anonKey: string,
  accessToken: string,
): Promise<
  | { ok: true; id: string }
  | { ok: false; detail: string }
> {
  const base = supabaseUrl.replace(/\/$/, "");
  let lastDetail =
    "Sessão inválida ou expirada. Faça login de novo (ou atualize a página).";

  try {
    const supabase = createClient(supabaseUrl, anonKey, {
      global: { headers: { Authorization: `Bearer ${accessToken}` } },
      auth: { autoRefreshToken: false, persistSession: false },
    });
    const { data, error } = await supabase.auth.getUser(accessToken);
    if (!error && data?.user?.id) {
      return { ok: true, id: data.user.id };
    }
    if (error?.message) lastDetail = error.message;
  } catch (e) {
    lastDetail = e instanceof Error ? e.message : String(e);
  }

  const res = await fetch(`${base}/auth/v1/user`, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      apikey: anonKey,
    },
  });

  if (res.ok) {
    try {
      const body = (await res.json()) as { id?: string };
      if (body?.id) return { ok: true, id: body.id };
    } catch {
      /* ignore */
    }
    lastDetail = "Resposta inválida do servidor de autenticação.";
    return { ok: false, detail: lastDetail };
  }

  try {
    const j = (await res.json()) as {
      msg?: string;
      error_description?: string;
      message?: string;
    };
    lastDetail =
      j.msg ||
      j.error_description ||
      j.message ||
      `Auth HTTP ${res.status}`;
  } catch {
    lastDetail = `Auth HTTP ${res.status}`;
  }

  try {
    const base = supabaseUrl.replace(/\/$/, "");
    let jwks = jwksCache.get(base);
    if (!jwks) {
      jwks = createRemoteJWKSet(new URL(`${base}/auth/v1/.well-known/jwks.json`));
      jwksCache.set(base, jwks);
    }
    const { payload } = await jwtVerify(accessToken, jwks, {
      issuer: `${base}/auth/v1`,
      audience: "authenticated",
    });
    if (typeof payload.sub === "string" && payload.sub.trim()) {
      return { ok: true, id: payload.sub.trim() };
    }
    lastDetail = "JWT válido, mas sem subject do usuário.";
  } catch (e) {
    lastDetail = e instanceof Error ? e.message : String(e);
  }

  return { ok: false, detail: lastDetail };
}

async function getContext(req: Request) {
  const userToken = req.headers
    .get("Authorization")
    ?.replace(/^Bearer\s+/i, "")
    ?.trim();
  if (!userToken)
    return { error: json({ error: "Missing authorization" }, 401) };

  const supabaseUrl = Deno.env.get("SUPABASE_URL")!;
  const anonKey = Deno.env.get("SUPABASE_ANON_KEY")!;
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!;

  const authUser = await resolveUserIdFromGoTrue(
    supabaseUrl,
    anonKey,
    userToken,
  );

  if (!authUser.ok) {
    return {
      error: json(
        {
          error: "Unauthorized",
          detail: authUser.detail,
        },
        401,
      ),
    };
  }

  const user = { id: authUser.id };

  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  const authClient = createClient(supabaseUrl, anonKey, {
    global: { headers: { Authorization: `Bearer ${userToken}` } },
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
  context: Pick<RequestContext, "authClient" | "admin" | "officeId">,
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

  const authorized = new Set(
    (Array.isArray(data) ? data : [])
      .map((row) =>
        String((row as { file_path?: string | null })?.file_path ?? "").trim(),
      )
      .filter(Boolean),
  );

  const pendingCertidaoPaths = normalizedPaths.filter(
    (filePath) =>
      !authorized.has(filePath) &&
      /\/fiscal\/certidoes\/\d{4}\/\d{2}\/\d{2}\//i.test(filePath),
  );
  if (pendingCertidaoPaths.length === 0) return authorized;

  const requestedCompanyFolders = [
    ...new Set(
      pendingCertidaoPaths
        .map((filePath) => filePath.split("/")[0]?.trim())
        .filter(Boolean),
    ),
  ];
  if (requestedCompanyFolders.length === 0) return authorized;

  const { data: companies, error: companiesError } = await context.admin
    .from("companies")
    .select("id, name")
    .eq("office_id", context.officeId);
  if (companiesError) {
    throw new Error(
      companiesError.message ||
        "Não foi possível validar as empresas das certidões.",
    );
  }

  const companyMap = new Map<string, string>();
  for (const company of companies ?? []) {
    const companyId = String(company.id ?? "").trim();
    const companyName = String(company.name ?? "").trim();
    if (!companyId || !companyName) continue;
    const sanitizedName = sanitizeCompanyFolderName(companyName);
    if (
      requestedCompanyFolders.includes(companyName) ||
      requestedCompanyFolders.includes(sanitizedName)
    ) {
      companyMap.set(companyId, companyName);
    }
  }
  const companyIds = [...companyMap.keys()].filter(Boolean);
  if (companyIds.length === 0) return authorized;

  const { data: certRows, error: certError } = await context.admin
    .from("sync_events")
    .select("company_id, payload")
    .eq("office_id", context.officeId)
    .eq("tipo", "certidao_resultado")
    .in("company_id", companyIds);
  if (certError) {
    throw new Error(
      certError.message ||
        "Não foi possível validar os arquivos de certidão.",
    );
  }

  const pendingSet = new Set(pendingCertidaoPaths);
  for (const row of certRows ?? []) {
    let payload: Record<string, unknown> = {};
    try {
      payload = JSON.parse(String(row.payload ?? "{}"));
    } catch {
      payload = {};
    }

    const explicitPath = String(payload.arquivo_pdf ?? "").trim();
    if (explicitPath && pendingSet.has(explicitPath)) {
      authorized.add(explicitPath);
    }

    const companyName =
      companyMap.get(String(row.company_id ?? "").trim()) ?? "";
    const tipo = String(payload.tipo_certidao ?? "").trim();
    const documentDate = String(
      payload.document_date ?? payload.data_consulta ?? "",
    ).slice(0, 10);
    if (!companyName || !tipo || !/^\d{4}-\d{2}-\d{2}$/.test(documentDate)) {
      continue;
    }
    const [year, month, day] = documentDate.split("-");
    const fallbackRawPath =
      `${companyName}/FISCAL/CERTIDOES/${year}/${month}/${day}/${tipo}.pdf`;
    const fallbackSanitizedPath =
      `${sanitizeCompanyFolderName(companyName)}/FISCAL/CERTIDOES/${year}/${month}/${day}/${tipo}.pdf`;
    if (pendingSet.has(fallbackRawPath)) {
      authorized.add(fallbackRawPath);
    }
    if (pendingSet.has(fallbackSanitizedPath)) {
      authorized.add(fallbackSanitizedPath);
    }
  }

  return authorized;
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
      const requestedPath = validateRelativePath(String(body.file_path ?? ""));
      const candidatePaths = await buildRequestedPathCandidates(
        context,
        requestedPath,
      );
      const authorizedPaths = await getAuthorizedFilePaths(
        context,
        candidatePaths,
      );
      const allowedCandidates = candidatePaths.filter((filePath) =>
        authorizedPaths.has(filePath),
      );
      if (allowedCandidates.length === 0) {
        return json(
          { error: "Arquivo não autorizado para este escritório." },
          403,
        );
      }
      let lastResponse: Response | null = null;
      for (const filePath of allowedCandidates) {
        const response = await proxyBinary(
          server,
          connectorSecretHash,
          userToken,
          `/api/files/download?path=${encodeURIComponent(filePath)}`,
          {
            method: "GET",
          },
        );
        if (response.status === 200) return response;
        lastResponse = response;
      }
      return lastResponse ?? json({ error: "Arquivo não encontrado." }, 404);
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
        zip_inner_segment?: string;
      }> = [];
      const requestedPaths: string[] = [];
      for (const it of rawItems) {
        try {
          const requestedPath = validateRelativePath(
            String(it?.file_path ?? "").trim(),
          );
          const candidatePaths = await buildRequestedPathCandidates(
            context,
            requestedPath,
          );
          requestedPaths.push(...candidatePaths);
          const rawInner =
            typeof it?.zip_inner_segment === "string"
              ? it.zip_inner_segment.trim()
              : "";
          const zip_inner_segment =
            rawInner === "55" || rawInner === "65" ? rawInner : undefined;
          items.push({
            file_path: requestedPath,
            requested_candidates: candidatePaths,
            company_name:
              typeof it?.company_name === "string"
                ? it.company_name.trim()
                : undefined,
            category:
              typeof it?.category === "string" ? it.category.trim() : undefined,
            ...(zip_inner_segment ? { zip_inner_segment } : {}),
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
      const resolvedItems = items
        .map((item) => {
          const resolvedPath =
            item.requested_candidates.find((candidate) =>
              authorizedPaths.has(candidate),
            ) ?? null;
          return resolvedPath ? { ...item, file_path: resolvedPath } : null;
        })
        .filter(Boolean) as Array<{
          file_path: string;
          company_name?: string;
          category?: string;
          zip_inner_segment?: string;
          requested_candidates: string[];
        }>;
      if (resolvedItems.length !== items.length) {
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
            items: resolvedItems.map((item) => ({
              file_path: item.file_path,
              company_name: item.company_name,
              category: item.category,
              ...(item.zip_inner_segment
                ? { zip_inner_segment: item.zip_inner_segment }
                : {}),
            })),
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

  if (action === "stop-robot-runtime") {
    if (!hasAnyPanelAccess(context, ["fiscal"])) {
      return json(
        { error: "Sem permissao para controlar a execucao dos robos fiscais." },
        403,
      );
    }

    const body = await req.json().catch(() => ({}));
    const robotTechnicalIds = Array.isArray(body.robot_technical_ids)
      ? body.robot_technical_ids
          .map((value) => String(value ?? "").trim())
          .filter(Boolean)
      : [];
    if (robotTechnicalIds.length === 0) {
      return json({ error: "robot_technical_ids is required" }, 400);
    }

    return proxyJson(
      server,
      connectorSecretHash,
      userToken,
      "/api/robots/runtime/stop",
      {
        method: "POST",
        body: JSON.stringify({
          robot_technical_ids: robotTechnicalIds,
          reason: body.reason,
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

  if (action === "whatsapp") {
    if (!hasAnyPanelAccess(context, ["alteracao_empresarial"])) {
      return json(
        {
          error:
            "Sem permissao para WhatsApp (alteracao empresarial) neste escritorio.",
        },
        403,
      );
    }

    const body = await req.json().catch(() => ({}));
    const call = String(body.call ?? "").trim();
    const allowed = new Set([
      "status",
      "qr",
      "groups",
      "connect",
      "disconnect",
      "send",
      "deliver",
    ]);
    if (!allowed.has(call)) {
      return json(
        {
          error: "call invalido",
          detail:
            "Envie JSON com call em: status, qr, groups, connect, disconnect, send, deliver.",
          received: call || null,
        },
        400,
      );
    }

    const q =
      typeof body.query === "string" ? body.query.replace(/^\?/, "") : "";
    const basePath = `/api/whatsapp/${call}`;
    const pathWithQuery =
      call === "groups" && q ? `${basePath}?${q}` : basePath;
    const method =
      call === "send" ||
      call === "deliver" ||
      call === "connect" ||
      call === "disconnect"
        ? "POST"
        : "GET";

    let postBody: string | undefined;
    if (method === "POST") {
      if (call === "connect" || call === "disconnect") {
        postBody = "{}";
      } else {
        postBody = JSON.stringify(
          body.payload && typeof body.payload === "object"
            ? body.payload
            : {},
        );
      }
    }

    const response = await fetch(
      `${server.public_base_url}${pathWithQuery}`,
      {
        method,
        headers: {
          Authorization: `Bearer ${connectorSecretHash}`,
          "X-Office-User-JWT": userToken,
          "Content-Type": "application/json",
          "ngrok-skip-browser-warning": "1",
        },
        body: postBody,
      },
    );

    const text = await response.text().catch(() => "");
    return new Response(text, {
      status: response.status,
      headers: {
        ...corsHeaders,
        "Content-Type":
          response.headers.get("Content-Type") ?? "application/json",
      },
    });
  }

  if (action === "status") {
    return json({ office_server: server });
  }

  return json(
    {
      error: "Unsupported action",
      action: action || null,
      hint:
        "Deploy a Edge Function office-server atualizada (ex.: action=whatsapp).",
    },
    400,
  );
});
