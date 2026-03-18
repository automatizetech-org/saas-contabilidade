import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type",
};

type ChargeType = "PIX" | "BOLETO" | "BOLETO_HIBRIDO";

type IrClientRow = {
  id: string;
  nome: string;
  cpf_cnpj: string;
  valor_servico: number;
  observacoes: string | null;
  status_pagamento: string;
  payment_metadata: Record<string, unknown> | null;
};

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

function onlyDigits(value: string) {
  return value.replace(/\D/g, "");
}

function sanitizeName(value: string) {
  return value.normalize("NFD").replace(/[\u0300-\u036f]/g, "").slice(0, 120);
}

function resolveBtgChargeKind(chargeType: ChargeType) {
  if (chargeType === "PIX") return "DUE_DATE_QRCODE";
  if (chargeType === "BOLETO_HIBRIDO") return "BANKSLIP_QRCODE";
  return "BANKSLIP";
}

async function getAccessToken() {
  const tokenUrl = Deno.env.get("BTG_TOKEN_URL");
  const clientId = Deno.env.get("BTG_CLIENT_ID");
  const clientSecret = Deno.env.get("BTG_CLIENT_SECRET");
  const scope = Deno.env.get("BTG_SCOPE") ?? "openid brn:btg:empresas:banking:collections";

  if (!tokenUrl || !clientId || !clientSecret) {
    throw new Error("BTG_TOKEN_URL, BTG_CLIENT_ID e BTG_CLIENT_SECRET precisam estar configurados.");
  }

  const response = await fetch(tokenUrl, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "client_credentials",
      client_id: clientId,
      client_secret: clientSecret,
      scope,
    }),
  });

  if (!response.ok) {
    throw new Error(`Falha ao autenticar no BTG (${response.status}).`);
  }

  const data = await response.json();
  if (!data.access_token) {
    throw new Error("BTG nao retornou access_token.");
  }

  return data.access_token as string;
}

function buildChargePayload(client: IrClientRow, chargeType: ChargeType, webhookUrl: string, correlationId: string) {
  const taxId = onlyDigits(client.cpf_cnpj);
  const isCompany = taxId.length > 11;

  return {
    type: resolveBtgChargeKind(chargeType),
    amount: Number(client.valor_servico),
    dueDate: new Date().toISOString().slice(0, 10),
    overDueDate: new Date(Date.now() + 1000 * 60 * 60 * 24 * 30).toISOString().slice(0, 10),
    correlationId,
    webhookUrl,
    payer: {
      name: sanitizeName(client.nome),
      taxId,
      personType: isCompany ? "J" : "F",
    },
    description: `IR ${sanitizeName(client.nome)}`.slice(0, 120),
    detail: {
      externalId: client.id,
      documentNumber: `IR-${client.id.slice(0, 8)}`,
    },
  };
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  if (req.method !== "POST") {
    return jsonResponse({ error: "Method not allowed" }, 405);
  }

  const authHeader = req.headers.get("Authorization");
  if (!authHeader) {
    return jsonResponse({ error: "Nao autenticado." }, 401);
  }

  const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
  const anonKey = Deno.env.get("SUPABASE_ANON_KEY") ?? "";
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";

  if (!supabaseUrl || !anonKey || !serviceRoleKey) {
    return jsonResponse({ error: "Configuracao do Supabase incompleta." }, 500);
  }

  const supabaseAuth = createClient(supabaseUrl, anonKey, {
    global: { headers: { Authorization: authHeader } },
    auth: { autoRefreshToken: false, persistSession: false },
  });
  const { data: authData, error: authError } = await supabaseAuth.auth.getUser();
  if (authError || !authData.user) {
    return jsonResponse({ error: "Nao autenticado." }, 401);
  }

  let body: { clientId?: string; chargeType?: ChargeType };
  try {
    body = await req.json();
  } catch {
    return jsonResponse({ error: "JSON invalido." }, 400);
  }

  if (!body.clientId || !body.chargeType || !["PIX", "BOLETO", "BOLETO_HIBRIDO"].includes(body.chargeType)) {
    return jsonResponse({ error: "clientId e chargeType sao obrigatorios." }, 400);
  }

  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  const { data: clientRow, error: clientError } = await admin
    .from("ir_clients")
    .select("id, nome, cpf_cnpj, valor_servico, observacoes, status_pagamento, payment_metadata")
    .eq("id", body.clientId)
    .single();

  if (clientError || !clientRow) {
    return jsonResponse({ error: "Cliente IR nao encontrado." }, 404);
  }

  const btgCollectionsUrl = Deno.env.get("BTG_COLLECTIONS_URL");
  const btgWebhookBaseUrl = Deno.env.get("BTG_IR_WEBHOOK_URL");
  if (!btgCollectionsUrl || !btgWebhookBaseUrl) {
    return jsonResponse({ error: "BTG_COLLECTIONS_URL e BTG_IR_WEBHOOK_URL precisam estar configurados." }, 500);
  }

  const correlationId = crypto.randomUUID();
  const token = await getAccessToken();
  const payload = buildChargePayload(clientRow as IrClientRow, body.chargeType, btgWebhookBaseUrl, correlationId);

  const btgResponse = await fetch(btgCollectionsUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      Accept: "application/json",
    },
    body: JSON.stringify(payload),
  });

  const btgData = await btgResponse.json().catch(() => ({}));
  if (!btgResponse.ok) {
    return jsonResponse({ error: "Falha ao gerar cobranca no BTG.", detail: btgData }, 502);
  }

  const detail = (btgData.detail ?? {}) as Record<string, unknown>;
  const metadata = {
    requestPayload: payload,
    responsePayload: btgData,
  };

  const updates = {
    payment_charge_type: body.chargeType,
    payment_charge_status: "pending",
    payment_charge_id: String(btgData.collectionId ?? btgData.chargeId ?? ""),
    payment_charge_correlation_id: String(btgData.correlationId ?? correlationId),
    payment_provider: "BTG",
    payment_link: String((detail.link as string | undefined) ?? (detail.url as string | undefined) ?? btgData.url ?? ""),
    payment_pix_copy_paste: String((detail.emv as string | undefined) ?? btgData.emv ?? ""),
    payment_pix_qr_code: String((detail.qrCode as string | undefined) ?? btgData.qrCode ?? ""),
    payment_boleto_pdf_base64: String((detail.pdfBase64 as string | undefined) ?? btgData.pdfBase64 ?? ""),
    payment_boleto_barcode: String((detail.barCode as string | undefined) ?? btgData.barCode ?? ""),
    payment_boleto_digitable_line: String((detail.digitableLine as string | undefined) ?? btgData.digitableLine ?? ""),
    payment_paid_at: null,
    payment_payer_name: null,
    payment_payer_tax_id: null,
    payment_generated_at: new Date().toISOString(),
    payment_last_webhook_at: null,
    payment_metadata: metadata,
    status_pagamento: "A PAGAR",
    updated_at: new Date().toISOString(),
  };

  const { data: updatedClient, error: updateError } = await admin
    .from("ir_clients")
    .update(updates as never)
    .eq("id", body.clientId)
    .select("*")
    .single();

  if (updateError || !updatedClient) {
    return jsonResponse({ error: "Cobranca criada no BTG, mas falhou ao salvar vinculo local.", detail: updateError?.message }, 500);
  }

  return jsonResponse({
    client: updatedClient,
    paymentLink: updates.payment_link || null,
    pixCopyPaste: updates.payment_pix_copy_paste || null,
    boletoPdfBase64: updates.payment_boleto_pdf_base64 || null,
    boletoBarcode: updates.payment_boleto_barcode || null,
    boletoDigitableLine: updates.payment_boleto_digitable_line || null,
  });
});
