import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "authorization, x-client-info, apikey, content-type, x-btg-signature",
};

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...corsHeaders, "Content-Type": "application/json" },
  });
}

function mapChargeStatus(eventName: string) {
  if (eventName === "collections.paid") return "paid";
  if (eventName === "collections.failed") return "failed";
  if (eventName === "collections.cancelled" || eventName === "collections.canceled") return "cancelled";
  return "pending";
}

Deno.serve(async (req) => {
  if (req.method === "OPTIONS") {
    return new Response("ok", { headers: corsHeaders });
  }

  if (req.method !== "POST") {
    return jsonResponse({ error: "Method not allowed" }, 405);
  }

  const expectedSecret = Deno.env.get("BTG_WEBHOOK_SECRET");
  const receivedSecret = req.headers.get("x-btg-signature");
  if (expectedSecret && receivedSecret !== expectedSecret) {
    return jsonResponse({ error: "Webhook não autorizado." }, 401);
  }

  let body: { event?: string; data?: Record<string, unknown> };
  try {
    body = await req.json();
  } catch {
    return jsonResponse({ error: "JSON inválido." }, 400);
  }

  const eventName = String(body.event ?? "");
  const data = (body.data ?? {}) as Record<string, unknown>;
  if (!eventName) {
    return jsonResponse({ error: "Evento inválido." }, 400);
  }

  const supabaseUrl = Deno.env.get("SUPABASE_URL") ?? "";
  const serviceRoleKey = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";
  if (!supabaseUrl || !serviceRoleKey) {
    return jsonResponse({ error: "Configuração do Supabase incompleta." }, 500);
  }

  const admin = createClient(supabaseUrl, serviceRoleKey, {
    auth: { autoRefreshToken: false, persistSession: false },
  });

  const collectionId = String(data.collectionId ?? "");
  const correlationId = String(data.correlationId ?? "");

  const query = admin
    .from("ir_clients")
    .select("*")
    .or(`payment_charge_id.eq.${collectionId},payment_charge_correlation_id.eq.${correlationId}`)
    .limit(1)
    .maybeSingle();

  const { data: client, error: clientError } = await query;
  if (clientError) {
    return jsonResponse({ error: clientError.message }, 500);
  }
  if (!client) {
    return jsonResponse({ ok: true, ignored: true });
  }

  const payer = (data.payer ?? {}) as Record<string, unknown>;
  const paidAt = String(data.paidAt ?? data.settledAt ?? "");
  const paymentMethod = String(data.paymentMethod ?? "");
  const chargeStatus = mapChargeStatus(eventName);

  const updates = {
    payment_charge_status: chargeStatus,
    payment_paid_at: paidAt || null,
    payment_payer_name: String(payer.name ?? ""),
    payment_payer_tax_id: String(payer.taxId ?? ""),
    payment_last_webhook_at: new Date().toISOString(),
    payment_metadata: {
      ...((client.payment_metadata as Record<string, unknown> | null) ?? {}),
      lastWebhook: body,
    },
    status_pagamento:
      chargeStatus === "paid"
        ? (client.payment_charge_type ?? (paymentMethod === "QRCODE" ? "PIX" : "BOLETO"))
        : "A PAGAR",
    vencimento:
      chargeStatus === "paid"
        ? (paidAt ? String(paidAt).slice(0, 10) : new Date().toISOString().slice(0, 10))
        : client.vencimento,
    updated_at: new Date().toISOString(),
  };

  const { error: updateError } = await admin
    .from("ir_clients")
    .update(updates as never)
    .eq("id", client.id);

  if (updateError) {
    return jsonResponse({ error: updateError.message }, 500);
  }

  return jsonResponse({ ok: true });
});
