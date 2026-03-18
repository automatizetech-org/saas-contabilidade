import { supabase } from "./supabaseClient";
import { fetchAllPages } from "./supabasePagination";
import type { Tables } from "@/types/database";
import { isValidCpfOrCnpj, onlyDigits } from "@/lib/brazilDocuments";

export type IrChargeType = "PIX" | "BOLETO" | "BOLETO_HIBRIDO";
export type IrChargeStatus = "none" | "pending" | "paid" | "failed" | "cancelled";
export type IrBaseClient = Tables<"ir_clients">;
export type IrClient = IrBaseClient & {
  status_pagamento: IrPaymentStatus;
  payment_charge_type: IrChargeType | null;
  payment_charge_status: IrChargeStatus;
  payment_charge_id: string | null;
  payment_charge_correlation_id: string | null;
  payment_provider: string | null;
  payment_link: string | null;
  payment_pix_copy_paste: string | null;
  payment_pix_qr_code: string | null;
  payment_boleto_pdf_base64: string | null;
  payment_boleto_barcode: string | null;
  payment_boleto_digitable_line: string | null;
  payment_paid_at: string | null;
  payment_payer_name: string | null;
  payment_payer_tax_id: string | null;
  payment_generated_at: string | null;
  payment_last_webhook_at: string | null;
  payment_metadata: Record<string, unknown> | null;
};
export type IrSettings = Tables<"ir_settings">;
export type IrPaymentStatus = "PIX" | "BOLETO" | "DINHEIRO" | "TRANSFERÊNCIA POUPANÇA" | "PERMUTA" | "A PAGAR";
export type IrDeclarationStatus = "Concluido" | "Pendente";

export type SaveIrClientInput = {
  nome: string;
  cpf_cnpj: string;
  responsavel_ir?: string | null;
  vencimento?: string | null;
  valor_servico: number;
  status_pagamento?: IrPaymentStatus;
  status_declaracao?: IrDeclarationStatus;
  observacoes?: string | null;
};

export type GenerateIrChargeInput = {
  clientId: string;
  chargeType: IrChargeType;
};

export type GenerateIrChargeResult = {
  client: IrClient;
  paymentLink: string | null;
  pixCopyPaste: string | null;
  boletoPdfBase64: string | null;
  boletoBarcode: string | null;
  boletoDigitableLine: string | null;
};

export type IrOverviewSummary = {
  cards: {
    clientesIr: number
    recebidos: number
    aPagar: number
    concluidoPercent: number
    concluidoTotal: number
    clientesTotal: number
    valorTotal: number
  }
  progressData: Array<{ name: string; value: number }>
  paymentValueData: Array<{ name: string; value: number }>
  paidValuePercent: number
}

function normalizeIrPaymentStatus(status: string | null | undefined): IrPaymentStatus {
  if (status === "Pendente" || status === "A Pagar") return "A PAGAR";
  if (status === "Pago") return "PIX";
  if (status === "PIX" || status === "BOLETO" || status === "DINHEIRO" || status === "PERMUTA" || status === "A PAGAR") {
    return status;
  }
  if (status === "TRANSFERÊNCIA POUPANÇA" || status === "TRANSFERÃŠNCIA POUPANÃ‡A") return "TRANSFERÊNCIA POUPANÇA";
  if (status === "Dinheiro") return "DINHEIRO";
  if (status === "TransferÃªncia PoupanÃ§a" || status === "Transferência Poupança") return "TRANSFERÊNCIA POUPANÇA";
  if (status === "Permuta") return "PERMUTA";
  return "A PAGAR";
}

function normalizeIrChargeStatus(status: string | null | undefined): IrChargeStatus {
  if (status === "pending" || status === "paid" || status === "failed" || status === "cancelled" || status === "none") {
    return status;
  }
  return "none";
}

function normalizeIrClient(client: Partial<IrClient>): IrClient {
  return {
    ...(client as IrBaseClient),
    payment_charge_type:
      client.payment_charge_type === "PIX" ||
      client.payment_charge_type === "BOLETO" ||
      client.payment_charge_type === "BOLETO_HIBRIDO"
        ? client.payment_charge_type
        : null,
    payment_charge_status: normalizeIrChargeStatus(client.payment_charge_status),
    payment_charge_id: client.payment_charge_id ?? null,
    payment_charge_correlation_id: client.payment_charge_correlation_id ?? null,
    payment_provider: client.payment_provider ?? null,
    payment_link: client.payment_link ?? null,
    payment_pix_copy_paste: client.payment_pix_copy_paste ?? null,
    payment_pix_qr_code: client.payment_pix_qr_code ?? null,
    payment_boleto_pdf_base64: client.payment_boleto_pdf_base64 ?? null,
    payment_boleto_barcode: client.payment_boleto_barcode ?? null,
    payment_boleto_digitable_line: client.payment_boleto_digitable_line ?? null,
    payment_paid_at: client.payment_paid_at ?? null,
    payment_payer_name: client.payment_payer_name ?? null,
    payment_payer_tax_id: client.payment_payer_tax_id ?? null,
    payment_generated_at: client.payment_generated_at ?? null,
    payment_last_webhook_at: client.payment_last_webhook_at ?? null,
    payment_metadata: (client.payment_metadata as Record<string, unknown> | null | undefined) ?? null,
    status_pagamento: normalizeIrPaymentStatus(client.status_pagamento),
  } as IrClient;
}

export async function getIrClients(): Promise<IrClient[]> {
  const data = await fetchAllPages<IrClient>((from, to) =>
    supabase
      .from("ir_clients")
      .select("*")
      .order("nome", { ascending: true })
      .range(from, to),
  );
  return data.map((client) => normalizeIrClient(client as Partial<IrClient>));
}

export async function getIrOverviewSummary(responsavelFilter: string | null): Promise<IrOverviewSummary> {
  try {
    const { data, error } = await supabase.rpc("get_ir_overview_summary", {
      responsavel_filter: responsavelFilter,
    })
    if (error) throw error

    const payload = (data ?? {}) as IrOverviewSummary
    return {
      cards: {
        clientesIr: Number(payload.cards?.clientesIr ?? 0),
        recebidos: Number(payload.cards?.recebidos ?? 0),
        aPagar: Number(payload.cards?.aPagar ?? 0),
        concluidoPercent: Number(payload.cards?.concluidoPercent ?? 0),
        concluidoTotal: Number(payload.cards?.concluidoTotal ?? 0),
        clientesTotal: Number(payload.cards?.clientesTotal ?? 0),
        valorTotal: Number(payload.cards?.valorTotal ?? 0),
      },
      progressData: (payload.progressData ?? []).map((item) => ({ name: item.name, value: Number(item.value ?? 0) })),
      paymentValueData: (payload.paymentValueData ?? []).map((item) => ({ name: item.name, value: Number(item.value ?? 0) })),
      paidValuePercent: Number(payload.paidValuePercent ?? 0),
    }
  } catch {
    const clients = await getIrClients()
    const filteredClients = responsavelFilter
      ? clients.filter((client) => (client.responsavel_ir?.trim() || "") === responsavelFilter)
      : clients
    const paidCount = filteredClients.filter((client) => client.status_pagamento !== "A PAGAR").length
    const pendingCount = filteredClients.filter((client) => client.status_pagamento === "A PAGAR").length
    const concludedCount = filteredClients.filter((client) => client.status_declaracao === "Concluido").length
    const pendingExecutionCount = filteredClients.length - concludedCount
    const paidValue = filteredClients
      .filter((client) => client.status_pagamento !== "A PAGAR")
      .reduce((sum, client) => sum + Number(client.valor_servico || 0), 0)
    const pendingValue = filteredClients
      .filter((client) => client.status_pagamento === "A PAGAR")
      .reduce((sum, client) => sum + Number(client.valor_servico || 0), 0)
    const totalValue = filteredClients.reduce((sum, client) => sum + Number(client.valor_servico || 0), 0)

    return {
      cards: {
        clientesIr: filteredClients.length,
        recebidos: paidCount,
        aPagar: pendingCount,
        concluidoPercent: filteredClients.length ? Math.round((concludedCount / filteredClients.length) * 100) : 0,
        concluidoTotal: concludedCount,
        clientesTotal: filteredClients.length,
        valorTotal: totalValue,
      },
      progressData: [
        { name: "Concluídos", value: concludedCount },
        { name: "Pendentes", value: pendingExecutionCount },
      ],
      paymentValueData: [
        { name: "Recebido", value: paidValue },
        { name: "A PAGAR", value: pendingValue },
      ],
      paidValuePercent: totalValue ? Math.round((paidValue / totalValue) * 100) : 0,
    }
  }
}

export async function createIrClient(input: SaveIrClientInput): Promise<IrClient> {
  const document = onlyDigits(input.cpf_cnpj);
  if (!isValidCpfOrCnpj(document)) {
    throw new Error("Informe um CPF ou CNPJ válido para o cliente de IR.");
  }
  const { data, error } = await supabase
    .from("ir_clients")
    .insert({
      nome: input.nome.trim(),
      cpf_cnpj: document,
      responsavel_ir: input.responsavel_ir?.trim() || null,
      vencimento: input.vencimento || null,
      valor_servico: input.valor_servico,
      status_pagamento: input.status_pagamento ?? (input.vencimento ? "PIX" : "A PAGAR"),
      status_declaracao: input.status_declaracao ?? "Pendente",
      observacoes: input.observacoes?.trim() || null,
    } as never)
    .select("*")
    .single();
  if (error) throw error;
  return normalizeIrClient(data as Partial<IrClient>);
}

export async function updateIrClient(
  id: string,
  updates: Partial<Pick<IrClient, "status_pagamento" | "status_declaracao" | "observacoes" | "valor_servico" | "nome" | "cpf_cnpj" | "responsavel_ir" | "vencimento" | "payment_charge_type" | "payment_charge_status" | "payment_charge_id" | "payment_charge_correlation_id" | "payment_provider" | "payment_link" | "payment_pix_copy_paste" | "payment_pix_qr_code" | "payment_boleto_pdf_base64" | "payment_boleto_barcode" | "payment_boleto_digitable_line" | "payment_paid_at" | "payment_payer_name" | "payment_payer_tax_id" | "payment_generated_at" | "payment_last_webhook_at" | "payment_metadata">>,
): Promise<IrClient> {
  const payload = {
    ...updates,
    responsavel_ir:
      updates.responsavel_ir === undefined ? undefined : updates.responsavel_ir?.trim() || null,
    vencimento: updates.vencimento === undefined ? undefined : updates.vencimento || null,
    observacoes:
      updates.observacoes === undefined ? undefined : updates.observacoes?.trim() || null,
    updated_at: new Date().toISOString(),
  } as Record<string, unknown>;

  if (updates.cpf_cnpj !== undefined) {
    const document = onlyDigits(updates.cpf_cnpj);
    if (!isValidCpfOrCnpj(document)) {
      throw new Error("Informe um CPF ou CNPJ válido para o cliente de IR.");
    }
    payload.cpf_cnpj = document;
  }

  const { data, error } = await supabase
    .from("ir_clients")
    .update(payload as never)
    .eq("id", id)
    .select("*")
    .single();
  if (error) throw error;
  return normalizeIrClient(data as Partial<IrClient>);
}

export async function deleteIrClient(id: string): Promise<void> {
  const { error } = await supabase
    .from("ir_clients")
    .delete()
    .eq("id", id);
  if (error) throw error;
}

export async function getIrSettings(): Promise<IrSettings | null> {
  const { data, error } = await supabase
    .from("ir_settings")
    .select("*")
    .eq("singleton", true)
    .maybeSingle();
  if (error) throw error;
  return data ?? null;
}

export async function upsertIrSettings(paymentDueDate: string | null): Promise<IrSettings> {
  const { data, error } = await supabase
    .from("ir_settings")
    .upsert(
      {
        singleton: true,
        payment_due_date: paymentDueDate || null,
        updated_at: new Date().toISOString(),
      } as never,
      { onConflict: "singleton" },
    )
    .select("*")
    .single();
  if (error) throw error;
  return data;
}

export async function generateIrCharge(input: GenerateIrChargeInput): Promise<GenerateIrChargeResult> {
  const { data, error } = await supabase.functions.invoke("btg-create-ir-charge", {
    body: input,
  });

  if (error) throw error;

  return {
    client: normalizeIrClient(data.client as Partial<IrClient>),
    paymentLink: data.paymentLink ?? null,
    pixCopyPaste: data.pixCopyPaste ?? null,
    boletoPdfBase64: data.boletoPdfBase64 ?? null,
    boletoBarcode: data.boletoBarcode ?? null,
    boletoDigitableLine: data.boletoDigitableLine ?? null,
  };
}

export function downloadBoletoPdf(fileName: string, boletoPdfBase64: string) {
  const cleanBase64 = boletoPdfBase64.includes(",") ? boletoPdfBase64.split(",").pop() ?? "" : boletoPdfBase64;
  const binary = atob(cleanBase64);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  const blob = new Blob([bytes], { type: "application/pdf" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = fileName;
  anchor.click();
  URL.revokeObjectURL(url);
}
