import { fetchAllPages } from "./supabasePagination";
import { supabase } from "./supabaseClient";
import type { Tables } from "@/types/database";

export type EcacMailboxStatusFilter = "all" | "unread" | "read" | "archived";

type EcacMailboxRow = Tables<"ecac_mailbox_messages">;
type CompanyLookup = { id: string; name: string; document?: string | null };

export type EcacMailboxMessage = {
  id: string;
  companyId: string;
  companyName: string;
  companyDocument: string | null;
  robotTechnicalId: string | null;
  externalMessageId: string;
  subject: string;
  senderName: string | null;
  senderDocument: string | null;
  category: string | null;
  status: EcacMailboxRow["status"];
  receivedAt: string;
  fetchedAt: string;
  isRead: boolean;
  readAt: string | null;
  createdAt: string;
  updatedAt: string;
  payload: EcacMailboxRow["payload"];
};

export type EcacMailboxSummary = {
  totalMessages: number;
  unreadMessages: number;
  messagesToday: number;
  lastReceivedAt: string | null;
};

function normalizeCompanyIds(companyIds: string[] | null) {
  if (!companyIds?.length) return [];
  return [...new Set(companyIds.map((id) => String(id || "").trim()).filter(Boolean))];
}

async function fetchCompaniesByIds(companyIds: string[]): Promise<CompanyLookup[]> {
  const normalizedIds = normalizeCompanyIds(companyIds);
  if (normalizedIds.length === 0) return [];

  return fetchAllPages<CompanyLookup>((from, to) =>
    supabase
      .from("companies")
      .select("id, name, document")
      .in("id", normalizedIds)
      .order("name")
      .range(from, to)
  );
}

function applyCompanyFilter<T extends { company_id: string }>(
  query: ReturnType<typeof supabase.from<"ecac_mailbox_messages", EcacMailboxRow>>,
  companyIds: string[] | null,
) {
  const normalizedIds = normalizeCompanyIds(companyIds);
  if (normalizedIds.length > 0) {
    return query.in("company_id", normalizedIds);
  }

  return query;
}

function startOfTodayIso() {
  const now = new Date();
  const start = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  return start.toISOString();
}

export function getEcacMailboxSummaryQueryKey(companyIds: string[] | null) {
  return ["ecac-mailbox-summary", normalizeCompanyIds(companyIds)];
}

export function getEcacMailboxMessagesQueryKey(
  companyIds: string[] | null,
  search: string,
  status: EcacMailboxStatusFilter,
) {
  return ["ecac-mailbox-messages", normalizeCompanyIds(companyIds), search.trim().toLowerCase(), status];
}

export async function getEcacMailboxSummary(companyIds: string[] | null): Promise<EcacMailboxSummary> {
  const todayIso = startOfTodayIso();
  const baseQuery = () => applyCompanyFilter(supabase.from("ecac_mailbox_messages"), companyIds);

  const [
    totalResponse,
    unreadResponse,
    todayResponse,
    latestResponse,
  ] = await Promise.all([
    baseQuery().select("id", { count: "exact", head: true }),
    baseQuery().select("id", { count: "exact", head: true }).eq("is_read", false),
    baseQuery().select("id", { count: "exact", head: true }).gte("received_at", todayIso),
    baseQuery().select("received_at").order("received_at", { ascending: false }).limit(1).maybeSingle(),
  ]);

  if (totalResponse.error) throw totalResponse.error;
  if (unreadResponse.error) throw unreadResponse.error;
  if (todayResponse.error) throw todayResponse.error;
  if (latestResponse.error) throw latestResponse.error;

  return {
    totalMessages: totalResponse.count ?? 0,
    unreadMessages: unreadResponse.count ?? 0,
    messagesToday: todayResponse.count ?? 0,
    lastReceivedAt: latestResponse.data?.received_at ?? null,
  };
}

export async function getEcacMailboxMessages(filters: {
  companyIds: string[] | null;
  search?: string;
  status?: EcacMailboxStatusFilter;
  limit?: number;
}): Promise<EcacMailboxMessage[]> {
  const search = String(filters.search ?? "").trim().toLowerCase();
  const limit = Math.min(Math.max(filters.limit ?? 100, 1), 500);
  let query = applyCompanyFilter(
    supabase
      .from("ecac_mailbox_messages")
      .select("*")
      .order("received_at", { ascending: false })
      .limit(limit),
    filters.companyIds,
  );

  if (filters.status === "unread") {
    query = query.eq("is_read", false);
  } else if (filters.status === "read") {
    query = query.eq("is_read", true).neq("status", "arquivado");
  } else if (filters.status === "archived") {
    query = query.eq("status", "arquivado");
  }

  const { data, error } = await query;
  if (error) throw error;

  const rows = (data ?? []) as EcacMailboxRow[];
  const companies = await fetchCompaniesByIds(rows.map((row) => row.company_id));
  const companyMap = new Map(companies.map((company) => [company.id, company]));

  const mapped = rows.map<EcacMailboxMessage>((row) => {
    const company = companyMap.get(row.company_id);
    return {
      id: row.id,
      companyId: row.company_id,
      companyName: company?.name ?? "Empresa",
      companyDocument: company?.document ?? null,
      robotTechnicalId: row.robot_technical_id,
      externalMessageId: row.external_message_id,
      subject: row.subject,
      senderName: row.sender_name,
      senderDocument: row.sender_document,
      category: row.category,
      status: row.status,
      receivedAt: row.received_at,
      fetchedAt: row.fetched_at,
      isRead: row.is_read,
      readAt: row.read_at,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      payload: row.payload,
    };
  });

  if (!search) {
    return mapped;
  }

  return mapped.filter((message) => {
    const haystack = [
      message.companyName,
      message.companyDocument,
      message.subject,
      message.senderName,
      message.senderDocument,
      message.category,
      message.externalMessageId,
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();

    return haystack.includes(search);
  });
}

export async function markEcacMailboxMessagesAsRead(ids: string[]) {
  const normalizedIds = [...new Set(ids.map((id) => String(id || "").trim()).filter(Boolean))];
  if (normalizedIds.length === 0) return;

  const timestamp = new Date().toISOString();
  const { error } = await supabase
    .from("ecac_mailbox_messages")
    .update({
      is_read: true,
      read_at: timestamp,
      status: "lido",
    })
    .in("id", normalizedIds)
    .eq("is_read", false);

  if (error) throw error;
}

export async function archiveEcacMailboxMessage(id: string) {
  const normalizedId = String(id || "").trim();
  if (!normalizedId) return;

  const { error } = await supabase
    .from("ecac_mailbox_messages")
    .update({
      status: "arquivado",
      is_read: true,
      read_at: new Date().toISOString(),
    })
    .eq("id", normalizedId);

  if (error) throw error;
}
