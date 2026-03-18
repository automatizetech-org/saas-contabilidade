import { supabase } from "./supabaseClient"
import { fetchAllPages } from "./supabasePagination"
import type { Tables } from "@/types/database"
import { formatCpf as formatCpfValue, isValidCpf, onlyDigits as digitsOnly } from "@/lib/brazilDocuments"

export type Accountant = Tables<"accountants">

export function onlyDigits(value: string) {
  return digitsOnly(value)
}

export function formatCpf(value: string) {
  return formatCpfValue(value)
}

export async function getAccountants(activeOnly = true): Promise<Accountant[]> {
  const list = await fetchAllPages<Accountant>((from, to) => {
    let query = supabase.from("accountants").select("*").order("name").range(from, to)
    if (activeOnly) query = query.eq("active", true)
    return query
  })
  return list.sort((a, b) => a.name.localeCompare(b.name, "pt-BR"))
}

export function findAccountantByCpf(accountants: Accountant[], cpf: string | null | undefined) {
  const digits = onlyDigits(cpf ?? "")
  if (!digits) return null
  return accountants.find((accountant) => onlyDigits(accountant.cpf) === digits) ?? null
}

export async function createAccountant(params: { name: string; cpf: string }): Promise<Accountant> {
  const cpfDigits = onlyDigits(params.cpf)
  if (!isValidCpf(cpfDigits)) throw new Error("Informe um CPF válido para o contador.")
  const { data, error } = await supabase
    .from("accountants")
    .insert({ name: params.name.trim(), cpf: cpfDigits })
    .select()
    .single()
  if (error) throw error
  return data as Accountant
}

export async function updateAccountant(
  id: string,
  updates: { name?: string; cpf?: string; active?: boolean }
): Promise<Accountant> {
  const payload: Record<string, unknown> = { updated_at: new Date().toISOString() }
  if (updates.name !== undefined) payload.name = updates.name.trim()
  if (updates.cpf !== undefined) {
    const digits = onlyDigits(updates.cpf)
    if (!isValidCpf(digits)) throw new Error("Informe um CPF válido para o contador.")
    payload.cpf = digits
  }
  if (updates.active !== undefined) payload.active = updates.active
  const { data, error } = await supabase.from("accountants").update(payload).eq("id", id).select().single()
  if (error) throw error
  return data as Accountant
}

export async function deleteAccountant(id: string): Promise<void> {
  const { error } = await supabase.from("accountants").delete().eq("id", id)
  if (error) throw error
}
