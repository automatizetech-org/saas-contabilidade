import { supabase } from "./supabaseClient"
import type { Tables } from "@/types/database"

export type Accountant = Tables<"accountants">

export function onlyDigits(value: string) {
  return value.replace(/\D/g, "")
}

export function formatCpf(value: string) {
  const digits = onlyDigits(value)
  if (digits.length !== 11) return value
  return digits.replace(/(\d{3})(\d{3})(\d{3})(\d{2})/, "$1.$2.$3-$4")
}

export async function getAccountants(activeOnly = true): Promise<Accountant[]> {
  let query = supabase.from("accountants").select("*").order("name")
  if (activeOnly) query = query.eq("active", true)
  const { data, error } = await query
  if (error) throw error
  const list = (data ?? []) as Accountant[]
  return list.sort((a, b) => a.name.localeCompare(b.name, "pt-BR"))
}

export function findAccountantByCpf(accountants: Accountant[], cpf: string | null | undefined) {
  const digits = onlyDigits(cpf ?? "")
  if (!digits) return null
  return accountants.find((accountant) => onlyDigits(accountant.cpf) === digits) ?? null
}

export async function createAccountant(params: { name: string; cpf: string }): Promise<Accountant> {
  const cpfDigits = onlyDigits(params.cpf)
  if (cpfDigits.length !== 11) throw new Error("CPF deve ter 11 dígitos.")
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
    if (digits.length !== 11) throw new Error("CPF deve ter 11 dígitos.")
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
