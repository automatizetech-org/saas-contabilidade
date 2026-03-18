import { supabase } from "./supabaseClient"
import type { FolderStructureNodeRow, FolderStructureNodeTree } from "@/types/folderStructure"
import type { TablesInsert, TablesUpdate } from "@/types/database"

export type FolderNodeInsert = TablesInsert<"folder_structure_nodes">
export type FolderNodeUpdate = TablesUpdate<"folder_structure_nodes">

/** Lista plana de nós ordenada por parent+position. */
export async function getFolderStructureFlat(): Promise<FolderStructureNodeRow[]> {
  const { data, error } = await supabase
    .from("folder_structure_nodes")
    .select("*")
    .order("parent_id", { nullsFirst: true })
    .order("position", { ascending: true })
  if (error) throw error
  return (data ?? []) as FolderStructureNodeRow[]
}

/** Constrói árvore a partir da lista plana. */
export function buildFolderTree(flat: FolderStructureNodeRow[]): FolderStructureNodeTree[] {
  const byParent = new Map<string | null, FolderStructureNodeRow[]>()
  byParent.set(null, [])
  for (const n of flat) {
    const key = n.parent_id ?? null
    if (!byParent.has(key)) byParent.set(key, [])
    byParent.get(key)!.push(n)
  }
  function toTree(nodes: FolderStructureNodeRow[]): FolderStructureNodeTree[] {
    return nodes.map((n) => ({
      ...n,
      children: toTree(byParent.get(n.id) ?? []),
    }))
  }
  return toTree(byParent.get(null) ?? [])
}

/** Busca árvore completa (flat + tree). */
export async function getFolderStructureTree(): Promise<FolderStructureNodeTree[]> {
  const flat = await getFolderStructureFlat()
  return buildFolderTree(flat)
}

export async function createFolderNode(
  payload: FolderNodeInsert
): Promise<FolderStructureNodeRow> {
  const { data, error } = await supabase
    .from("folder_structure_nodes")
    .insert(payload)
    .select()
    .single()
  if (error) throw error
  return data as FolderStructureNodeRow
}

export async function updateFolderNode(
  id: string,
  updates: FolderNodeUpdate
): Promise<FolderStructureNodeRow> {
  const { data, error } = await supabase
    .from("folder_structure_nodes")
    .update(updates)
    .eq("id", id)
    .select()
    .single()
  if (error) throw error
  return data as FolderStructureNodeRow
}

export async function deleteFolderNode(id: string): Promise<void> {
  const { error } = await supabase
    .from("folder_structure_nodes")
    .delete()
    .eq("id", id)
  if (error) throw error
}
