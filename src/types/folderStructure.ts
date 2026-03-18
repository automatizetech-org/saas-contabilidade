/**
 * Estrutura de pastas centralizada — tipos e helpers para painel admin e robôs.
 * Path na VM: BASE_PATH/EMPRESAS/{nome_empresa}/{segmentos}
 */

export type DateRule = "year" | "year_month" | "year_month_day" | null;

export interface FolderStructureNodeRow {
  id: string;
  parent_id: string | null;
  name: string;
  slug: string | null;
  date_rule: DateRule;
  position: number;
  created_at: string;
  updated_at: string;
}

export interface FolderStructureNodeTree extends FolderStructureNodeRow {
  children: FolderStructureNodeTree[];
}

/** Segmento de path para montar caminho no disco (slug ou name sanitizado). */
export function nodeSegment(node: { slug?: string | null; name: string }): string {
  const s = (node.slug || node.name || "").trim();
  if (s) return s;
  return node.name.replace(/[<>:"/\\|?*]/g, "_").replace(/\s+/g, " ").trim() || "pasta";
}

/**
 * Monta o path relativo (segmentos) da raiz até o nó, para uso em BASE/EMPRESAS/{empresa}/{segmentos}.
 * Retorna ex.: ["FISCAL", "NFS"] ou ["CONTABIL", "DRE"].
 */
export function pathSegmentsToNode(
  flatNodes: FolderStructureNodeRow[],
  nodeId: string
): string[] {
  const byId = new Map(flatNodes.map((n) => [n.id, n]));
  const segments: string[] = [];
  let current: FolderStructureNodeRow | undefined = byId.get(nodeId);
  while (current) {
    segments.unshift(nodeSegment(current));
    current = current.parent_id ? byId.get(current.parent_id) : undefined;
  }
  return segments;
}

/**
 * Dado um caminho lógico (ex.: "FISCAL/NFS"), encontra o nó folha e retorna o date_rule dele.
 * pathLogical = segmentos separados por / (slug ou name).
 */
export function findDateRuleByPath(
  flatNodes: FolderStructureNodeRow[],
  pathLogical: string
): DateRule {
  const parts = pathLogical.split("/").map((p) => p.trim()).filter(Boolean);
  if (parts.length === 0) return null;
  const byParentAndSlug = new Map<string, FolderStructureNodeRow>();
  for (const n of flatNodes) {
    const key = `${n.parent_id ?? "root"}:${(n.slug || n.name).toLowerCase()}`;
    byParentAndSlug.set(key, n);
  }
  let parentId: string | null = null;
  let node: FolderStructureNodeRow | undefined;
  for (const part of parts) {
    const key = `${parentId ?? "root"}:${part.toLowerCase()}`;
    node = byParentAndSlug.get(key);
    if (!node) return null;
    parentId = node.id;
  }
  return node?.date_rule ?? null;
}

/**
 * Monta segmentos de data conforme date_rule (year, year_month, year_month_day).
 */
export function dateSegments(rule: DateRule, d: Date): string[] {
  if (!rule) return [];
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  if (rule === "year") return [`${y}`];
  if (rule === "year_month") return [`${y}`, m];
  if (rule === "year_month_day") return [`${y}`, m, day];
  return [];
}

/**
 * Path completo relativo para um robô: EMPRESAS/{companyName}/{segmentPath}/{dateSegments}.
 * companyName já deve ser o nome da pasta da empresa (ex.: "Grupo Fleury").
 */
export function buildRelativePath(
  companyName: string,
  segmentPath: string[],
  dateRule: DateRule,
  date?: Date
): string {
  const parts = ["EMPRESAS", companyName, ...segmentPath];
  if (dateRule && date) {
    parts.push(...dateSegments(dateRule, date));
  }
  return parts.join("/");
}
