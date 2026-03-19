/**
 * Estrutura de pastas centralizada - tipos e helpers para painel admin e robos.
 * Path na VM: BASE_PATH/{nome_empresa}/{segmentos}
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
export function nodeSegment(node: {
  slug?: string | null;
  name: string;
}): string {
  const segment = (node.slug || node.name || "").trim();
  if (segment) return segment;
  return (
    node.name
      .replace(/[<>:"/\\|?*]/g, "_")
      .replace(/\s+/g, " ")
      .trim() || "pasta"
  );
}

/**
 * Monta o path relativo (segmentos) da raiz ate o no, para uso em BASE/{empresa}/{segmentos}.
 * Retorna ex.: ["FISCAL", "NFS"] ou ["CONTABIL", "DRE"].
 */
export function pathSegmentsToNode(
  flatNodes: FolderStructureNodeRow[],
  nodeId: string,
): string[] {
  const byId = new Map(flatNodes.map((node) => [node.id, node]));
  const segments: string[] = [];
  let current: FolderStructureNodeRow | undefined = byId.get(nodeId);

  while (current) {
    segments.unshift(nodeSegment(current));
    current = current.parent_id ? byId.get(current.parent_id) : undefined;
  }

  return segments;
}

/**
 * Dado um caminho logico (ex.: "FISCAL/NFS"), encontra o no folha e retorna o date_rule dele.
 * pathLogical = segmentos separados por / (slug ou name).
 */
export function findDateRuleByPath(
  flatNodes: FolderStructureNodeRow[],
  pathLogical: string,
): DateRule {
  const parts = pathLogical
    .split("/")
    .map((part) => part.trim())
    .filter(Boolean);
  if (parts.length === 0) return null;

  const byParentAndSlug = new Map<string, FolderStructureNodeRow>();
  for (const node of flatNodes) {
    const key = `${node.parent_id ?? "root"}:${(node.slug || node.name).toLowerCase()}`;
    byParentAndSlug.set(key, node);
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

/** Monta segmentos de data conforme date_rule (year, year_month, year_month_day). */
export function dateSegments(rule: DateRule, date: Date): string[] {
  if (!rule) return [];

  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");

  if (rule === "year") return [`${year}`];
  if (rule === "year_month") return [`${year}`, month];
  if (rule === "year_month_day") return [`${year}`, month, day];
  return [];
}

/**
 * Path completo relativo para um robo: {companyName}/{segmentPath}/{dateSegments}.
 * companyName ja deve ser o nome da pasta da empresa (ex.: "Grupo Fleury").
 */
export function buildRelativePath(
  companyName: string,
  segmentPath: string[],
  dateRule: DateRule,
  date?: Date,
): string {
  const parts = [companyName, ...segmentPath];
  if (dateRule && date) {
    parts.push(...dateSegments(dateRule, date));
  }
  return parts.join("/");
}
