/**
 * Chave de painel para cada rota. Usado no sidebar (panel_access) e no guard de rota.
 * /admin nao tem panelKey, controle por super_admin ou owner do escritorio.
 */
export const PATH_TO_PANEL: Record<string, string> = {
  "/dashboard": "dashboard",
  "/fiscal": "fiscal",
  "/dp": "dp",
  "/contabil": "contabil",
  "/inteligencia-tributaria": "inteligencia_tributaria",
  "/ir": "ir",
  "/paralegal": "paralegal",
  "/financeiro": "financeiro",
  "/operacoes": "operacoes",
  "/documentos": "documentos",
  "/empresas": "empresas",
  "/alteracao-empresarial": "alteracao_empresarial",
  "/sync": "sync",
}

export const PANEL_KEYS = [
  "dashboard",
  "fiscal",
  "dp",
  "contabil",
  "inteligencia_tributaria",
  "ir",
  "paralegal",
  "financeiro",
  "operacoes",
  "documentos",
  "empresas",
  "alteracao_empresarial",
  "sync",
] as const

export type PanelKey = (typeof PANEL_KEYS)[number]

export const PANEL_LABELS: Record<PanelKey, string> = {
  dashboard: "Dashboard",
  fiscal: "Fiscal",
  dp: "Depto. Pessoal",
  contabil: "Contabil",
  inteligencia_tributaria: "Inteligencia Tributaria",
  ir: "IR",
  paralegal: "Paralegal",
  financeiro: "Financeiro",
  operacoes: "Operacoes",
  documentos: "Documentos",
  empresas: "Empresas",
  alteracao_empresarial: "Alteracao Empresarial",
  sync: "Sincronizacao",
}

export function pathToPanelKey(pathname: string): string | null {
  for (const [path, key] of Object.entries(PATH_TO_PANEL)) {
    if (pathname === path || pathname.startsWith(path + "/")) return key
  }
  return null
}
