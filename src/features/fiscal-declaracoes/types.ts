import type { Json } from "@/types/database";

export type DeclarationTab = "simples-nacional" | "mei";

export type DeclarationActionKind =
  | "simples_emitir_guia"
  | "simples_extrato"
  | "simples_defis"
  | "mei_declaracao_anual"
  | "mei_guias_mensais";

export type DeclarationActionMode = "emitir" | "recalcular";

export type DeclarationCompany = {
  id: string;
  name: string;
  document: string | null;
  active: boolean;
};

export type OverdueGuide = {
  id: string;
  companyId: string;
  companyName: string;
  companyDocument: string | null;
  competence: string;
  dueDate: string;
  status: "vencido";
  amountCents: number | null;
  referenceLabel: string | null;
};

export type DeclarationActionAvailability = {
  enabled: boolean;
  reason: string | null;
  robotTechnicalId: string | null;
};

export type DeclarationBootstrapData = {
  availableCompanies: DeclarationCompany[];
  overdueGuides: OverdueGuide[];
  actionAvailability: Record<DeclarationActionKind, DeclarationActionAvailability>;
};

export type DeclarationDocumentSourceStatus =
  | "ready"
  | "robot_missing"
  | "segment_missing"
  | "folder_missing";

export type DeclarationDocumentSource = {
  status: DeclarationDocumentSourceStatus;
  reason: string | null;
  robot_technical_id: string | null;
  robot_display_name?: string | null;
  action_key?: string | null;
  segment_path?: string | null;
  logical_folder_path?: string | null;
  date_rule?: string | null;
  competence?: string | null;
};

export type DeclarationArtifactListItem = {
  artifact_key: string;
  company_id: string;
  company_name: string;
  company_document: string | null;
  file_name: string;
  modified_at: string | null;
  size_bytes: number | null;
};

export type DeclarationArtifactListResponse = {
  action: DeclarationActionKind;
  title: string;
  source: DeclarationDocumentSource;
  items: DeclarationArtifactListItem[];
};

export type DeclarationRunItemStatus =
  | "pendente"
  | "processando"
  | "sucesso"
  | "erro";

export type DeclarationArtifact = {
  label: string;
  filePath?: string | null;
  url?: string | null;
  artifactKey?: string | null;
};

export type DeclarationRunItem = {
  companyId: string;
  companyName: string;
  companyDocument: string | null;
  status: DeclarationRunItemStatus;
  message: string;
  executionRequestId?: string | null;
  artifact?: DeclarationArtifact | null;
  meta?: Json;
};

export type DeclarationRunState = {
  runId: string;
  action: DeclarationActionKind;
  mode: DeclarationActionMode;
  title: string;
  requestIds: string[];
  items: DeclarationRunItem[];
  startedAt: string;
  finishedAt: string | null;
  terminal: boolean;
};

export type DeclarationRunHistoryPage = {
  items: DeclarationRunState[];
  total: number;
};

export type DeclarationGuideModalState = {
  open: boolean;
  action: DeclarationActionKind;
  source: "card" | "overdue-guide";
  presetCompanyId?: string | null;
  presetCompetence?: string | null;
  presetDueDate?: string | null;
  recalculateByDefault: boolean;
};

export type DeclarationStoredDocumentsModalState = {
  open: boolean;
  action: Extract<DeclarationActionKind, "simples_extrato" | "simples_defis">;
  presetCompanyId?: string | null;
  presetYear?: string | null;
};

export type DeclarationGuideSubmitInput = {
  companyIds: string[];
  competence?: string | null;
  recalculate: boolean;
  recalculateDueDate?: string | null;
};
