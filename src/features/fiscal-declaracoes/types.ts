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

export type DeclarationRunItemStatus =
  | "pendente"
  | "processando"
  | "sucesso"
  | "erro";

export type DeclarationArtifact = {
  label: string;
  filePath?: string | null;
  url?: string | null;
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

export type DeclarationGuideModalState = {
  open: boolean;
  source: "card" | "overdue-guide";
  presetCompanyId?: string | null;
  presetCompetence?: string | null;
  presetDueDate?: string | null;
  recalculateByDefault: boolean;
};

export type DeclarationGuideSubmitInput = {
  companyIds: string[];
  competence: string;
  recalculate: boolean;
  recalculateDueDate?: string | null;
};
