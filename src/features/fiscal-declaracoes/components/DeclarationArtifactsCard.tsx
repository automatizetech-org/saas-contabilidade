import { Download, FileText, FolderSearch, Loader2 } from "lucide-react";
import { format } from "date-fns";
import { ptBR } from "date-fns/locale";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/utils";
import type {
  DeclarationActionKind,
  DeclarationArtifactListItem,
  DeclarationArtifactListResponse,
} from "@/features/fiscal-declaracoes/types";

function formatBytes(value: number | null) {
  if (!value || value <= 0) return "Tamanho indisponivel";
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

function statusTone(status: DeclarationArtifactListResponse["source"]["status"]) {
  if (status === "ready") return "border-emerald-500/20 bg-emerald-500/5 text-emerald-100";
  if (status === "folder_missing" || status === "mapping_missing") {
    return "border-amber-500/20 bg-amber-500/5 text-amber-100";
  }
  return "border-border bg-background text-muted-foreground";
}

function statusLabel(status: DeclarationArtifactListResponse["source"]["status"]) {
  if (status === "ready") return "Origem pronta";
  if (status === "robot_missing") return "Robo nao encontrado";
  if (status === "segment_missing") return "Segmento nao configurado";
  if (status === "mapping_missing") return "Mapeamento pendente";
  if (status === "folder_missing") return "Pasta final ausente";
  return "Indisponivel";
}

export function DeclarationArtifactsCard({
  action,
  title,
  description,
  competence,
  loading,
  response,
  onDownload,
  busyArtifactKey,
}: {
  action: DeclarationActionKind;
  title: string;
  description: string;
  competence: string;
  loading: boolean;
  response?: DeclarationArtifactListResponse;
  onDownload: (item: DeclarationArtifactListItem) => void;
  busyArtifactKey?: string | null;
}) {
  const items = response?.items ?? [];
  const source = response?.source;

  return (
    <GlassCard className="border border-border/70 p-5">
      <div className="space-y-4">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div className="space-y-2">
            <div className="flex flex-wrap items-center gap-2">
              <h3 className="text-base font-semibold font-display tracking-tight">{title}</h3>
              {source ? (
                <Badge className={cn("border", statusTone(source.status))}>
                  {statusLabel(source.status)}
                </Badge>
              ) : null}
            </div>
            <p className="text-sm text-muted-foreground">{description}</p>
          </div>
          <div className="rounded-2xl border border-border bg-background/70 px-4 py-3 text-right">
            <p className="text-[11px] uppercase tracking-[0.12em] text-muted-foreground">Competencia</p>
            <p className="mt-1 text-sm font-semibold">
              {competence.slice(5, 7)}/{competence.slice(0, 4)}
            </p>
          </div>
        </div>

        {source ? (
          <div className="rounded-2xl border border-border bg-background/50 px-4 py-3 text-xs text-muted-foreground">
            <div className="flex flex-wrap gap-x-4 gap-y-1">
              <span>Robo: {source.robot_display_name || source.robot_technical_id || "Nao configurado"}</span>
              <span>Segmento: {source.segment_path || "Nao configurado"}</span>
              <span>Subpasta: {source.subfolder_path || "Nao configurada"}</span>
              <span>Regra de data: {source.date_rule || "Sem segmentacao"}</span>
            </div>
            {source.reason ? <p className="mt-2 text-amber-200">{source.reason}</p> : null}
          </div>
        ) : null}

        {loading ? (
          <div className="flex items-center gap-2 rounded-2xl border border-dashed border-border bg-background/50 px-4 py-5 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Localizando documentos desta rotina no servidor...
          </div>
        ) : source?.status !== "ready" ? (
          <div className="rounded-2xl border border-dashed border-border bg-background/50 px-4 py-5 text-sm text-muted-foreground">
            A listagem desta rotina fica disponivel assim que o robo tiver `segment_path`, mapeamento da subpasta final e a estrutura correspondente no escritorio.
          </div>
        ) : items.length === 0 ? (
          <div className="rounded-2xl border border-dashed border-border bg-background/50 px-4 py-5 text-sm text-muted-foreground">
            <div className="flex items-center gap-2">
              <FolderSearch className="h-4 w-4" />
              Nenhum documento localizado para esta competencia no disco do servidor.
            </div>
          </div>
        ) : (
          <div className="space-y-2">
            {items.map((item) => (
              <div
                key={`${action}-${item.company_id}-${item.artifact_key}`}
                className="flex flex-col gap-3 rounded-2xl border border-border bg-background/60 px-4 py-3 lg:flex-row lg:items-center lg:justify-between"
              >
                <div className="min-w-0">
                  <div className="flex flex-wrap items-center gap-2">
                    <FileText className="h-4 w-4 text-primary-icon" />
                    <p className="truncate text-sm font-medium">{item.file_name}</p>
                  </div>
                  <div className="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
                    <span>{item.company_name}</span>
                    <span>{item.company_document || "Documento nao informado"}</span>
                    <span>{formatBytes(item.size_bytes)}</span>
                    <span>
                      {item.modified_at
                        ? format(new Date(item.modified_at), "dd/MM/yyyy HH:mm", { locale: ptBR })
                        : "Data indisponivel"}
                    </span>
                  </div>
                </div>
                <Button
                  type="button"
                  variant="outline"
                  size="sm"
                  disabled={busyArtifactKey === item.artifact_key}
                  onClick={() => onDownload(item)}
                >
                  {busyArtifactKey === item.artifact_key ? (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  ) : (
                    <Download className="h-4 w-4" />
                  )}
                  Baixar
                </Button>
              </div>
            ))}
          </div>
        )}
      </div>
    </GlassCard>
  );
}
