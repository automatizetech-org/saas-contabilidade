import { ArrowRight, Download, FileText, FolderSearch, Loader2 } from "lucide-react";
import { format } from "date-fns";
import { ptBR } from "date-fns/locale";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { Button } from "@/components/ui/button";
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

export function DeclarationArtifactsCard({
  action,
  title,
  description,
  loading,
  response,
  onDownload,
  busyArtifactKey,
  ctaLabel,
  onPrimaryAction,
  actionBusy,
  actionDisabled,
}: {
  action: DeclarationActionKind;
  title: string;
  description: string;
  loading: boolean;
  response?: DeclarationArtifactListResponse;
  onDownload: (item: DeclarationArtifactListItem) => void;
  busyArtifactKey?: string | null;
  ctaLabel?: string;
  onPrimaryAction?: () => void;
  actionBusy?: boolean;
  actionDisabled?: boolean;
}) {
  const items = response?.items ?? [];

  return (
    <GlassCard className="border border-border/70 p-5">
      <div className="space-y-4">
        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
          <div className="space-y-2">
            <h3 className="text-base font-semibold font-display tracking-tight">{title}</h3>
            <p className="text-sm text-muted-foreground">{description}</p>
          </div>
          {ctaLabel && onPrimaryAction ? (
            <Button
              type="button"
              onClick={onPrimaryAction}
              disabled={actionDisabled || actionBusy}
              className="shrink-0 gap-2"
            >
              {actionBusy ? "Processando..." : ctaLabel}
              <ArrowRight className="h-4 w-4" />
            </Button>
          ) : null}
        </div>

        {loading ? (
          <div className="flex items-center gap-2 rounded-2xl border border-dashed border-border bg-background/50 px-4 py-5 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" />
            Localizando documentos desta rotina no servidor...
          </div>
        ) : items.length === 0 ? (
          <div className="rounded-2xl border border-dashed border-border bg-background/50 px-4 py-5 text-sm text-muted-foreground">
            <div className="flex items-center gap-2">
              <FolderSearch className="h-4 w-4" />
              Nenhum documento localizado ate o momento.
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
