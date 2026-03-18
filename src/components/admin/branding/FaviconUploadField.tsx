import { useCallback, useState } from "react";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Loader2, Trash2, ImageIcon } from "lucide-react";
import { cn } from "@/utils";
import { validateFaviconFile } from "@/services/brandingService";

type FaviconUploadFieldProps = {
  currentUrl: string | null;
  onUpload: (file: File) => Promise<void>;
  onRemove: () => Promise<void>;
  disabled?: boolean;
};

export function FaviconUploadField({
  currentUrl,
  onUpload,
  onRemove,
  disabled,
}: FaviconUploadFieldProps) {
  const [dragOver, setDragOver] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleFile = useCallback(
    async (file: File | null) => {
      if (!file) return;
      setError(null);
      const valid = validateFaviconFile(file);
      if (!valid.ok) {
        setError(valid.error);
        return;
      }
      setLoading(true);
      try {
        await onUpload(file);
      } catch (e) {
        setError(e instanceof Error ? e.message : "Falha no upload.");
      } finally {
        setLoading(false);
      }
    },
    [onUpload]
  );

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      setDragOver(false);
      const file = e.dataTransfer.files?.[0];
      if (file) handleFile(file);
    },
    [handleFile]
  );

  const onDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(true);
  }, []);

  const onDragLeave = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    setDragOver(false);
  }, []);

  const onInputChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const file = e.target.files?.[0];
      if (file) handleFile(file);
      e.target.value = "";
    },
    [handleFile]
  );

  return (
    <div className="space-y-3">
      <Label className="text-sm font-medium">Ícone do site (favicon)</Label>
      <div
        onDrop={onDrop}
        onDragOver={onDragOver}
        onDragLeave={onDragLeave}
        className={cn(
          "relative rounded-xl border-2 border-dashed transition-colors min-h-[120px] flex flex-col items-center justify-center gap-3 p-4",
          dragOver && "border-primary-icon bg-primary/5",
          !dragOver && "border-border bg-muted/30 hover:bg-muted/50",
          disabled && "pointer-events-none opacity-60"
        )}
      >
        <input
          type="file"
          accept=".png,.ico,.svg,.jpg,.jpeg,.webp"
          onChange={onInputChange}
          disabled={disabled || loading}
          className="absolute inset-0 w-full h-full opacity-0 cursor-pointer"
        />
        {loading ? (
          <Loader2 className="h-8 w-8 animate-spin text-primary-icon" />
        ) : currentUrl ? (
          <>
            <div className="flex items-center gap-4">
              <div className="rounded-lg bg-card p-2 border border-border shadow-sm">
                <img src={currentUrl} alt="Favicon (claro)" className="h-10 w-10 object-contain" />
              </div>
              <div className="rounded-lg bg-muted p-2 border border-border shadow-sm">
                <img src={currentUrl} alt="Favicon (escuro)" className="h-10 w-10 object-contain" />
              </div>
            </div>
            <p className="text-xs text-muted-foreground">Clique ou arraste para substituir</p>
            <Button
              type="button"
              variant="outline"
              size="sm"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                onRemove();
              }}
              disabled={disabled || loading}
              className="gap-1.5"
            >
              <Trash2 className="h-3.5 w-3.5" />
              Remover favicon
            </Button>
          </>
        ) : (
          <>
            <ImageIcon className="h-8 w-8 text-muted-foreground" />
            <p className="text-sm text-muted-foreground text-center">
              Arraste um ícone ou clique para selecionar
            </p>
            <p className="text-xs text-muted-foreground">PNG, ICO, SVG, JPG ou WEBP — até 512 KB</p>
          </>
        )}
      </div>
      {error && <p className="text-sm text-destructive">{error}</p>}
    </div>
  );
}
