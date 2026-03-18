import { useCallback, useState } from "react";
import { Label } from "@/components/ui/label";
import { Button } from "@/components/ui/button";
import { Loader2, Trash2, ImageIcon } from "lucide-react";
import { cn } from "@/utils";
import { validateLogoFile } from "@/services/brandingService";

type LogoUploadFieldProps = {
  currentUrl: string | null;
  onUpload: (file: File) => Promise<void>;
  onRemove: () => Promise<void>;
  disabled?: boolean;
  /** Se true, indica que é logo + ícone do site (mesmo arquivo). */
  unified?: boolean;
};

export function LogoUploadField({
  currentUrl,
  onUpload,
  onRemove,
  disabled,
  unified = true,
}: LogoUploadFieldProps) {
  const [dragOver, setDragOver] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleFile = useCallback(
    async (file: File | null) => {
      if (!file) return;
      setError(null);
      const valid = validateLogoFile(file);
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
    <div className="space-y-2">
      <Label className="text-xs font-medium">
        {unified ? "Logo e ícone do site" : "Logo do cliente"}
      </Label>
      <div
        onDrop={onDrop}
        onDragOver={onDragOver}
        onDragLeave={onDragLeave}
        className={cn(
          "relative rounded-lg border-2 border-dashed transition-colors min-h-[100px] flex flex-col items-center justify-center gap-2 p-3",
          dragOver && "border-primary-icon bg-primary/5",
          !dragOver && "border-border bg-muted/30 hover:bg-muted/50",
          disabled && "pointer-events-none opacity-60"
        )}
      >
        <input
          type="file"
          accept=".png,.svg,.jpg,.jpeg,.webp,.ico"
          onChange={onInputChange}
          disabled={disabled || loading}
          className="absolute inset-0 w-full h-full opacity-0 cursor-pointer z-0"
          tabIndex={-1}
          aria-hidden
        />
        {loading ? (
          <Loader2 className="h-6 w-6 animate-spin text-primary-icon relative z-10" />
        ) : currentUrl ? (
          <>
            <div className="flex gap-2 justify-center items-center relative z-10">
              <div className="rounded bg-card p-1.5 border border-border">
                <img src={currentUrl} alt="Logo (claro)" className="h-10 w-auto max-w-[100px] object-contain" />
              </div>
              <div className="rounded bg-muted p-1.5 border border-border">
                <img src={currentUrl} alt="Logo (escuro)" className="h-10 w-auto max-w-[100px] object-contain" />
              </div>
            </div>
            <p className="text-[10px] text-muted-foreground relative z-10">Clique para substituir</p>
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
              className="gap-1 relative z-10 pointer-events-auto"
            >
              <Trash2 className="h-3 w-3" />
              Remover
            </Button>
          </>
        ) : (
          <>
            <ImageIcon className="h-6 w-6 text-muted-foreground relative z-10" />
            <p className="text-xs text-muted-foreground text-center relative z-10">
              Arraste ou clique — PNG, SVG, JPG, WEBP, ICO (até 2 MB)
            </p>
          </>
        )}
      </div>
      {error && <p className="text-xs text-destructive">{error}</p>}
    </div>
  );
}
