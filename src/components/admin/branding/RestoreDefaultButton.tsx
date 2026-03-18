import { Button } from "@/components/ui/button";
import { RotateCcw } from "lucide-react";

type RestoreDefaultButtonProps = {
  onRestore: () => void;
  disabled?: boolean;
  label?: string;
};

export function RestoreDefaultButton({
  onRestore,
  disabled,
  label = "Restaurar padrão",
}: RestoreDefaultButtonProps) {
  return (
    <Button
      type="button"
      variant="outline"
      size="sm"
      onClick={onRestore}
      disabled={disabled}
      className="gap-1.5 text-muted-foreground hover:text-foreground"
    >
      <RotateCcw className="h-3.5 w-3.5" />
      {label}
    </Button>
  );
}
