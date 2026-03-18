import { cn } from "@/utils";

type ColorSwatchPreviewProps = {
  colors: (string | null)[];
  className?: string;
};

export function ColorSwatchPreview({ colors, className }: ColorSwatchPreviewProps) {
  const list = colors.filter(Boolean) as string[];
  return (
    <div className={cn("flex items-center gap-1.5", className)}>
      {list.map((hex, i) => (
        <div
          key={i}
          className="h-8 w-8 rounded-lg border border-border shadow-inner shrink-0"
          style={{ backgroundColor: hex }}
          title={hex}
        />
      ))}
    </div>
  );
}
