import { cn } from "@/utils";

type ThemePreviewPanelProps = {
  primaryHex: string | null;
  secondaryHex: string | null;
  tertiaryHex: string | null;
  className?: string;
};

export function ThemePreviewPanel({
  primaryHex,
  secondaryHex,
  tertiaryHex,
  className,
}: ThemePreviewPanelProps) {
  const primary = primaryHex || "hsl(var(--primary))";
  const secondary = secondaryHex || "hsl(var(--accent))";
  const tertiary = tertiaryHex || "hsl(var(--chart-3))";

  return (
    <div
      className={cn(
        "rounded-xl border border-border bg-card p-4 space-y-4 shadow-sm",
        className
      )}
    >
      <p className="text-xs font-medium text-muted-foreground uppercase tracking-wider">
        Preview do tema
      </p>
      <div className="flex flex-wrap gap-3">
        <div
          className="rounded-lg border border-border p-3 shadow-sm min-w-[100px]"
          style={{ backgroundColor: "hsl(var(--card))" }}
        >
          <div
            className="h-8 rounded-md mb-2"
            style={{ backgroundColor: primary }}
          />
          <p className="text-[10px] font-medium truncate">Card</p>
        </div>
        <button
          type="button"
          className="rounded-lg px-3 py-2 text-xs font-medium text-white transition-opacity hover:opacity-90 min-w-[80px]"
          style={{ backgroundColor: primary }}
        >
          Botão
        </button>
        <span
          className="inline-flex items-center rounded-full px-2.5 py-0.5 text-[10px] font-medium text-white"
          style={{ backgroundColor: secondary }}
        >
          Badge
        </span>
        <div
          className="h-6 rounded w-16 border"
          style={{
            borderColor: primary,
            backgroundColor: `${primary}15`,
          }}
        />
      </div>
      <div className="flex gap-2 pt-2 border-t border-border">
        <div
          className="h-4 w-4 rounded shrink-0"
          style={{ backgroundColor: primary }}
          title="Primária"
        />
        <div
          className="h-4 w-4 rounded shrink-0"
          style={{ backgroundColor: secondary }}
          title="Secundária"
        />
        <div
          className="h-4 w-4 rounded shrink-0"
          style={{ backgroundColor: tertiary }}
          title="Terciária"
        />
      </div>
    </div>
  );
}
