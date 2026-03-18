import { ReactNode } from "react";
import { cn } from "@/utils";

type BrandingSettingsCardProps = {
  title: string;
  description?: string;
  children: ReactNode;
  className?: string;
};

export function BrandingSettingsCard({
  title,
  description,
  children,
  className,
}: BrandingSettingsCardProps) {
  return (
    <div
      className={cn(
        "rounded-xl border border-border bg-card/80 backdrop-blur-sm overflow-hidden shadow-sm",
        "ring-1 ring-black/5 dark:ring-white/5",
        className
      )}
    >
      <div className="px-4 py-3 border-b border-border bg-muted/20">
        <h3 className="text-base font-semibold font-display tracking-tight">{title}</h3>
        {description && (
          <p className="text-xs text-muted-foreground mt-0.5">{description}</p>
        )}
      </div>
      <div className="p-4">{children}</div>
    </div>
  );
}
