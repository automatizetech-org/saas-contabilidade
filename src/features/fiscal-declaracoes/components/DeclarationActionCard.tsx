import type { ReactNode } from "react";
import { ArrowRight, ShieldAlert } from "lucide-react";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn } from "@/utils";

type DeclarationActionCardProps = {
  title: string;
  description: string;
  eyebrow: string;
  icon: ReactNode;
  ctaLabel: string;
  secondaryCtaLabel?: string;
  disabled?: boolean;
  secondaryDisabled?: boolean;
  disabledReason?: string | null;
  busy?: boolean;
  toneClassName?: string;
  onClick?: () => void;
  onSecondaryClick?: () => void;
};

export function DeclarationActionCard({
  title,
  description,
  eyebrow,
  icon,
  ctaLabel,
  secondaryCtaLabel,
  disabled = false,
  secondaryDisabled = false,
  disabledReason,
  busy = false,
  toneClassName,
  onClick,
  onSecondaryClick,
}: DeclarationActionCardProps) {
  return (
    <GlassCard
      className={cn(
        "relative overflow-hidden border border-border/70 p-6",
        "bg-gradient-to-br from-background via-background to-muted/20",
        toneClassName,
      )}
    >
      <div className="absolute right-0 top-0 h-24 w-24 rounded-full bg-primary/8 blur-2xl" />
      <div className="relative space-y-5">
        <div className="flex items-start justify-between gap-3">
          <div className="space-y-2">
            <Badge variant="secondary" className="rounded-full px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.12em]">
              {eyebrow}
            </Badge>
            <div>
              <h3 className="text-lg font-semibold font-display tracking-tight">{title}</h3>
              <p className="mt-1 text-sm text-muted-foreground">{description}</p>
            </div>
          </div>
          <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-2xl bg-primary/10 text-primary-icon shadow-sm">
            {icon}
          </div>
        </div>

        <div className="flex min-h-[44px] items-end justify-between gap-3">
          <div className="min-h-[20px] text-xs text-muted-foreground">
            {disabledReason ? (
              <span className="inline-flex items-center gap-1.5 text-warning">
                <ShieldAlert className="h-3.5 w-3.5" />
                {disabledReason}
              </span>
            ) : (
              "Disponível para as empresas visíveis no contexto atual."
            )}
          </div>
          <div className="flex shrink-0 gap-2">
            {secondaryCtaLabel ? (
              <Button
                type="button"
                variant="outline"
                onClick={onSecondaryClick}
                disabled={secondaryDisabled || busy}
                className="gap-2"
              >
                {secondaryCtaLabel}
              </Button>
            ) : null}
            <Button
              type="button"
              onClick={onClick}
              disabled={disabled || busy}
              className="gap-2"
            >
              {busy ? "Processando..." : ctaLabel}
              <ArrowRight className="h-4 w-4" />
            </Button>
          </div>
        </div>
      </div>
    </GlassCard>
  );
}
