import { cn } from "@/utils";
import { LucideIcon } from "lucide-react";
import { AnimatedNumber } from "@/components/dashboard/AnimatedNumber";

interface StatsCardProps {
  title: string;
  value: string | number;
  change?: string;
  changeType?: "positive" | "negative" | "neutral";
  icon: LucideIcon;
  description?: string;
  className?: string;
  delay?: number;
  /** Se false, desativa a animação de troca de número para este card. Default true. */
  animateValue?: boolean;
}

function isNumericValue(v: string | number): boolean {
  if (typeof v === "number") return Number.isFinite(v);
  const s = String(v).trim();
  if (s === "" || s === "—" || s === "-") return false;
  if (s.includes("/")) return false;
  if (/[R$%kM]/.test(s)) return false;
  const compact = s.replace(/\s/g, "");
  return /^-?\d+$/.test(compact) || /^-?\d+[.,]\d+$/.test(compact);
}

export function StatsCard({ title, value, change, changeType = "neutral", icon: Icon, description, className, delay = 0, animateValue = true }: StatsCardProps) {
  const showAnimated = animateValue && isNumericValue(value);
  const displayValue = showAnimated ? (
    <AnimatedNumber value={value} format={(n) => n.toLocaleString("pt-BR")} className="text-xl sm:text-2xl md:text-3xl font-bold font-display tracking-tight truncate" />
  ) : (
    <span className="text-xl sm:text-2xl md:text-3xl font-bold font-display tracking-tight truncate">{value}</span>
  );

  return (
    <div
      className={cn(
        "glass-card rounded-xl p-4 sm:p-6 hover-lift group cursor-default min-w-0",
        className
      )}
      style={{ animationDelay: `${delay}ms` }}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="space-y-1 min-w-0">
          <p className="text-xs sm:text-sm font-medium text-muted-foreground truncate">{title}</p>
          <p className="text-xl sm:text-2xl md:text-3xl font-bold font-display tracking-tight truncate">{displayValue}</p>
          {change && (
            <p className={cn(
              "text-xs font-medium truncate",
              changeType === "positive" && "text-success",
              changeType === "negative" && "text-destructive",
              changeType === "neutral" && "text-muted-foreground"
            )}>
              {change}
            </p>
          )}
          {description && (
            <p className="text-xs text-muted-foreground mt-1 line-clamp-2">{description}</p>
          )}
        </div>
        <div
          className="rounded-lg border p-2.5 sm:p-3 transition-all duration-300 shrink-0 group-hover:shadow-lg"
          style={{
            borderColor: "hsl(var(--tertiary) / 0.24)",
            background:
              "linear-gradient(135deg, hsl(var(--primary) / 0.14), hsl(var(--accent) / 0.12) 58%, hsl(var(--tertiary) / 0.16))",
            boxShadow: "0 10px 24px hsl(var(--tertiary) / 0.1)",
          }}
        >
          <Icon
            className="h-4 w-4 sm:h-5 sm:w-5 transition-colors duration-300"
            style={{ color: "hsl(var(--tertiary))" }}
          />
        </div>
      </div>
    </div>
  );
}
