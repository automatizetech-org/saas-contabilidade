import { ReactNode } from "react";
import { cn } from "@/utils";
import { LucideIcon } from "lucide-react";

interface StatsCardProps {
  title: string;
  value: string | number;
  change?: string;
  changeType?: "positive" | "negative" | "neutral";
  icon: LucideIcon;
  description?: string;
  className?: string;
  delay?: number;
}

export function StatsCard({ title, value, change, changeType = "neutral", icon: Icon, description, className, delay = 0 }: StatsCardProps) {
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
          <p className="text-xl sm:text-2xl md:text-3xl font-bold font-display tracking-tight truncate">{value}</p>
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
        <div className="rounded-lg bg-primary/10 p-2.5 sm:p-3 group-hover:bg-accent/20 transition-colors duration-300 shrink-0">
          <Icon className="h-4 w-4 sm:h-5 sm:w-5 text-primary-icon group-hover:text-accent transition-colors duration-300" />
        </div>
      </div>
    </div>
  );
}
