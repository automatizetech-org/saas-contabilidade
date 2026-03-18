import { ReactNode } from "react";
import { cn } from "@/utils";

interface GlassCardProps {
  children: ReactNode;
  className?: string;
  hover?: boolean;
  gradient?: boolean;
}

export function GlassCard({ children, className, hover = true, gradient = false }: GlassCardProps) {
  return (
    <div className={cn(
      "card-3d-elevated rounded-xl",
      gradient && "gradient-border",
      !hover && "!transform-none",
      className
    )}>
      {children}
    </div>
  );
}
