import { cn } from "@/utils";

interface SkeletonCardProps {
  className?: string;
}

export function SkeletonCard({ className }: SkeletonCardProps) {
  return (
    <div className={cn("glass-card rounded-xl p-6 space-y-4", className)}>
      <div className="flex items-start justify-between">
        <div className="space-y-2 flex-1">
          <div className="h-3 w-24 rounded bg-muted animate-shimmer bg-gradient-to-r from-muted via-muted-foreground/10 to-muted bg-[length:200%_100%]" />
          <div className="h-8 w-32 rounded bg-muted animate-shimmer bg-gradient-to-r from-muted via-muted-foreground/10 to-muted bg-[length:200%_100%]" />
          <div className="h-3 w-20 rounded bg-muted animate-shimmer bg-gradient-to-r from-muted via-muted-foreground/10 to-muted bg-[length:200%_100%]" />
        </div>
        <div className="h-11 w-11 rounded-lg bg-muted animate-shimmer bg-gradient-to-r from-muted via-muted-foreground/10 to-muted bg-[length:200%_100%]" />
      </div>
    </div>
  );
}

export function SkeletonTable({ rows = 5 }: { rows?: number }) {
  return (
    <div className="glass-card rounded-xl overflow-hidden">
      <div className="p-4 border-b border-border">
        <div className="h-5 w-40 rounded bg-muted animate-shimmer bg-gradient-to-r from-muted via-muted-foreground/10 to-muted bg-[length:200%_100%]" />
      </div>
      <div className="divide-y divide-border">
        {Array.from({ length: rows }).map((_, i) => (
          <div key={i} className="p-4 flex gap-4">
            <div className="h-4 w-28 rounded bg-muted animate-shimmer bg-gradient-to-r from-muted via-muted-foreground/10 to-muted bg-[length:200%_100%]" />
            <div className="h-4 w-20 rounded bg-muted animate-shimmer bg-gradient-to-r from-muted via-muted-foreground/10 to-muted bg-[length:200%_100%]" />
            <div className="h-4 w-16 rounded bg-muted animate-shimmer bg-gradient-to-r from-muted via-muted-foreground/10 to-muted bg-[length:200%_100%]" />
            <div className="h-4 flex-1 rounded bg-muted animate-shimmer bg-gradient-to-r from-muted via-muted-foreground/10 to-muted bg-[length:200%_100%]" />
          </div>
        ))}
      </div>
    </div>
  );
}
