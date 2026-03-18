import { cn } from "@/utils";

export function LionIcon({ className }: { className?: string }) {
  return (
    <span
      aria-hidden="true"
      className={cn("inline-flex items-center justify-center text-base leading-none text-foreground", className)}
    >
      {"🦁︎"}
    </span>
  );
}
