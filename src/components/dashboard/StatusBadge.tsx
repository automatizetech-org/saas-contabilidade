import { cn } from "@/utils";

interface StatusBadgeProps {
  status: string | null | undefined;
  className?: string;
}

const statusConfig = {
  novo: { label: "Novo", className: "bg-info/15 text-info" },
  validado: { label: "Validado", className: "bg-success/15 text-success" },
  divergente: { label: "Divergente", className: "bg-warning/15 text-warning" },
  pendente: { label: "Pendente", className: "bg-muted/50 text-muted-foreground" },
  erro: { label: "Erro", className: "bg-destructive/15 text-destructive" },
  sucesso: { label: "Sucesso", className: "bg-success/15 text-success" },
  processando: { label: "Processando", className: "bg-info/15 text-info" },
  // Certidões (mesmo estilo usado na tabela de Certidões)
  negativa: { label: "Negativa", className: "bg-sky-500/15 text-sky-700 dark:text-sky-400" },
  positiva: { label: "Positiva", className: "bg-red-500/15 text-red-700 dark:text-red-400" },
  irregular: { label: "Irregular", className: "bg-red-500/15 text-red-700 dark:text-red-400" },
  irregularidade: { label: "Irregular", className: "bg-red-500/15 text-red-700 dark:text-red-400" },
  indefinido: { label: "Indefinido", className: "bg-muted/50 text-muted-foreground" },
  // Taxas/Impostos (municipais, guias)
  vencido: { label: "Vencido", className: "bg-rose-500/15 text-rose-700 dark:text-rose-300" },
  a_vencer: { label: "A vencer (próximos 30 dias)", className: "bg-amber-500/15 text-amber-700 dark:text-amber-300" },
  regular: { label: "Regular", className: "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300" },
};

export function StatusBadge({ status, className }: StatusBadgeProps) {
  const raw = String(status || "").trim();
  const normalized = raw.toLowerCase();
  const alias: Record<string, keyof typeof statusConfig> = {
    "irregularidade": "irregular",
    "irregular": "irregular",
    "regular": "negativa",
    "positiva": "irregular",
    "positiva com efeito de negativa": "negativa",
    "positiva com efeitos de negativa": "negativa",
    "empregador nao cadastrado": "negativa",
    "empregador não cadastrado": "negativa",
    "não processado": "pendente",
    "nao processado": "pendente",
    "erro: nao processado": "erro",
    "erro: não processado": "erro",
  };
  const key = (alias[normalized] ?? (normalized as keyof typeof statusConfig));
  const config = statusConfig[key];
  const fallbackLabel = raw || "—";
  return (
    <span className={cn(
      // Visual "soft pill" (igual ao padrão de Taxas e Impostos), sem borda/bolinha.
      "inline-flex items-center rounded-full px-2 py-0.5 text-[11px] font-medium whitespace-nowrap",
      (config?.className ?? "bg-muted/50 text-muted-foreground"),
      className
    )}>
      {config?.label ?? fallbackLabel}
    </span>
  );
}
