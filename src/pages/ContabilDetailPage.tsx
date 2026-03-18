import { useParams, Link } from "react-router-dom";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { ArrowLeft } from "lucide-react";

const TOPIC_LABELS: Record<string, string> = {
  balancete: "Balancete",
  dre: "DRE",
};

export default function ContabilDetailPage() {
  const { topic } = useParams<{ topic: string }>();
  const label = (topic && TOPIC_LABELS[topic]) || topic || "Tópico";

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2">
        <Link
          to="/contabil"
          className="rounded-lg border border-border bg-card p-2 text-muted-foreground hover:bg-muted transition-colors"
          aria-label="Voltar"
        >
          <ArrowLeft className="h-4 w-4" />
        </Link>
        <div>
          <h1 className="text-2xl font-bold font-display tracking-tight">{label}</h1>
          <p className="text-sm text-muted-foreground mt-1">Demonstrações e relatórios</p>
        </div>
      </div>
      <GlassCard className="p-8">
        <p className="text-sm text-muted-foreground">Conteúdo específico de {label} será exibido aqui.</p>
      </GlassCard>
    </div>
  );
}
