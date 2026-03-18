import { StatsCard } from "@/components/dashboard/StatsCard";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatusBadge } from "@/components/dashboard/StatusBadge";
import { Users, FileText, CheckSquare, AlertTriangle, Clock } from "lucide-react";
import { Link } from "react-router-dom";
import { toast } from "sonner";

const checklist = [
  { tarefa: "Folha de pagamento", empresa: "Tech Solutions Ltda", competencia: "07/2025", status: "validado" as const },
  { tarefa: "FGTS", empresa: "Tech Solutions Ltda", competencia: "07/2025", status: "validado" as const },
  { tarefa: "INSS", empresa: "Comércio ABC", competencia: "07/2025", status: "pendente" as const },
  { tarefa: "IRRF", empresa: "Comércio ABC", competencia: "07/2025", status: "processando" as const },
  { tarefa: "Folha de pagamento", empresa: "Indústria XYZ", competencia: "07/2025", status: "divergente" as const },
  { tarefa: "Rescisão", empresa: "Serviços Delta", competencia: "07/2025", status: "pendente" as const },
  { tarefa: "Férias", empresa: "Logística Beta", competencia: "06/2025", status: "validado" as const },
];

const guias = [
  { nome: "Guia FGTS - Jul/2025", empresa: "Tech Solutions Ltda", tipo: "PDF", data: "2025-07-10" },
  { nome: "Guia INSS - Jul/2025", empresa: "Comércio ABC", tipo: "PDF", data: "2025-07-08" },
  { nome: "Folha Analítica - Jul/2025", empresa: "Indústria XYZ", tipo: "PDF", data: "2025-07-05" },
  { nome: "Guia IRRF - Jun/2025", empresa: "Serviços Delta", tipo: "PDF", data: "2025-06-30" },
];

export default function DPPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold font-display tracking-tight">Departamento Pessoal</h1>
        <p className="text-sm text-muted-foreground mt-1">Checklist de fechamento, guias e pendências — FGTS, DARF, INSS</p>
      </div>

      <div className="flex flex-wrap gap-2">
        {[
          { label: "Visão Geral", path: "/dp" },
          { label: "FGTS", path: "/dp/fgts" },
          { label: "DARF", path: "/dp/darf" },
          { label: "INSS", path: "/dp/inss" },
        ].map((item) => (
          <Link
            key={item.path}
            to={item.path}
            className="rounded-lg border border-border bg-card px-4 py-2 text-xs font-medium hover:bg-muted transition-colors"
          >
            {item.label}
          </Link>
        ))}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <StatsCard title="Empresas em Processo" value="142" icon={Users} />
        <StatsCard title="Guias Geradas" value="328" icon={FileText} change="+24 esta semana" changeType="positive" />
        <StatsCard title="Tarefas Concluídas" value="89%" icon={CheckSquare} changeType="positive" />
        <StatsCard title="Pendências" value="16" icon={AlertTriangle} change="4 urgentes" changeType="negative" />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <GlassCard className="overflow-hidden">
          <div className="p-4 border-b border-border">
            <h3 className="text-sm font-semibold font-display">Checklist de Fechamento</h3>
            <p className="text-xs text-muted-foreground">Competência atual</p>
          </div>
          <div className="divide-y divide-border">
            {checklist.map((item, i) => (
              <div key={i} className="px-4 py-3 flex items-center justify-between hover:bg-muted/30 transition-colors">
                <div>
                  <p className="text-xs font-medium">{item.tarefa}</p>
                  <p className="text-[10px] text-muted-foreground">{item.empresa} · {item.competencia}</p>
                </div>
                <StatusBadge status={item.status} />
              </div>
            ))}
          </div>
        </GlassCard>

        <GlassCard className="overflow-hidden">
          <div className="p-4 border-b border-border">
            <h3 className="text-sm font-semibold font-display">Guias em PDF</h3>
            <p className="text-xs text-muted-foreground">Documentos disponíveis para download</p>
          </div>
          <div className="divide-y divide-border">
            {guias.map((guia, i) => (
              <div
                key={i}
                role="button"
                tabIndex={0}
                onClick={() => toast.info("Download em breve", { description: guia.nome })}
                onKeyDown={(e) => e.key === "Enter" && toast.info("Download em breve", { description: guia.nome })}
                className="px-4 py-3 flex items-center justify-between hover:bg-muted/30 transition-colors cursor-pointer"
              >
                <div className="flex items-center gap-3">
                  <div className="h-8 w-8 rounded-lg bg-destructive/10 flex items-center justify-center">
                    <FileText className="h-3.5 w-3.5 text-destructive" />
                  </div>
                  <div>
                    <p className="text-xs font-medium">{guia.nome}</p>
                    <p className="text-[10px] text-muted-foreground">{guia.empresa}</p>
                  </div>
                </div>
                <span className="text-[10px] text-muted-foreground">{guia.data}</span>
              </div>
            ))}
          </div>
        </GlassCard>
      </div>

      {/* Atalhos para FGTS, DARF, INSS */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {[
          { titulo: "FGTS", path: "/dp/fgts", descricao: "Guias e recolhimento FGTS", valor: "—" },
          { titulo: "DARF", path: "/dp/darf", descricao: "Impostos e DARF", valor: "—" },
          { titulo: "INSS", path: "/dp/inss", descricao: "Guias INSS e folha", valor: "—" },
        ].map((item) => (
          <Link key={item.path} to={item.path}>
            <GlassCard className="p-5 cursor-pointer h-full">
              <h4 className="text-sm font-semibold font-display">{item.titulo}</h4>
              <p className="text-xs text-muted-foreground mt-1">{item.descricao}</p>
              <p className="text-lg font-bold font-display mt-2">{item.valor}</p>
            </GlassCard>
          </Link>
        ))}
      </div>
    </div>
  );
}
