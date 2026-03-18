import { StatsCard } from "@/components/dashboard/StatsCard";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { Calculator, FileText, TrendingUp, PieChart } from "lucide-react";
import { Link } from "react-router-dom";

export default function ContabilPage() {
  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold font-display tracking-tight">Contábil</h1>
        <p className="text-sm text-muted-foreground mt-1">Balancete, DRE e demonstrações</p>
      </div>

      <div className="flex flex-wrap gap-2">
        {[
          { label: "Visão Geral", path: "/contabil" },
          { label: "Balancete", path: "/contabil/balancete" },
          { label: "DRE", path: "/contabil/dre" },
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

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <StatsCard title="Empresas Ativas" value="—" icon={Calculator} />
        <StatsCard title="Balancetes" value="—" icon={FileText} />
        <StatsCard title="DREs" value="—" icon={TrendingUp} />
        <StatsCard title="Conciliações" value="—" icon={PieChart} />
      </div>

      {/* Cards Balancete e DRE */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Link to="/contabil/balancete">
          <GlassCard className="p-6 cursor-pointer h-full">
            <h3 className="text-sm font-semibold font-display flex items-center gap-2">
              <FileText className="h-4 w-4 text-primary-icon" /> Balancete
            </h3>
            <p className="text-xs text-muted-foreground mt-2">Demonstração contábil por período</p>
            <p className="text-2xl font-bold font-display mt-3">—</p>
            <p className="text-[10px] text-muted-foreground">períodos disponíveis</p>
          </GlassCard>
        </Link>
        <Link to="/contabil/dre">
          <GlassCard className="p-6 cursor-pointer h-full">
            <h3 className="text-sm font-semibold font-display flex items-center gap-2">
              <TrendingUp className="h-4 w-4 text-primary-icon" /> DRE
            </h3>
            <p className="text-xs text-muted-foreground mt-2">Demonstração do resultado do exercício</p>
            <p className="text-2xl font-bold font-display mt-3">—</p>
            <p className="text-[10px] text-muted-foreground">relatórios disponíveis</p>
          </GlassCard>
        </Link>
      </div>
    </div>
  );
}
