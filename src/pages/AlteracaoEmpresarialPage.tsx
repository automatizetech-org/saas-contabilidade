import { useLocation, useNavigate, Link } from "react-router-dom";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { AlteracaoVisaoGeralTab } from "@/pages/alteracao-empresarial/AlteracaoVisaoGeralTab";
import { AlteracaoContratosTab } from "@/pages/alteracao-empresarial/AlteracaoContratosTab";

export default function AlteracaoEmpresarialPage() {
  const location = useLocation();
  const navigate = useNavigate();
  const isContratos = location.pathname === "/alteracao-empresarial/contratos";

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold font-display tracking-tight">Alteração Empresarial</h1>
        <p className="text-sm text-muted-foreground mt-1">Visão geral, formulário e contratos de honorários</p>
      </div>

      {/* Atalhos e métricas */}
      <div className="flex flex-wrap gap-2">
        <Link
          to="/alteracao-empresarial"
          className="rounded-lg border border-border bg-card px-4 py-2 text-xs font-medium hover:bg-muted transition-colors"
        >
          Visão Geral
        </Link>
        <Link
          to="/alteracao-empresarial/contratos"
          className="rounded-lg border border-border bg-card px-4 py-2 text-xs font-medium hover:bg-muted transition-colors"
        >
          Contratos
        </Link>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <Link to="/alteracao-empresarial">
          <GlassCard className="p-6 cursor-pointer h-full border-primary-icon/20 hover:border-primary-icon/40 transition-colors">
            <h3 className="text-sm font-semibold font-display">Visão Geral</h3>
            <p className="text-xs text-muted-foreground mt-2">Formulário de alteração empresarial e envio por WhatsApp</p>
            <p className="text-2xl font-bold font-display mt-3">—</p>
            <p className="text-[10px] text-muted-foreground">formulários</p>
          </GlassCard>
        </Link>
        <Link to="/alteracao-empresarial/contratos">
          <GlassCard className="p-6 cursor-pointer h-full border-primary-icon/20 hover:border-primary-icon/40 transition-colors">
            <h3 className="text-sm font-semibold font-display">Contratos</h3>
            <p className="text-xs text-muted-foreground mt-2">Contratos de honorários e documentos</p>
            <p className="text-2xl font-bold font-display mt-3">—</p>
            <p className="text-[10px] text-muted-foreground">documentos</p>
          </GlassCard>
        </Link>
      </div>

      <Tabs
        value={isContratos ? "contratos" : "visao-geral"}
        onValueChange={(v) => navigate(v === "contratos" ? "/alteracao-empresarial/contratos" : "/alteracao-empresarial")}
      >
        <TabsList className="grid w-full max-w-md grid-cols-2">
          <TabsTrigger value="visao-geral">Visão Geral</TabsTrigger>
          <TabsTrigger value="contratos">Contratos</TabsTrigger>
        </TabsList>
        <TabsContent value="visao-geral" className="mt-4">
          <AlteracaoVisaoGeralTab />
        </TabsContent>
        <TabsContent value="contratos" className="mt-4">
          <AlteracaoContratosTab />
        </TabsContent>
      </Tabs>
    </div>
  );
}
