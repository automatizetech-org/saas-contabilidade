import { Link, useLocation } from "react-router-dom";
import {
  LayoutDashboard,
  FileText,
  Users,
  DollarSign,
  Activity,
  FileSpreadsheet,
  Settings,
  ChevronDown,
  ChevronRight,
  Menu,
  X,
  Plus,
  Check,
  ChevronsUpDown,
  Building2,
  PanelLeft,
  Calculator,
  ClipboardList,
  Shield,
  Landmark,
} from "lucide-react";
import { cn } from "@/utils";
import { useState, useEffect } from "react";
import { useCompanies } from "@/hooks/useCompanies";
import { useProfile } from "@/hooks/useProfile";
import { useSelectedCompanyIds } from "@/hooks/useSelectedCompanies";
import { useBranding, getSidebarTitle } from "@/contexts/BrandingContext";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Button } from "@/components/ui/button";
import { LionIcon } from "@/components/icons/LionIcon";

interface NavItem {
  name: string;
  path: string;
  icon: React.ElementType;
  description?: string;
  panelKey?: string;
  children?: { name: string; path: string }[];
}

const navItems: NavItem[] = [
  { name: "Dashboard", path: "/dashboard", icon: LayoutDashboard, description: "Visão consolidada", panelKey: "dashboard" },
  {
    name: "Fiscal",
    path: "/fiscal",
    icon: FileText,
    description: "NFS, NFE e NFC",
    panelKey: "fiscal",
    children: [
      { name: "Visão Geral", path: "/fiscal" },
      { name: "NFS", path: "/fiscal/nfs" },
      { name: "NFE/NFC", path: "/fiscal/nfe-nfc" },
      { name: "DIFAL", path: "/fiscal/difal" },
      { name: "IRRF/CSLL", path: "/fiscal/irrf-csll" },
      { name: "Certidões", path: "/fiscal/certidoes" },
    ],
  },
  {
    name: "Depto. Pessoal",
    path: "/dp",
    icon: Users,
    description: "RH e Folha",
    panelKey: "dp",
    children: [
      { name: "Visão Geral", path: "/dp" },
      { name: "FGTS", path: "/dp/fgts" },
      { name: "DARF", path: "/dp/darf" },
      { name: "INSS", path: "/dp/inss" },
    ],
  },
  {
    name: "Contábil",
    path: "/contabil",
    icon: Calculator,
    description: "Balancete e DRE",
    panelKey: "contabil",
    children: [
      { name: "Visão Geral", path: "/contabil" },
      { name: "Balancete", path: "/contabil/balancete" },
      { name: "DRE", path: "/contabil/dre" },
    ],
  },
  { name: "IR", path: "/ir", icon: LionIcon, description: "Clientes e declarações de IR", panelKey: "ir" },
  {
    name: "Inteligência Tributária",
    path: "/inteligencia-tributaria",
    icon: Landmark,
    description: "Simples, Lucro Real e Presumido",
    panelKey: "inteligencia_tributaria",
    children: [
      { name: "Visão Geral", path: "/inteligencia-tributaria" },
      { name: "Simples Nacional", path: "/inteligencia-tributaria/simples-nacional" },
      { name: "Lucro Real", path: "/inteligencia-tributaria/lucro-real" },
      { name: "Lucro Presumido", path: "/inteligencia-tributaria/lucro-presumido" },
    ],
  },
  {
    name: "Paralegal",
    path: "/paralegal",
    icon: Shield,
    description: "Certificados e rotinas",
    panelKey: "paralegal",
    children: [
      { name: "Visão Geral", path: "/paralegal" },
      { name: "Certificados", path: "/paralegal/certificados" },
      { name: "Tarefas", path: "/paralegal/tarefas" },
      { name: "Clientes", path: "/paralegal/clientes" },
      { name: "Taxas e Impostos", path: "/paralegal/taxas-impostos" },
    ],
  },
  { name: "Financeiro", path: "/financeiro", icon: DollarSign, description: "Contas e Receita", panelKey: "financeiro" },
  { name: "Operações", path: "/operacoes", icon: Activity, description: "Sincronização e monitoramento", panelKey: "operacoes" },
  { name: "Documentos", path: "/documentos", icon: FileSpreadsheet, description: "Gestão documental", panelKey: "documentos" },
  { name: "Empresas", path: "/empresas", icon: Building2, description: "Lista e edição de empresas", panelKey: "empresas" },
  {
    name: "Alteração Empresarial",
    path: "/alteracao-empresarial",
    icon: ClipboardList,
    description: "Visão geral e contratos",
    panelKey: "alteracao_empresarial",
    children: [
      { name: "Visão Geral", path: "/alteracao-empresarial" },
      { name: "Contratos", path: "/alteracao-empresarial/contratos" },
    ],
  },
  { name: "Administração", path: "/admin", icon: Settings, description: "Usuários e config" },
];

export function AppSidebar({ open = true, onToggle }: { open?: boolean; onToggle?: () => void }) {
  const location = useLocation();
  const [expandedItems, setExpandedItems] = useState<string[]>([]);
  const [mobileOpen, setMobileOpen] = useState(false);
  const { data: companies = [] } = useCompanies();
  const { selectedCompanyIds, setSelectedCompanyIds } = useSelectedCompanyIds();
  const { isSuperAdmin, canAccessAdmin, profile } = useProfile();
  const { logoUrl, branding } = useBranding();
  const sidebarTitle = getSidebarTitle(branding?.client_name);

  const visibleNavItems = navItems.filter((item) => {
    if (item.path === "/admin") return canAccessAdmin;
    if (isSuperAdmin) return true;
    const access = profile?.panel_access;
    if (!access || access[item.panelKey!] !== false) return true;
    return false;
  });

  const toggleExpand = (path: string) => {
    setExpandedItems((prev) =>
      prev.includes(path) ? prev.filter((p) => p !== path) : [...prev, path]
    );
  };

  const isActive = (path: string) => location.pathname === path;
  const isParentActive = (item: NavItem) =>
    item.children?.some((child) => location.pathname === child.path) || location.pathname === item.path;

  useEffect(() => {
    if (window.innerWidth < 768) setMobileOpen(false);
  }, [location.pathname]);

  const sidebarContent = (
    <>
      <div className="absolute inset-0 bg-gradient-to-br from-primary/5 via-transparent to-accent/5 dark:from-primary/10 dark:to-accent/10 pointer-events-none transition-opacity duration-500" />
      <div className="relative p-6 md:p-4 md:pt-6 border-b border-border bg-gradient-to-r from-primary/10 via-primary/5 to-transparent z-10 transition-colors duration-500 flex-shrink-0">
        <div className="flex flex-col items-center gap-5 md:gap-3">
          <div className="relative wk-sidebar-brand flex items-center justify-center w-20 h-20 md:w-14 md:h-14 rounded-2xl md:rounded-xl overflow-hidden bg-card shadow-lg shrink-0">
            <img src={logoUrl} alt="Logo" className="w-full h-full object-contain" />
          </div>
          <div className="text-center w-full min-w-0">
            <h2 className="wk-sidebar-title font-bold tracking-tight text-lg md:text-sm truncate text-foreground">{sidebarTitle}</h2>
            <p className="wk-sidebar-subtitle text-muted-foreground text-sm md:text-xs mt-1">Selecione uma área</p>
            <div className="mt-4 md:mt-2 flex items-center gap-2 w-full min-w-0">
              <Popover>
                <PopoverTrigger asChild>
                  <Button
                    variant="outline"
                    role="combobox"
                    className={cn(
                      "flex-1 min-w-0 h-12 md:h-8 justify-between rounded-xl md:rounded-md border border-border bg-card text-foreground text-base md:text-xs font-normal hover:bg-muted/50 truncate",
                      selectedCompanyIds.length === 0 && "text-muted-foreground"
                    )}
                  >
                    <span className="truncate min-w-0">
                      {selectedCompanyIds.length === 0
                        ? "Todas as empresas"
                        : selectedCompanyIds.length === 1
                          ? companies.find((c) => c.id === selectedCompanyIds[0])?.name ?? "1 empresa selecionada"
                          : `${selectedCompanyIds.length} empresas selecionadas`}
                    </span>
                    <ChevronsUpDown className="ml-1 h-5 w-5 md:h-3 md:w-3 shrink-0 opacity-50 flex-shrink-0" />
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-[var(--radix-popover-trigger-width)] max-w-[min(18rem,85vw)] p-0" align="start">
                  <Command className="rounded-md border-0 shadow-none">
                    <CommandInput placeholder="Buscar empresa..." className="h-8 text-xs" />
                    <CommandList>
                      <CommandEmpty className="text-xs py-3">Nenhuma empresa encontrada.</CommandEmpty>
                      <CommandGroup className="p-0">
                        <CommandItem
                          value="Todas as empresas"
                          onSelect={() => setSelectedCompanyIds([])}
                          className="text-xs py-1.5 gap-2"
                        >
                          <span className={cn("flex h-4 w-4 shrink-0 items-center justify-center rounded border", selectedCompanyIds.length === 0 ? "bg-primary border-primary" : "border-input")}>
                            {selectedCompanyIds.length === 0 ? <Check className="h-2.5 w-2.5 text-primary-foreground" /> : null}
                          </span>
                          <span className="truncate min-w-0">Todas as empresas</span>
                        </CommandItem>
                        {companies.map((c) => {
                          const selected = selectedCompanyIds.includes(c.id);
                          return (
                            <CommandItem
                              key={c.id}
                              value={c.name}
                              onSelect={() => {
                                setSelectedCompanyIds(
                                  selected
                                    ? selectedCompanyIds.filter((id) => id !== c.id)
                                    : [...selectedCompanyIds, c.id]
                                );
                              }}
                              className="text-xs py-1.5 gap-2"
                            >
                              <span className={cn("flex h-4 w-4 shrink-0 items-center justify-center rounded border", selected ? "bg-primary border-primary" : "border-input")}>
                                {selected ? <Check className="h-2.5 w-2.5 text-primary-foreground" /> : null}
                              </span>
                              <span className="truncate min-w-0">{c.name}</span>
                            </CommandItem>
                          );
                        })}
                      </CommandGroup>
                    </CommandList>
                  </Command>
                </PopoverContent>
              </Popover>
              <Link
                to="/empresas/nova"
                className="flex-shrink-0 p-2.5 md:p-1.5 rounded-xl md:rounded-md bg-primary text-primary-foreground hover:opacity-90 transition-colors min-w-[48px] min-h-[48px] md:min-w-[36px] md:min-h-[36px] flex items-center justify-center touch-manipulation"
                title="Cadastrar nova empresa"
              >
                <Plus className="h-5 w-5 md:h-3.5 md:w-3.5" />
              </Link>
            </div>
          </div>
        </div>
      </div>
      <nav className="flex-1 overflow-y-auto overflow-x-hidden p-3 sm:p-4 relative z-10 min-h-0">
        {visibleNavItems.map((item) => (
          <div key={item.path} className="mb-2 sm:mb-3">
            {item.children ? (
              <>
                <button
                  onClick={() => toggleExpand(item.path)}
                  className={cn(
                    "w-full text-left p-3 sm:p-4 rounded-xl transition-all duration-300 card-3d relative min-w-0",
                    isParentActive(item)
                      ? "bg-primary text-primary-foreground shadow-3d sm:transform sm:scale-105"
                      : "bg-card/80 backdrop-blur-sm text-sidebar-foreground hover:bg-sidebar-accent hover:shadow-3d-hover border border-border"
                  )}
                  style={{ transformStyle: "preserve-3d" }}
                >
                  <div className="flex items-center gap-2 min-w-0">
                    <item.icon size={18} className="shrink-0 text-inherit" />
                    <span className="font-medium truncate">{item.name}</span>
                    <span className="ml-auto shrink-0">
                      {expandedItems.includes(item.path) ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
                    </span>
                  </div>
                  {item.description && (
                    <p className={cn("text-xs sm:text-sm mt-1 truncate", isParentActive(item) ? "text-primary-foreground/80" : "text-muted-foreground")}>{item.description}</p>
                  )}
                </button>
                {expandedItems.includes(item.path) && (
                  <div className="ml-3 sm:ml-4 mt-1 space-y-0.5 border-l-2 border-primary-icon/20 pl-3">
                    {item.children.map((child) => (
                      <Link
                        key={child.path}
                        to={child.path}
                        className={cn(
                          "flex items-center rounded-lg px-3 py-2 text-xs font-medium transition-all duration-200 truncate min-w-0",
                          isActive(child.path) ? "bg-primary/10 text-foreground font-semibold" : "text-muted-foreground hover:text-foreground hover:bg-sidebar-accent"
                        )}
                      >
                        {child.name}
                      </Link>
                    ))}
                  </div>
                )}
              </>
            ) : (
              <Link
                to={item.path}
                className={cn(
                  "w-full text-left p-3 sm:p-4 rounded-xl transition-all duration-300 card-3d relative block min-w-0",
                  isActive(item.path)
                    ? "bg-primary text-primary-foreground shadow-3d sm:transform sm:scale-105"
                    : "bg-card/80 backdrop-blur-sm text-sidebar-foreground hover:bg-sidebar-accent hover:shadow-3d-hover border border-border"
                )}
                style={{ transformStyle: "preserve-3d" }}
              >
                <div className="flex items-center gap-2 min-w-0">
                  <item.icon size={18} className="shrink-0 text-inherit" />
                  <span className="font-medium truncate">{item.name}</span>
                </div>
                {item.description && (
                  <p className={cn("text-xs sm:text-sm mt-1 truncate", isActive(item.path) ? "text-primary-foreground/80" : "text-muted-foreground")}>{item.description}</p>
                )}
              </Link>
            )}
          </div>
        ))}
      </nav>
    </>
  );

  return (
    <>
      <button
        onClick={() => setMobileOpen(true)}
        className="fixed z-50 md:hidden p-2.5 min-w-[44px] min-h-[44px] flex items-center justify-center bg-card rounded-xl shadow-lg border border-border hover:bg-primary/10 transition-all touch-manipulation active:scale-95 sm:top-4 sm:left-4"
        style={{
          top: "max(6px, calc(6px + env(safe-area-inset-top, 0px)))",
          left: "max(12px, calc(12px + env(safe-area-inset-left, 0px)))",
        }}
        aria-label="Abrir menu"
      >
        <Menu size={22} className="text-foreground shrink-0" />
      </button>
      <div
        className={cn(
          "fixed inset-0 z-40 md:hidden transition-opacity duration-300",
          mobileOpen ? "opacity-100 bg-black/50 pointer-events-auto" : "opacity-0 bg-black/50 pointer-events-none"
        )}
        onClick={() => setMobileOpen(false)}
        aria-hidden="true"
      />
      <aside
        className={cn(
          "hidden md:flex md:w-full md:flex-shrink-0 md:h-dvh bg-sidebar border-r border-sidebar-border flex-col shadow-3d relative overflow-hidden transition-colors duration-500 sticky top-0 z-40 transition-transform duration-300 ease-in-out",
          open ? "translate-x-0" : "-translate-x-full"
        )}
        aria-hidden={!open}
      >
        {sidebarContent}
        {!open && onToggle && (
          <button
            onClick={onToggle}
            className="absolute top-4 right-0 translate-x-full rounded-r-lg bg-card border border-l-0 border-border p-2 shadow-md hover:bg-primary/10 transition-colors"
            aria-label="Exibir painel lateral"
          >
            <PanelLeft className="h-4 w-4 text-foreground" />
          </button>
        )}
      </aside>
      <aside
        className={cn(
          "fixed inset-y-0 left-0 z-50 w-[min(18rem,85vw)] max-w-[18rem] bg-sidebar border-r border-sidebar-border flex flex-col shadow-3d overflow-hidden md:hidden transform-gpu transition-transform duration-300 ease-out safe-area-inset-left"
        )}
        style={{ transform: mobileOpen ? "translateX(0)" : "translateX(-100%)" }}
        aria-hidden={!mobileOpen}
      >
        <button
          onClick={() => setMobileOpen(false)}
          className="absolute top-3 right-3 z-20 p-2 min-w-[44px] min-h-[44px] flex items-center justify-center hover:bg-black/5 dark:hover:bg-white/10 rounded-lg transition-colors touch-manipulation active:scale-95"
          aria-label="Fechar menu"
        >
          <X size={22} className="text-foreground shrink-0" />
        </button>
        <div className="flex-1 min-h-0 overflow-y-auto overflow-x-hidden overscroll-contain flex flex-col">
          {sidebarContent}
        </div>
      </aside>
    </>
  );
}
