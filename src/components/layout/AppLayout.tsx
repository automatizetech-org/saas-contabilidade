import { ReactNode } from "react";
import { useNavigate, useLocation, Navigate } from "react-router-dom";
import { AppSidebar } from "./AppSidebar";
import { CommandPalette } from "./CommandPalette";
import { useProfile } from "@/hooks/useProfile";
import { useSelectedCompanyIds } from "@/hooks/useSelectedCompanies";
import { useCompanies } from "@/hooks/useCompanies";
import { pathToPanelKey } from "@/lib/panelAccess";
import { cn } from "@/utils";
import { Moon, Sun, PanelLeftClose, PanelLeft } from "lucide-react";
import { useState, useEffect } from "react";
import { supabase } from "@/services/supabaseClient";

const SIDEBAR_OPEN_KEY = "sidebar-open";

export function AppLayout({ children }: { children: ReactNode }) {
  const navigate = useNavigate();
  const location = useLocation();
  const { isSuperAdmin, profile } = useProfile();
  const { selectedCompanyIds } = useSelectedCompanyIds();
  const { data: companies = [] } = useCompanies();
  const panelKey = pathToPanelKey(location.pathname);
  const noAccess =
    !isSuperAdmin &&
    profile &&
    panelKey &&
    (profile.panel_access as Record<string, boolean>)?.[panelKey] === false;
  const [dark, setDark] = useState(() => {
    const stored = localStorage.getItem("theme");
    if (stored === "light" || stored === "dark") return stored === "dark";
    return true; // padrão: tema escuro
  });
  const [sidebarOpen, setSidebarOpen] = useState(() => {
    const stored = localStorage.getItem(SIDEBAR_OPEN_KEY);
    if (stored === "true" || stored === "false") return stored === "true";
    return true;
  });

  useEffect(() => {
    localStorage.setItem(SIDEBAR_OPEN_KEY, String(sidebarOpen));
  }, [sidebarOpen]);

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    localStorage.setItem("theme", dark ? "dark" : "light");
  }, [dark]);

  const handleSignOut = async () => {
    await supabase.auth.signOut();
    navigate("/login", { replace: true });
  };

  return (
    <div
      className={cn(
        "grid w-full max-w-full min-w-0 h-dvh overflow-x-hidden bg-background transition-colors duration-500",
        "grid-cols-1 md:transition-[grid-template-columns] md:duration-300 md:ease-in-out",
        sidebarOpen ? "md:grid-cols-[16rem_1fr]" : "md:grid-cols-[0_1fr]"
      )}
    >
      {/* No mobile: w-0 e h-0 para não ocupar espaço; o sidebar só mostra o botão/drawer (position fixed) */}
      <div
        className={cn(
          "w-0 h-0 min-h-0 shrink-0 overflow-visible md:h-dvh md:flex-shrink-0",
          sidebarOpen ? "md:w-64" : "md:w-0"
        )}
      >
        <AppSidebar open={sidebarOpen} onToggle={() => setSidebarOpen((v) => !v)} />
      </div>

      <main className="min-w-0 w-full max-w-full flex flex-col overflow-x-hidden relative min-h-0 h-dvh md:h-auto">
        <header className="flex-shrink-0 w-full flex flex-col border-b border-border bg-card/90 backdrop-blur-sm min-w-0 pt-[env(safe-area-inset-top)]">
          <div className="h-14 min-h-[56px] flex items-center justify-between gap-2 pl-14 pr-3 sm:pl-4 sm:pr-4 md:pl-4 md:pr-6">
          <button
            onClick={() => setSidebarOpen((v) => !v)}
            className="hidden md:flex items-center justify-center w-9 h-9 rounded-lg text-muted-foreground hover:text-primary-icon hover:bg-muted transition-colors shrink-0"
            aria-label={sidebarOpen ? "Recolher painel lateral" : "Exibir painel lateral"}
          >
            {sidebarOpen ? <PanelLeftClose className="h-5 w-5" /> : <PanelLeft className="h-5 w-5" />}
          </button>
          <p className="text-sm text-muted-foreground truncate min-w-0 hidden sm:block">
            Seja bem-vindo,{" "}
            {selectedCompanyIds.length === 0
              ? "Todas as empresas"
              : selectedCompanyIds.length === 1
                ? companies.find((c) => c.id === selectedCompanyIds[0])?.name ?? "—"
                : `${selectedCompanyIds.length} empresas selecionadas`}
          </p>
          <div className="flex items-center gap-1 sm:gap-2 min-w-0 justify-end ml-auto">
            <button
              onClick={() => setDark(!dark)}
              className="flex items-center justify-center w-9 h-9 rounded-lg text-muted-foreground hover:text-amber-600 dark:hover:text-amber-400 hover:bg-muted transition-colors shrink-0 touch-manipulation"
              aria-label="Alternar tema"
            >
              {dark ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
            </button>
            <button
              onClick={handleSignOut}
              className="flex items-center justify-center h-9 px-3 sm:px-4 rounded-lg text-sm font-medium text-foreground hover:text-primary-icon hover:bg-muted transition-colors shrink-0 touch-manipulation"
            >
              Sair
            </button>
          </div>
          </div>
        </header>

        <div className="flex-1 overflow-y-auto overflow-x-hidden min-h-0 min-w-0 max-w-full pb-[env(safe-area-inset-bottom)]">
          <CommandPalette />
          <div className="p-3 sm:p-4 md:p-6 w-full min-w-0 max-w-full animate-fade-in-up box-border">
            {noAccess ? <Navigate to="/dashboard" replace /> : children}
          </div>
        </div>
      </main>
    </div>
  );
}
