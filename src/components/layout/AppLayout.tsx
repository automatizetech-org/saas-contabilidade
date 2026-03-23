import { ReactNode } from "react";
import { useNavigate, useLocation, Navigate } from "react-router-dom";
import { keepPreviousData, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { AppSidebar } from "./AppSidebar";
import { CommandPalette } from "./CommandPalette";
import { useProfile } from "@/hooks/useProfile";
import { useSelectedCompanyIds } from "@/hooks/useSelectedCompanies";
import { useCompanies } from "@/hooks/useCompanies";
import { useSupabaseConnectionStatus } from "@/hooks/useSupabaseConnectionStatus";
import { getEcacMailboxSummary, getEcacMailboxSummaryQueryKey } from "@/services/ecacMailboxService";
import { pathToPanelKey } from "@/lib/panelAccess";
import { getVisibilityAwareRefetchInterval } from "@/lib/queryPolling";
import { cn } from "@/utils";
import { Moon, Sun, PanelLeftClose, PanelLeft } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { supabase } from "@/services/supabaseClient";
import { MaintenanceBanner } from "@/components/MaintenanceBanner";
import { getFiscalDetailDocumentsPage, getFiscalDetailSummary, type FiscalDetailKind } from "@/services/documentsService";
import { getNfsStatsByDateRange } from "@/services/dashboardService";
import { persistQueryClient } from "@tanstack/query-persist-client-core";
import { createIndexedDBPersisterForOffice } from "@/lib/reactQueryPersistenceIndexedDB";

const SIDEBAR_OPEN_KEY = "sidebar-open";

export function AppLayout({ children }: { children: ReactNode }) {
  const navigate = useNavigate();
  const location = useLocation();
  const { isSuperAdmin, profile } = useProfile();
  const { selectedCompanyIds } = useSelectedCompanyIds();
  const { data: companies = [] } = useCompanies();
  const queryClient = useQueryClient();
  const [prefetchedForKey, setPrefetchedForKey] = useState<string | null>(null);
  const persistenceRef = useRef<{ officeId: string; unsubscribe: () => void } | null>(null);
  const panelKey = pathToPanelKey(location.pathname);
  const noAccess =
    !isSuperAdmin &&
    profile &&
    panelKey &&
    profile.panel_access?.[panelKey] === false;
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

  const showMaintenanceBanner = useSupabaseConnectionStatus();
  const companyFilter = selectedCompanyIds.length > 0 ? selectedCompanyIds : null;
  const { data: ecacMailboxSummary } = useQuery({
    queryKey: getEcacMailboxSummaryQueryKey(companyFilter),
    queryFn: () => getEcacMailboxSummary(companyFilter),
    placeholderData: keepPreviousData,
    refetchInterval: () => getVisibilityAwareRefetchInterval(),
    refetchIntervalInBackground: true,
    enabled: Boolean(profile?.office_id) && profile?.office_status !== "inactive",
  });
  const mailboxToastStateRef = useRef<{ scope: string; unread: number | null }>({
    scope: "",
    unread: null,
  });

  useEffect(() => {
    localStorage.setItem(SIDEBAR_OPEN_KEY, String(sidebarOpen));
  }, [sidebarOpen]);

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
    localStorage.setItem("theme", dark ? "dark" : "light");
  }, [dark]);

  useEffect(() => {
    const scope = `${profile?.office_id ?? "none"}|${companyFilter?.join(",") ?? "all"}`;
    if (mailboxToastStateRef.current.scope !== scope) {
      mailboxToastStateRef.current = { scope, unread: null };
    }
  }, [profile?.office_id, companyFilter]);

  useEffect(() => {
    if (!profile?.office_id || profile.office_status === "inactive") return;
    if (!ecacMailboxSummary) return;

    const scope = `${profile.office_id}|${companyFilter?.join(",") ?? "all"}`;
    const currentUnread = ecacMailboxSummary.unreadMessages;
    const previousUnread = mailboxToastStateRef.current.unread;

    if (previousUnread == null) {
      const sessionKey = `ecac-mailbox-login-toast:${scope}`;
      if (currentUnread > 0 && sessionStorage.getItem(sessionKey) !== "1") {
        toast("Caixa Postal E-CAC com novidades", {
          description: `${currentUnread} mensagem(ns) nova(s) aguardando leitura.`,
        });
        sessionStorage.setItem(sessionKey, "1");
      }
    } else if (currentUnread > previousUnread) {
      const diff = currentUnread - previousUnread;
      toast("Nova notificacao da Caixa Postal E-CAC", {
        description: `${diff} nova(s) mensagem(ns) recebida(s).`,
      });
    }

    mailboxToastStateRef.current = {
      scope,
      unread: currentUnread,
    };
  }, [companyFilter, ecacMailboxSummary, profile?.office_id, profile?.office_status]);

  useEffect(() => {
    if (!profile?.office_id || profile.office_status !== "inactive") return;
    const msg = `O escritório "${profile.office_name ?? ""}" está inativado. Entre em contato com o suporte.`;
    supabase.auth.signOut().then(() => {
      navigate("/login", { replace: true, state: { message: msg } });
    });
  }, [profile?.office_id, profile?.office_status, profile?.office_name, navigate]);

  // Persistência do cache do React Query em IndexedDB (por office_id).
  // Isso faz com que, após login/recarregar, as telas apareçam imediatamente com dados do cache.
  useEffect(() => {
    if (!profile?.office_id) return;
    if (profile.office_status === "inactive") return;

    const officeId = profile.office_id;
    if (persistenceRef.current?.officeId === officeId) return;

    // Na primeira carga após login não limpamos o cache inteiro, porque isso
    // remove auth/profile e pode disparar uma cascata de refetch logo ao entrar.
    // Só limpamos queries de dados ao trocar de escritório de fato.
    if (persistenceRef.current) {
      persistenceRef.current.unsubscribe();
      if (persistenceRef.current.officeId !== officeId) {
        queryClient.removeQueries({
          predicate: (query) => {
            const rootKey = String(query.queryKey[0] ?? "");
            return rootKey !== "auth-session" && rootKey !== "profile";
          },
        });
      }
      persistenceRef.current = null;
    }

    const persister = createIndexedDBPersisterForOffice(officeId);
    const buster = officeId;
    const [unsubscribe] = persistQueryClient({
      queryClient,
      persister,
      buster,
      maxAge: 1000 * 60 * 60 * 24 * 7, // 7 dias
    });

    persistenceRef.current = { officeId, unsubscribe };

    return () => {
      unsubscribe();
    };
  }, [profile?.office_id, profile?.office_status, queryClient]);

  // Warm cache após login: antecipa cards e agregados sem disparar a lista cursorizada.
  useEffect(() => {
    if (!profile?.office_id || profile.office_status === "inactive") return;
    if (location.pathname.startsWith("/admin")) return;

    const companyFilterKey = selectedCompanyIds.length ? selectedCompanyIds.join(",") : "all";
    const cacheKey = `${profile.office_id}|${companyFilterKey}`;
    if (prefetchedForKey === cacheKey) return;
    setPrefetchedForKey(cacheKey);

    const now = new Date();
    const first = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}-01`;
    const last = new Date(now.getFullYear(), now.getMonth() + 1, 0).toISOString().slice(0, 10);

    const kind: FiscalDetailKind = "nfs";
    const companyIdsFilter = selectedCompanyIds.length ? selectedCompanyIds : null;
    const prev = new Date(Number(first.slice(0, 4)), Number(first.slice(5, 7)) - 2, 1);
    const prevFirst = `${prev.getFullYear()}-${String(prev.getMonth() + 1).padStart(2, "0")}-01`;
    const prevLast = new Date(prev.getFullYear(), prev.getMonth() + 1, 0).toISOString().slice(0, 10);

    queryClient.prefetchQuery({
      queryKey: ["fiscal-detail-summary", kind, companyIdsFilter, first, last],
      queryFn: () =>
        getFiscalDetailSummary({
          kind,
          companyIds: companyIdsFilter,
          dateFrom: first,
          dateTo: last,
          limit: 50_000,
        }),
    });

    queryClient.prefetchQuery({
      queryKey: ["nfs-stats", companyIdsFilter, first, last],
      queryFn: () => getNfsStatsByDateRange(companyIdsFilter, first, last),
    });

    queryClient.prefetchQuery({
      queryKey: ["nfs-stats-prev", companyIdsFilter, prevFirst, prevLast],
      queryFn: () => getNfsStatsByDateRange(companyIdsFilter, prevFirst, prevLast),
    });
  }, [location.pathname, profile?.office_id, profile?.office_status, selectedCompanyIds, prefetchedForKey, queryClient]);

  const handleSignOut = async () => {
    // Limpa o cache em memória e para a persistência em background.
    queryClient.clear();
    if (persistenceRef.current) {
      persistenceRef.current.unsubscribe();
      persistenceRef.current = null;
    }
    await supabase.auth.signOut();
    navigate("/login", { replace: true });
  };

  return (
    <>
      {showMaintenanceBanner && <MaintenanceBanner />}
      <div
        className={cn(
          "grid w-full max-w-full min-w-0 h-dvh overflow-x-hidden bg-background transition-colors duration-500",
          "grid-cols-1 md:transition-[grid-template-columns] md:duration-300 md:ease-in-out",
          sidebarOpen ? "md:grid-cols-[16rem_1fr]" : "md:grid-cols-[0_1fr]",
          showMaintenanceBanner && "pt-[52px]"
        )}
      >
      {/* No mobile: w-0 e h-0 para não ocupar espaço; o sidebar só mostra o botão/drawer (position fixed) */}
      <div
        className={cn(
          "w-0 h-0 min-h-0 shrink-0 overflow-visible md:h-dvh md:flex-shrink-0",
          sidebarOpen ? "md:w-64" : "md:w-0"
        )}
      >
        <AppSidebar
          open={sidebarOpen}
          onToggle={() => setSidebarOpen((v) => !v)}
          ecacMailboxUnread={ecacMailboxSummary?.unreadMessages ?? 0}
        />
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
    </>
  );
}
