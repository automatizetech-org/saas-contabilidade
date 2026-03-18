import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter } from "react-router-dom";
import { AppRoutes } from "@/routes/AppRoutes";
import { SelectedCompaniesProvider } from "@/hooks/useSelectedCompanies";
import { BrandingProvider } from "@/contexts/BrandingContext";

const queryClient = new QueryClient();

const App = () => (
  <QueryClientProvider client={queryClient}>
    <TooltipProvider>
      <Toaster />
      <Sonner />
      <BrowserRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
        <BrandingProvider>
          <SelectedCompaniesProvider>
            <AppRoutes />
          </SelectedCompaniesProvider>
        </BrandingProvider>
      </BrowserRouter>
    </TooltipProvider>
  </QueryClientProvider>
);

export default App;
