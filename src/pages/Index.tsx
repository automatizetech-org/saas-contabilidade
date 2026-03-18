import { useMemo } from "react";
import { BarChart3 } from "lucide-react";
import { useBrandingOptional, getAnalyticsTitle } from "@/contexts/BrandingContext";

export default function Index() {
  const branding = useBrandingOptional()?.branding;
  const analyticsTitle = getAnalyticsTitle(branding?.client_name);
  const particles = useMemo(() =>
    Array.from({ length: 20 }, (_, i) => ({
      id: i,
      left: Math.random() * 100,
      top: Math.random() * 100,
      delay: Math.random() * 5,
      duration: 5 + Math.random() * 5,
    })),
  []);

  return (
    <div className="flex items-center justify-center min-h-[80vh] relative overflow-hidden">
      <div className="absolute inset-0 bg-gradient-to-br from-primary/5 via-accent/5 to-primary/5 dark:from-primary/10 dark:via-accent/10 dark:to-primary/10 transition-opacity duration-500">
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_30%_50%,hsl(var(--primary)/0.15),transparent_50%)] animate-pulse-slow" />
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_70%_50%,hsl(var(--accent)/0.15),transparent_50%)] animate-pulse-slow-delayed" />
      </div>

      <div className="absolute inset-0 overflow-hidden">
        {particles.map((particle) => (
          <div
            key={particle.id}
            className="absolute w-2 h-2 bg-primary/20 rounded-full animate-float"
            style={{
              left: `${particle.left}%`,
              top: `${particle.top}%`,
              animationDelay: `${particle.delay}s`,
              animationDuration: `${particle.duration}s`,
            }}
          />
        ))}
      </div>

      <div className="relative z-10 transform-3d max-w-4xl mx-auto px-4 md:px-6">
        <div className="flex flex-col items-center">
          <div className="mb-6 md:mb-10 relative inline-flex items-center justify-center logo-container">
            <div className="absolute inset-0 bg-gradient-to-r from-primary via-accent/40 to-primary rounded-full blur-3xl opacity-30 animate-pulse-glow" />
            <div className="relative logo-wrapper">
              <div className="h-32 md:h-44 w-32 md:w-44 flex items-center justify-center rounded-2xl bg-gradient-to-br from-primary to-accent shadow-2xl logo-image">
                <BarChart3 className="h-16 w-16 md:h-24 md:w-24 text-primary-foreground" />
              </div>
              <div className="absolute inset-0 rounded-full border-2 border-primary-icon/30 animate-ring-1" />
              <div className="absolute inset-0 rounded-full border border-accent/25 animate-ring-2" />
              <div className="absolute inset-0 rounded-full border border-primary-icon/15 animate-ring-1" style={{ animationDelay: "0.5s" }} />
            </div>
          </div>

          <div className="mb-6 md:mb-8 w-full max-w-2xl quote-container">
            <div className="relative quote-wrapper">
              <div className="absolute -left-4 md:-left-8 -top-2 md:-top-4 text-4xl md:text-6xl font-serif text-primary-icon/20 select-none quote-mark-left">"</div>
              <div className="absolute -right-4 md:-right-8 -bottom-2 md:-bottom-4 text-4xl md:text-6xl font-serif text-accent/20 select-none quote-mark-right">"</div>

              <p className="quote-text text-lg md:text-2xl lg:text-3xl font-bold leading-relaxed text-center px-4 md:px-8 py-4 md:py-6 relative z-10 text-foreground transition-colors duration-500">
                <span className="quote-gradient">Confia no Senhor de todo o seu coração!</span>
              </p>

              <div className="absolute inset-0 quote-border rounded-2xl" />
              <div className="absolute inset-0 quote-glow rounded-2xl" />
            </div>
          </div>

          <div className="text-center mb-4 md:mb-6">
            <h1 className="text-4xl md:text-6xl lg:text-7xl font-bold mb-2 md:mb-4 bg-gradient-to-r from-primary via-accent to-primary bg-clip-text text-transparent animate-gradient-shift drop-shadow-lg transition-all duration-500">
              {analyticsTitle}
            </h1>

            <p className="text-base md:text-lg lg:text-xl text-muted-foreground animate-fade-in-up font-medium transition-colors duration-500 px-4">
              Plataforma de Análise e Gestão Empresarial
            </p>
          </div>
        </div>

        <div className="flex justify-center gap-3 md:gap-4 mt-8 md:mt-12">
          <div className="w-2 h-2 md:w-2.5 md:h-2.5 bg-primary rounded-full animate-bounce shadow-lg shadow-primary/50" style={{ animationDelay: "0s" }} />
          <div className="w-2 h-2 md:w-2.5 md:h-2.5 bg-accent rounded-full animate-bounce shadow-lg shadow-accent/50" style={{ animationDelay: "0.2s" }} />
          <div className="w-2 h-2 md:w-2.5 md:h-2.5 bg-primary rounded-full animate-bounce shadow-lg shadow-primary/50" style={{ animationDelay: "0.4s" }} />
        </div>

        <div className="mt-6 md:mt-8 flex justify-center items-center gap-2 px-4 md:px-6 py-2 md:py-3 bg-gradient-to-r from-primary/10 via-accent/10 to-primary/10 backdrop-blur-sm rounded-full border border-primary-icon/20 animate-fade-in-up mx-auto w-fit" style={{ animationDelay: "0.3s" }}>
          <div className="w-2 h-2 bg-accent rounded-full animate-pulse" />
          <span className="text-xs md:text-sm font-semibold text-foreground">Insights em Tempo Real</span>
        </div>
      </div>
    </div>
  );
}
