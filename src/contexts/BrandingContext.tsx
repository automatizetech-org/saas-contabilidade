import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useLocation } from "react-router-dom";
import {
  getBranding,
  upsertBranding,
  uploadLogoAndFavicon as uploadLogoAndFaviconService,
  removeLogo as removeLogoService,
  type ClientBrandingRow,
  type ClientBrandingInput,
} from "@/services/brandingService";
import { deriveBrandTokens } from "@/lib/brandingTheme";
import { supabase } from "@/services/supabaseClient";
import { useProfile } from "@/hooks/useProfile";
import defaultLogoUrl from "@/assets/images/logo.png";

export const BRANDING_QUERY_KEY_PREFIX = ["branding"] as const;

export function getBrandingQueryKey(officeId?: string | null) {
  return [...BRANDING_QUERY_KEY_PREFIX, officeId ?? "current-office"] as const;
}

/** Nome da marca (ex.: "Contabilidade"). Vazio = usa "Dashboard" / "Analytics" sem sufixo. */
export function getBrandDisplayName(clientName: string | null | undefined): string {
  return (clientName ?? "").trim();
}

/** Título da sidebar: "Dashboard" ou "Dashboard {nome}" */
export function getSidebarTitle(clientName: string | null | undefined): string {
  const name = getBrandDisplayName(clientName);
  return name ? `Dashboard ${name}` : "Dashboard";
}

/** Título tipo Analytics: "Analytics" ou "{nome} Analytics" */
export function getAnalyticsTitle(clientName: string | null | undefined): string {
  const name = getBrandDisplayName(clientName);
  return name ? `${name} Analytics` : "Analytics";
}

type BrandingState = {
  branding: ClientBrandingRow | null;
  isLoading: boolean;
  error: Error | null;
  brandName: string;
  logoUrl: string;
  faviconUrl: string | null;
  useCustomPalette: boolean;
  primaryColor: string | null;
  secondaryColor: string | null;
  tertiaryColor: string | null;
  refetch: () => void;
  applyBranding: (row: ClientBrandingRow | null) => void;
  saveBranding: (input: ClientBrandingInput) => Promise<ClientBrandingRow>;
  uploadLogo: (file: File) => Promise<string>;
  removeLogo: () => Promise<void>;
};

const BrandingContext = createContext<BrandingState | null>(null);

const DEFAULT_APPLE_TOUCH_ICON = "/icons/apple-touch-icon.png";

/** Atualiza favicon, apple-touch-icon e meta og/twitter image no head. Quando url é null, restaura padrões. */
function setIconsAndMetaInHead(url: string | null): void {
  const iconUrl = url || defaultLogoUrl;
  const appleTouchUrl = url || DEFAULT_APPLE_TOUCH_ICON;
  const ogImageUrl = url || defaultLogoUrl;

  let link = document.querySelector<HTMLLinkElement>('link[rel="icon"]');
  if (link) link.href = iconUrl;
  else {
    link = document.createElement("link");
    link.rel = "icon";
    link.href = iconUrl;
    document.head.appendChild(link);
  }
  document.querySelectorAll('link[rel="icon"]').forEach((el, i) => { if (i > 0) el.remove(); });

  let appleTouch = document.querySelector<HTMLLinkElement>('link[rel="apple-touch-icon"]');
  if (appleTouch) appleTouch.href = appleTouchUrl;
  else {
    appleTouch = document.createElement("link");
    appleTouch.rel = "apple-touch-icon";
    appleTouch.href = appleTouchUrl;
    document.head.appendChild(appleTouch);
  }

  const ogImage = document.querySelector('meta[property="og:image"]');
  if (ogImage) ogImage.setAttribute("content", ogImageUrl);
  const twitterImage = document.querySelector('meta[name="twitter:image"]');
  if (twitterImage) twitterImage.setAttribute("content", ogImageUrl);
}

const DEFAULT_MANIFEST_URL = "/manifest.webmanifest";
let manifestBlobUrl: string | null = null;

/** Atualiza o link do manifest PWA para usar a logo e o título customizados, ou restaura o estático. */
function setManifestInHead(iconUrl: string | null, title: string): void {
  let link = document.querySelector<HTMLLinkElement>('link[rel="manifest"]');
  if (!link) {
    link = document.createElement("link");
    link.rel = "manifest";
    document.head.appendChild(link);
  }

  if (manifestBlobUrl) {
    URL.revokeObjectURL(manifestBlobUrl);
    manifestBlobUrl = null;
  }

  if (!iconUrl) {
    link.href = DEFAULT_MANIFEST_URL;
    return;
  }

  const manifest = {
    name: title,
    short_name: title,
    start_url: "/",
    scope: "/",
    display: "standalone" as const,
    background_color: "#0F172C",
    theme_color: "#0F172C",
    description: `${title} - Gestão e acompanhamento`,
    icons: [
      { src: iconUrl, sizes: "192x192", type: "image/png", purpose: "any" as const },
      { src: iconUrl, sizes: "512x512", type: "image/png", purpose: "any" as const },
      { src: iconUrl, sizes: "192x192", type: "image/png", purpose: "maskable" as const },
      { src: iconUrl, sizes: "512x512", type: "image/png", purpose: "maskable" as const },
    ],
  };
  const blob = new Blob([JSON.stringify(manifest)], { type: "application/manifest+json" });
  manifestBlobUrl = URL.createObjectURL(blob);
  link.href = manifestBlobUrl;
}

const PALETTE_VARS = [
  "--primary", "--primary-foreground", "--primary-icon", "--accent", "--accent-foreground", "--tertiary", "--tertiary-foreground", "--ring",
  "--sidebar-primary", "--sidebar-primary-foreground", "--sidebar-ring",
  "--chart-1", "--chart-2", "--chart-3",
  "--background", "--foreground", "--card", "--card-foreground",
  "--muted", "--muted-foreground", "--border",
  "--sidebar-background", "--sidebar-foreground", "--sidebar-accent", "--sidebar-accent-foreground", "--sidebar-border",
];

function applyCustomPaletteToDocument(row: ClientBrandingRow | null): void {
  const root = document.documentElement;
  if (!row?.use_custom_palette || !row.primary_color) {
    PALETTE_VARS.forEach((key) => root.style.removeProperty(key));
    return;
  }
  const tokens = deriveBrandTokens(row.primary_color, row.secondary_color, row.tertiary_color);
  Object.entries(tokens).forEach(([key, value]) => root.style.setProperty(key, value, "important"));
}

function setDocumentTitle(clientName: string | null | undefined): void {
  const name = getBrandDisplayName(clientName);
  const title = name ? `Dashboard ${name}` : "Dashboard";
  document.title = title;
  const ogTitle = document.querySelector('meta[property="og:title"]');
  if (ogTitle) ogTitle.setAttribute("content", title);
  const twitterTitle = document.querySelector('meta[name="twitter:title"]');
  if (twitterTitle) twitterTitle.setAttribute("content", title);
  const appleTitle = document.querySelector('meta[name="apple-mobile-web-app-title"]');
  if (appleTitle) appleTitle.setAttribute("content", `${title} - Web`);
}

export function resetBrandingInDocument(): void {
  setIconsAndMetaInHead(null);
  setManifestInHead(null, "Dashboard");
  setDocumentTitle(null);
  applyCustomPaletteToDocument(null);
}

export function BrandingProvider({ children }: { children: React.ReactNode }) {
  const location = useLocation();
  const queryClient = useQueryClient();
  const { officeId, profile } = useProfile();
  const [logoUrl, setLogoUrl] = useState<string>(defaultLogoUrl);
  const [faviconUrl, setFaviconUrl] = useState<string | null>(null);
  const [hasSession, setHasSession] = useState(false);

  useEffect(() => {
    let mounted = true;

    supabase.auth.getSession().then(({ data }) => {
      queryClient.setQueryData(["auth-session"], data.session ?? null);
      if (mounted) setHasSession(Boolean(data.session));
    });

    const {
      data: { subscription },
    } = supabase.auth.onAuthStateChange((event, session) => {
      queryClient.setQueryData(["auth-session"], session ?? null);
      setHasSession(Boolean(session));
      // TOKEN_REFRESHED dispara muito com refresh/getUser e rebenta o Network (profiles, user, memberships).
      if (event === "TOKEN_REFRESHED") return;
      if (session?.user?.id) {
        queryClient.invalidateQueries({ queryKey: ["profile", session.user.id] });
      } else {
        queryClient.removeQueries({ queryKey: BRANDING_QUERY_KEY_PREFIX });
        queryClient.removeQueries({ queryKey: ["profile"] });
        queryClient.removeQueries({ queryKey: ["admin"] });
      }
    });

    return () => {
      mounted = false;
      subscription.unsubscribe();
    };
  }, [queryClient]);

  const shouldUseOfficeBranding = hasSession && location.pathname !== "/login";
  const brandingQueryKey = getBrandingQueryKey(officeId);

  const { data: brandingData = null, isLoading, error, refetch } = useQuery({
    queryKey: brandingQueryKey,
    queryFn: () => getBranding(officeId),
    enabled: shouldUseOfficeBranding && Boolean(officeId),
    staleTime: 5 * 60 * 1000,
  });

  const branding = useMemo<ClientBrandingRow | null>(() => {
    if (brandingData) return brandingData;
    if (!shouldUseOfficeBranding || !officeId || !profile?.office_name) return null;
    return {
      id: `fallback-${officeId}`,
      office_id: officeId,
      client_name: profile.office_name,
      primary_color: null,
      secondary_color: null,
      tertiary_color: null,
      logo_path: null,
      favicon_path: null,
      logo_url: null,
      favicon_url: null,
      use_custom_palette: false,
      use_custom_logo: false,
      use_custom_favicon: false,
      created_at: "",
      updated_at: "",
    };
  }, [brandingData, shouldUseOfficeBranding, officeId, profile?.office_name]);

  const applyBranding = useCallback((row: ClientBrandingRow | null) => {
    if (!row) {
      setLogoUrl(defaultLogoUrl);
      setFaviconUrl(null);
      resetBrandingInDocument();
      return;
    }
    const title = getBrandDisplayName(row.client_name) ? `Dashboard ${getBrandDisplayName(row.client_name)}` : "Dashboard";
    setDocumentTitle(row.client_name);
    if (row.use_custom_logo && row.logo_url) {
      const iconUrl = row.favicon_url || row.logo_url;
      setLogoUrl(row.logo_url);
      setFaviconUrl(iconUrl);
      setIconsAndMetaInHead(iconUrl);
      setManifestInHead(iconUrl, title);
    } else {
      setLogoUrl(defaultLogoUrl);
      setFaviconUrl(null);
      setIconsAndMetaInHead(null);
      setManifestInHead(null, title);
    }
    applyCustomPaletteToDocument(row);
  }, []);

  useEffect(() => {
    applyBranding(shouldUseOfficeBranding ? branding ?? null : null);
  }, [branding, applyBranding, shouldUseOfficeBranding]);

  useEffect(() => {
    const observer = new MutationObserver(() => {
      if (branding?.use_custom_palette && branding.primary_color) {
        applyCustomPaletteToDocument(branding);
      }
    });
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ["class"] });
    return () => observer.disconnect();
  }, [branding]);

  const saveBranding = useCallback(
    async (input: ClientBrandingInput): Promise<ClientBrandingRow> => {
      const row = await upsertBranding(input);
      queryClient.setQueryData(getBrandingQueryKey(row.office_id), row);
      applyBranding(row);
      return row;
    },
    [queryClient, applyBranding]
  );

  const uploadLogo = useCallback(
    async (file: File): Promise<string> => {
      const row = await uploadLogoAndFaviconService(file);
      queryClient.setQueryData(getBrandingQueryKey(row.office_id), row);
      applyBranding(row);
      return row.logo_url ?? "";
    },
    [queryClient, applyBranding]
  );

  const removeLogo = useCallback(async () => {
    const row = await removeLogoService();
    queryClient.setQueryData(getBrandingQueryKey(row.office_id), row);
    applyBranding(row);
  }, [queryClient, applyBranding]);

  const value = useMemo<BrandingState>(
    () => ({
      branding,
      isLoading,
      error: error instanceof Error ? error : null,
      brandName: getBrandDisplayName(branding?.client_name),
      logoUrl,
      faviconUrl,
      useCustomPalette: branding?.use_custom_palette ?? false,
      primaryColor: branding?.primary_color ?? null,
      secondaryColor: branding?.secondary_color ?? null,
      tertiaryColor: branding?.tertiary_color ?? null,
      refetch,
      applyBranding,
      saveBranding,
      uploadLogo,
      removeLogo,
    }),
    [
      branding,
      isLoading,
      error,
      logoUrl,
      faviconUrl,
      refetch,
      applyBranding,
      saveBranding,
      uploadLogo,
      removeLogo,
    ]
  );

  return <BrandingContext.Provider value={value}>{children}</BrandingContext.Provider>;
}

export function useBranding(): BrandingState {
  const ctx = useContext(BrandingContext);
  if (!ctx) throw new Error("useBranding must be used within BrandingProvider");
  return ctx;
}

export function useBrandingOptional(): BrandingState | null {
  return useContext(BrandingContext);
}
