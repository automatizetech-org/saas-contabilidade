/**
 * Utilitários para tema de branding: conversão hex→HSL, derivação de tokens e contraste.
 * As variáveis CSS do projeto usam valores HSL sem "hsl()" (ex: "221 83% 53%").
 */

export const BRANDING_CLIENT_ID = "default";

const HEX_REGEX = /^#?([a-fA-F0-9]{6}|[a-fA-F0-9]{3})$/;

export function parseHex(hex: string): { r: number; g: number; b: number } | null {
  const m = hex.replace(/^#/, "").match(HEX_REGEX);
  if (!m) return null;
  let s = m[1];
  if (s.length === 3) {
    s = s[0] + s[0] + s[1] + s[1] + s[2] + s[2];
  }
  const n = parseInt(s, 16);
  return {
    r: (n >> 16) & 0xff,
    g: (n >> 8) & 0xff,
    b: n & 0xff,
  };
}

export function hexToHsl(hex: string): { h: number; s: number; l: number } | null {
  const rgb = parseHex(hex);
  if (!rgb) return null;
  const r = rgb.r / 255;
  const g = rgb.g / 255;
  const b = rgb.b / 255;
  const max = Math.max(r, g, b);
  const min = Math.min(r, g, b);
  let h = 0;
  let s = 0;
  const l = (max + min) / 2;
  if (max !== min) {
    const d = max - min;
    s = l > 0.5 ? d / (2 - max - min) : d / (max + min);
    switch (max) {
      case r:
        h = ((g - b) / d + (g < b ? 6 : 0)) / 6;
        break;
      case g:
        h = ((b - r) / d + 2) / 6;
        break;
      default:
        h = ((r - g) / d + 4) / 6;
    }
  }
  return {
    h: h * 360,
    s: s * 100,
    l: l * 100,
  };
}

/** Formato usado no CSS: "H S% L%" (sem "hsl()"). Precisão de 2 decimais para cor exata. */
export function hslToCssValue(h: number, s: number, l: number): string {
  return `${Number(h.toFixed(2))} ${Number(s.toFixed(2))}% ${Number(l.toFixed(2))}%`;
}

export function hexToCssHsl(hex: string): string | null {
  const hsl = hexToHsl(hex);
  if (!hsl) return null;
  return hslToCssValue(hsl.h, hsl.s, hsl.l);
}

/** Valida hex (com ou sem #). */
export function isValidHex(hex: string): boolean {
  return HEX_REGEX.test(hex.replace(/^#/, ""));
}

/** Normaliza para #RRGGBB. */
export function normalizeHex(hex: string): string {
  let s = hex.replace(/^#/, "");
  if (s.length === 3) s = s[0] + s[0] + s[1] + s[1] + s[2] + s[2];
  return "#" + s.toLowerCase();
}

/** Luminância relativa (0–1). > 0.6 = cor clara. */
export function luminance(hex: string): number {
  const rgb = parseHex(hex);
  if (!rgb) return 0;
  const [r, g, b] = [rgb.r, rgb.g, rgb.b].map((c) => {
    const v = c / 255;
    return v <= 0.03928 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4);
  });
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

/** Contraste relativo (WCAG). 4.5+ para texto normal, 3+ para texto grande. */
export function contrastRatio(hexForeground: string, hexBackground: string): number {
  const L1 = luminance(hexForeground);
  const L2 = luminance(hexBackground);
  const lighter = Math.max(L1, L2);
  const darker = Math.min(L1, L2);
  return (lighter + 0.05) / (darker + 0.05);
}

/** Retorna true se o contraste for adequado para texto (>= 4.5). */
export function hasGoodContrast(foreground: string, background: string): boolean {
  return contrastRatio(foreground, background) >= 4.5;
}

/** Escurece um hex por um fator (0–1). */
export function darkenHex(hex: string, factor: number): string {
  const rgb = parseHex(hex);
  if (!rgb) return hex;
  const r = Math.round(rgb.r * (1 - factor));
  const g = Math.round(rgb.g * (1 - factor));
  const b = Math.round(rgb.b * (1 - factor));
  return "#" + [r, g, b].map((c) => Math.max(0, c).toString(16).padStart(2, "0")).join("");
}

/** Clareia um hex por um fator (0–1). */
export function lightenHex(hex: string, factor: number): string {
  const rgb = parseHex(hex);
  if (!rgb) return hex;
  const r = Math.round(rgb.r + (255 - rgb.r) * factor);
  const g = Math.round(rgb.g + (255 - rgb.g) * factor);
  const b = Math.round(rgb.b + (255 - rgb.b) * factor);
  return "#" + [r, g, b].map((c) => Math.min(255, c).toString(16).padStart(2, "0")).join("");
}

/** Texto escuro para fundo claro. */
const FOREGROUND_ON_LIGHT = "222 47% 11%";
/** Texto claro para fundo escuro. */
const FOREGROUND_ON_DARK = "210 40% 98%";

/** Retorna branco ou preto para contraste sobre a cor (HSL string). */
function contrastOnColor(hex: string): string {
  return luminance(hex) > 0.6 ? FOREGROUND_ON_LIGHT : FOREGROUND_ON_DARK;
}

/**
 * Gera tokens de marca (primária, secundária, terciária). A cor primária fica exatamente como o usuário definiu.
 * primary-foreground e primary-icon: branco ou preto para contraste.
 * Se a primária for clara (ex.: #57a0ff), aplica também fundos claros para a interface não ficar escura.
 */
export function deriveBrandTokens(primary: string, secondary: string | null, tertiary: string | null): Record<string, string> {
  const primaryNorm = normalizeHex(primary.trim());
  const p = hexToHsl(primaryNorm);
  if (!p) return {};
  const s = secondary && secondary.trim() ? hexToHsl(normalizeHex(secondary.trim())) : null;
  const t = tertiary && tertiary.trim() ? hexToHsl(normalizeHex(tertiary.trim())) : null;
  const primaryHsl = hslToCssValue(p.h, p.s, p.l);
  const accentHsl = s ? hslToCssValue(s.h, s.s, s.l) : primaryHsl;
  const tertiaryHsl = t ? hslToCssValue(t.h, t.s, t.l) : accentHsl;
  const primaryIsLight = luminance(primaryNorm) > 0.6;
  const textOnPrimary = primaryIsLight ? FOREGROUND_ON_LIGHT : FOREGROUND_ON_DARK;
  const textOnAccent = s && secondary ? contrastOnColor(normalizeHex(secondary.trim())) : textOnPrimary;
  const primaryIcon = primaryIsLight ? FOREGROUND_ON_LIGHT : primaryHsl;
  const ringHsl = primaryIcon;

  const base: Record<string, string> = {
    "--primary": primaryHsl,
    "--primary-foreground": textOnPrimary,
    "--primary-icon": primaryIcon,
    "--accent": accentHsl,
    "--accent-foreground": textOnAccent,
    "--ring": ringHsl,
    "--sidebar-primary": primaryHsl,
    "--sidebar-primary-foreground": textOnPrimary,
    "--sidebar-ring": primaryHsl,
    "--chart-1": primaryHsl,
    "--chart-2": accentHsl,
    "--chart-3": tertiaryHsl,
  };

  if (primaryIsLight) {
    base["--background"] = "210 20% 98%";
    base["--foreground"] = FOREGROUND_ON_LIGHT;
    base["--card"] = "0 0% 100%";
    base["--card-foreground"] = FOREGROUND_ON_LIGHT;
    base["--muted"] = "210 40% 96%";
    base["--muted-foreground"] = "215 16% 47%";
    base["--border"] = "214 32% 91%";
    base["--sidebar-background"] = "0 0% 100%";
    base["--sidebar-foreground"] = FOREGROUND_ON_LIGHT;
    base["--sidebar-accent"] = "210 40% 96%";
    base["--sidebar-accent-foreground"] = FOREGROUND_ON_LIGHT;
    base["--sidebar-border"] = "214 32% 91%";
  }

  return base;
}

/** @deprecated Use deriveBrandTokens. Mantido por compatibilidade. */
export function deriveLightTokens(primary: string, secondary: string | null, tertiary: string | null): Record<string, string> {
  return deriveBrandTokens(primary, secondary, tertiary);
}

/** @deprecated Use deriveBrandTokens. Mantido por compatibilidade. */
export function deriveDarkTokens(primary: string, secondary: string | null, tertiary: string | null): Record<string, string> {
  return deriveBrandTokens(primary, secondary, tertiary);
}
