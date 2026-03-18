import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";
import fs from "fs";
import { componentTagger } from "lovable-tagger";

const projectRoot = path.resolve(__dirname, "..");

// Carrega .env na raiz (localhost / dev): .env, .env.local, .env.development
function loadEnvFromRoot(mode: string): Record<string, string> {
  const env: Record<string, string> = {};
  const envPath = path.join(projectRoot, ".env");
  if (fs.existsSync(envPath)) {
    const content = fs.readFileSync(envPath, "utf-8");
    for (const line of content.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith("#")) continue;
      const m = trimmed.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
      if (m) env[m[1]] = m[2].replace(/^["']|["']$/g, "").trim();
    }
  }
  // Sobrescreve com .env.local e .env.development se existirem (prioridade maior)
  const localPath = path.join(projectRoot, ".env.local");
  const devPath = path.join(projectRoot, `.env.${mode}`);
  for (const p of [devPath, localPath]) {
    if (fs.existsSync(p)) {
      const content = fs.readFileSync(p, "utf-8");
      for (const line of content.split("\n")) {
        const trimmed = line.trim();
        if (!trimmed || trimmed.startsWith("#")) continue;
        const m = trimmed.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
        if (m) env[m[1]] = m[2].replace(/^["']|["']$/g, "").trim();
      }
    }
  }
  return env;
}

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  const isDev = mode === "development";

  // Localhost: usa o .env da raiz do projeto (.env, .env.local, .env.development)
  // Nuvem (Vercel): usa as variáveis injetadas pela Vercel (Environment Variables no dashboard)
  const envFromFile = loadEnvFromRoot(mode);
  const SUPABASE_URL = isDev ? (envFromFile.SUPABASE_URL ?? process.env.SUPABASE_URL ?? "") : (process.env.SUPABASE_URL ?? envFromFile.SUPABASE_URL ?? "");
  const SUPABASE_ANON_KEY = isDev ? (envFromFile.SUPABASE_ANON_KEY ?? envFromFile.SUPABASE_PUBLISHABLE_KEY ?? process.env.SUPABASE_ANON_KEY ?? process.env.SUPABASE_PUBLISHABLE_KEY ?? "") : (process.env.SUPABASE_ANON_KEY ?? process.env.SUPABASE_PUBLISHABLE_KEY ?? envFromFile.SUPABASE_ANON_KEY ?? envFromFile.SUPABASE_PUBLISHABLE_KEY ?? "");
  const SUPABASE_PUBLISHABLE_KEY = isDev ? (envFromFile.SUPABASE_PUBLISHABLE_KEY ?? process.env.SUPABASE_PUBLISHABLE_KEY ?? "") : (process.env.SUPABASE_PUBLISHABLE_KEY ?? envFromFile.SUPABASE_PUBLISHABLE_KEY ?? "");
  const SERVER_API_URL = isDev ? (envFromFile.SERVER_API_URL ?? process.env.SERVER_API_URL ?? "") : (process.env.SERVER_API_URL ?? envFromFile.SERVER_API_URL ?? "");
  const WHATSAPP_API = isDev ? (envFromFile.WHATSAPP_API ?? process.env.WHATSAPP_API ?? "") : (process.env.WHATSAPP_API ?? envFromFile.WHATSAPP_API ?? "");

  if (!isDev) {
    const missingVars = [
      !SUPABASE_URL ? "SUPABASE_URL" : null,
      !SUPABASE_ANON_KEY ? "SUPABASE_ANON_KEY" : null,
    ].filter(Boolean);

    if (missingVars.length > 0) {
      throw new Error(
        `Build de produção bloqueado. Defina no ambiente: ${missingVars.join(", ")}.`
      );
    }
  }

  return {
  root: projectRoot,
  envPrefix: "VITE_",
  define: {
    "import.meta.env.SUPABASE_URL": JSON.stringify(SUPABASE_URL),
    "import.meta.env.SUPABASE_ANON_KEY": JSON.stringify(SUPABASE_ANON_KEY),
    "import.meta.env.SUPABASE_PUBLISHABLE_KEY": JSON.stringify(SUPABASE_PUBLISHABLE_KEY),
    "import.meta.env.SERVER_API_URL": JSON.stringify(SERVER_API_URL),
    "import.meta.env.WHATSAPP_API": JSON.stringify(WHATSAPP_API),
  },
  build: {
    esbuild: mode === "production" ? { drop: ["console", "debugger"] } : undefined,
    sourcemap: mode !== "production",
  },
  css: {
    postcss: path.resolve(projectRoot, "config/postcss.config.js"),
  },
  server: {
    host: true,
    port: 8080,
    hmr: {
      overlay: false,
    },
  },
  plugins: [react(), mode === "development" && componentTagger()].filter(Boolean),
  resolve: {
    alias: {
      "@": path.resolve(projectRoot, "src"),
    },
  },
  optimizeDeps: {
    include: ["pdfjs-dist"],
  },
};
});
