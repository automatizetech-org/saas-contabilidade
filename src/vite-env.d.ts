/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly SUPABASE_URL: string
  readonly SUPABASE_ANON_KEY: string
  readonly SUPABASE_PUBLISHABLE_KEY?: string
  readonly SERVER_API_URL?: string
  readonly WHATSAPP_API?: string
  /** "false" desliga o uso do Edge office-server para WhatsApp (usa só WHATSAPP_API). */
  readonly VITE_WHATSAPP_VIA_OFFICE_SERVER?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}

declare module "*.png" {
  const src: string
  export default src
}
