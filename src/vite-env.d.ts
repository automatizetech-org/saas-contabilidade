/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly SUPABASE_URL: string
  readonly SUPABASE_ANON_KEY: string
  readonly SUPABASE_PUBLISHABLE_KEY?: string
  readonly SERVER_API_URL?: string
  readonly WHATSAPP_API?: string
}

interface ImportMeta {
  readonly env: ImportMetaEnv
}

declare module "*.png" {
  const src: string
  export default src
}
