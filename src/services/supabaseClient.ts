import { createClient } from "@supabase/supabase-js"
import type { Database } from "@/types/database"

const SUPABASE_URL = import.meta.env.SUPABASE_URL
const SUPABASE_ANON_KEY =
  import.meta.env.SUPABASE_ANON_KEY ??
  import.meta.env.SUPABASE_PUBLISHABLE_KEY

const hasConfig = Boolean(SUPABASE_URL && SUPABASE_ANON_KEY)

// Avisa só em caso de configuração incompleta (uma variável definida e outra não)
if (import.meta.env.DEV && (SUPABASE_URL || SUPABASE_ANON_KEY) && !hasConfig) {
  console.warn(
    "Supabase: defina SUPABASE_URL e SUPABASE_ANON_KEY no .env (copie de .env.example)."
  )
}

// Em DEV sem .env, usa placeholders para o app montar; o login falhará até configurar o .env
const url = hasConfig ? SUPABASE_URL! : "https://placeholder.supabase.co"
const key = hasConfig ? SUPABASE_ANON_KEY! : "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.placeholder"

export const supabase = createClient<Database>(url, key, {
  auth: {
    storage: localStorage,
    persistSession: true,
    autoRefreshToken: true,
  },
})
