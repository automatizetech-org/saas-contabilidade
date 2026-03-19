import { useEffect, useState } from "react"

const PROBE_INTERVAL_MS = 30_000
const PROBE_TIMEOUT_MS = 5_000

const SUPABASE_URL = import.meta.env.SUPABASE_URL
const SUPABASE_ANON_KEY =
  import.meta.env.SUPABASE_ANON_KEY ??
  import.meta.env.SUPABASE_PUBLISHABLE_KEY

function getProbeUrl() {
  if (!SUPABASE_URL) return null

  try {
    return new URL("/auth/v1/health", SUPABASE_URL).toString()
  } catch {
    return null
  }
}

export function useSupabaseConnectionStatus() {
  const [isUnavailable, setIsUnavailable] = useState(false)

  useEffect(() => {
    const probeUrl = getProbeUrl()
    if (!probeUrl) {
      setIsUnavailable(false)
      return
    }

    let active = true
    let timeoutId: ReturnType<typeof setTimeout> | null = null

    const probe = async () => {
      if (!navigator.onLine) {
        if (active) setIsUnavailable(true)
        return
      }

      const controller = new AbortController()
      timeoutId = setTimeout(() => controller.abort(), PROBE_TIMEOUT_MS)

      try {
        await fetch(probeUrl, {
          method: "GET",
          headers: SUPABASE_ANON_KEY ? { apikey: SUPABASE_ANON_KEY } : undefined,
          cache: "no-store",
          signal: controller.signal,
        })

        if (active) setIsUnavailable(false)
      } catch {
        if (active) setIsUnavailable(true)
      } finally {
        if (timeoutId) {
          clearTimeout(timeoutId)
          timeoutId = null
        }
      }
    }

    void probe()

    const intervalId = window.setInterval(() => {
      void probe()
    }, PROBE_INTERVAL_MS)

    const handleOnline = () => {
      void probe()
    }

    const handleOffline = () => {
      setIsUnavailable(true)
    }

    window.addEventListener("online", handleOnline)
    window.addEventListener("offline", handleOffline)

    return () => {
      active = false

      if (timeoutId) clearTimeout(timeoutId)

      window.clearInterval(intervalId)
      window.removeEventListener("online", handleOnline)
      window.removeEventListener("offline", handleOffline)
    }
  }, [])

  return isUnavailable
}
