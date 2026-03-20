import { supabase } from "@/services/supabaseClient"
import { getCurrentOfficeContext } from "./officeContextService"

const BRANDING_BUCKET = "branding-assets"

export type ClientBrandingRow = {
  id: string
  office_id: string
  client_name: string | null
  primary_color: string | null
  secondary_color: string | null
  tertiary_color: string | null
  logo_path: string | null
  favicon_path: string | null
  logo_url: string | null
  favicon_url: string | null
  use_custom_palette: boolean
  use_custom_logo: boolean
  use_custom_favicon: boolean
  created_at: string
  updated_at: string
}

export type ClientBrandingInput = {
  client_name?: string | null
  primary_color?: string | null
  secondary_color?: string | null
  tertiary_color?: string | null
  logo_path?: string | null
  favicon_path?: string | null
  use_custom_palette?: boolean
  use_custom_logo?: boolean
  use_custom_favicon?: boolean
}

type BrandingDbRow = {
  id: string
  office_id: string
  display_name: string | null
  primary_color: string | null
  secondary_color: string | null
  accent_color: string | null
  logo_path: string | null
  favicon_path: string | null
  use_custom_palette: boolean
  use_custom_logo: boolean
  use_custom_favicon: boolean
  created_at: string
  updated_at: string
}

function getExtension(filename: string): string {
  const i = filename.lastIndexOf(".")
  return i >= 0 ? filename.slice(i + 1).toLowerCase() : ""
}

const LOGO_ACCEPT = ["png", "svg", "jpg", "jpeg", "webp", "ico"]
const FAVICON_ACCEPT = ["png", "ico", "svg", "jpg", "jpeg", "webp"]
const MAX_LOGO_BYTES = 2 * 1024 * 1024
const MAX_FAVICON_BYTES = 512 * 1024

export function validateLogoFile(file: File): { ok: true } | { ok: false; error: string } {
  const ext = getExtension(file.name)
  if (!LOGO_ACCEPT.includes(ext)) {
    return { ok: false, error: `Formato não permitido. Use: ${LOGO_ACCEPT.join(", ")}` }
  }
  if (file.size > MAX_LOGO_BYTES) {
    return { ok: false, error: `Arquivo muito grande. Máximo ${MAX_LOGO_BYTES / 1024 / 1024} MB` }
  }
  return { ok: true }
}

export function validateFaviconFile(file: File): { ok: true } | { ok: false; error: string } {
  const ext = getExtension(file.name)
  if (!FAVICON_ACCEPT.includes(ext)) {
    return { ok: false, error: `Formato não permitido. Use: ${FAVICON_ACCEPT.join(", ")}` }
  }
  if (file.size > MAX_FAVICON_BYTES) {
    return { ok: false, error: `Arquivo muito grande. Máximo ${MAX_FAVICON_BYTES / 1024} KB` }
  }
  return { ok: true }
}

async function createSignedUrl(path: string | null): Promise<string | null> {
  if (!path) return null
  const { data, error } = await supabase.storage.from(BRANDING_BUCKET).createSignedUrl(path, 60 * 60)
  if (error) return null
  return data?.signedUrl ?? null
}

async function getCurrentOfficeId(providedOfficeId?: string | null): Promise<string> {
  if (providedOfficeId) return providedOfficeId
  const context = await getCurrentOfficeContext()
  if (!context?.officeId) throw new Error("Nenhum escritório ativo encontrado para o usuário.")
  return context.officeId
}

function mapBrandingRow(
  row: BrandingDbRow,
  signed: { logoUrl: string | null; faviconUrl: string | null }
): ClientBrandingRow {
  return {
    id: row.id,
    office_id: row.office_id,
    client_name: row.display_name,
    primary_color: row.primary_color,
    secondary_color: row.secondary_color,
    tertiary_color: row.accent_color,
    logo_path: row.logo_path,
    favicon_path: row.favicon_path,
    logo_url: signed.logoUrl,
    favicon_url: signed.faviconUrl,
    use_custom_palette: row.use_custom_palette,
    use_custom_logo: row.use_custom_logo,
    use_custom_favicon: row.use_custom_favicon,
    created_at: row.created_at,
    updated_at: row.updated_at,
  }
}

function hasOwn<T extends object>(value: T, key: keyof ClientBrandingInput): boolean {
  return Object.prototype.hasOwnProperty.call(value, key)
}

async function getBrandingRecord(officeId: string): Promise<BrandingDbRow | null> {
  const { data, error } = await supabase
    .from("office_branding")
    .select("*")
    .eq("office_id", officeId)
    .maybeSingle()

  if (error) throw error
  return data
}

async function deleteBrandAssets(paths: Array<string | null | undefined>): Promise<void> {
  const uniquePaths = Array.from(new Set(paths.filter((path): path is string => Boolean(path))))
  if (uniquePaths.length === 0) return

  const { error } = await supabase.storage.from(BRANDING_BUCKET).remove(uniquePaths)
  if (error) throw error
}

export async function getBranding(officeId?: string | null): Promise<ClientBrandingRow | null> {
  const resolvedOfficeId = await getCurrentOfficeId(officeId)
  const data = await getBrandingRecord(resolvedOfficeId)
  if (!data) return null

  const [logoUrl, faviconUrl] = await Promise.all([
    createSignedUrl(data.logo_path),
    createSignedUrl(data.favicon_path || data.logo_path),
  ])

  return mapBrandingRow(data, { logoUrl, faviconUrl })
}

export async function upsertBranding(input: ClientBrandingInput): Promise<ClientBrandingRow> {
  const officeId = await getCurrentOfficeId()
  const current = await getBrandingRecord(officeId)
  const nextLogoPath = hasOwn(input, "logo_path") ? input.logo_path ?? null : current?.logo_path ?? null
  const nextFaviconPath = hasOwn(input, "favicon_path") ? input.favicon_path ?? null : current?.favicon_path ?? null

  const { data, error } = await supabase
    .from("office_branding")
    .upsert(
      {
        office_id: officeId,
        display_name: hasOwn(input, "client_name") ? input.client_name ?? null : current?.display_name ?? null,
        primary_color: hasOwn(input, "primary_color") ? input.primary_color ?? null : current?.primary_color ?? null,
        secondary_color: hasOwn(input, "secondary_color") ? input.secondary_color ?? null : current?.secondary_color ?? null,
        accent_color: hasOwn(input, "tertiary_color") ? input.tertiary_color ?? null : current?.accent_color ?? null,
        logo_path: nextLogoPath,
        favicon_path: nextFaviconPath,
        use_custom_palette: hasOwn(input, "use_custom_palette") ? input.use_custom_palette ?? false : current?.use_custom_palette ?? false,
        use_custom_logo: hasOwn(input, "use_custom_logo") ? input.use_custom_logo ?? false : current?.use_custom_logo ?? false,
        use_custom_favicon: hasOwn(input, "use_custom_favicon") ? input.use_custom_favicon ?? false : current?.use_custom_favicon ?? false,
        updated_at: new Date().toISOString(),
      },
      { onConflict: "office_id" }
    )
    .select()
    .single()

  if (error) throw error

  const stalePaths = [current?.logo_path, current?.favicon_path].filter(
    (path): path is string => Boolean(path) && path !== nextLogoPath && path !== nextFaviconPath
  )
  await deleteBrandAssets(stalePaths)

  const [logoUrl, faviconUrl] = await Promise.all([
    createSignedUrl(data.logo_path),
    createSignedUrl(data.favicon_path || data.logo_path),
  ])
  return mapBrandingRow(data, { logoUrl, faviconUrl })
}

async function uploadBrandAsset(file: File, folder: "logo" | "favicon"): Promise<string> {
  const officeId = await getCurrentOfficeId()
  const ext = getExtension(file.name)
  const path = `${officeId}/${folder}/current.${ext}`
  const { error } = await supabase.storage.from(BRANDING_BUCKET).upload(path, file, {
    upsert: true,
    contentType: file.type || (ext === "svg" ? "image/svg+xml" : ext === "ico" ? "image/x-icon" : "image/png"),
  })
  if (error) throw error
  return path
}

export async function uploadLogoAndFaviconAsset(file: File): Promise<{ logo_path: string; favicon_path: string }> {
  const valid = validateLogoFile(file)
  if (!valid.ok) throw new Error(valid.error)
  const path = await uploadBrandAsset(file, "logo")
  return {
    logo_path: path,
    favicon_path: path,
  }
}

export async function uploadLogo(file: File): Promise<ClientBrandingRow> {
  const valid = validateLogoFile(file)
  if (!valid.ok) throw new Error(valid.error)
  const path = await uploadBrandAsset(file, "logo")
  return upsertBranding({
    logo_path: path,
    use_custom_logo: true,
  })
}

export async function uploadLogoAndFavicon(file: File): Promise<ClientBrandingRow> {
  const valid = validateLogoFile(file)
  if (!valid.ok) throw new Error(valid.error)
  const path = await uploadBrandAsset(file, "logo")
  return upsertBranding({
    logo_path: path,
    favicon_path: path,
    use_custom_logo: true,
    use_custom_favicon: true,
  })
}

export async function uploadFavicon(file: File): Promise<ClientBrandingRow> {
  const valid = validateFaviconFile(file)
  if (!valid.ok) throw new Error(valid.error)
  const path = await uploadBrandAsset(file, "favicon")
  return upsertBranding({
    favicon_path: path,
    use_custom_favicon: true,
  })
}

export async function removeLogo(): Promise<ClientBrandingRow> {
  return upsertBranding({
    logo_path: null,
    favicon_path: null,
    use_custom_logo: false,
    use_custom_favicon: false,
  })
}

export async function removeFavicon(): Promise<ClientBrandingRow> {
  return upsertBranding({
    favicon_path: null,
    use_custom_favicon: false,
  })
}
