import { supabase } from "@/services/supabaseClient";
import { BRANDING_CLIENT_ID } from "@/lib/brandingTheme";

const BRANDING_BUCKET = "branding-assets";
const LOGO_PATH = (clientId: string) => `${clientId}/logo`;
const FAVICON_PATH = (clientId: string) => `${clientId}/favicon`;

export type ClientBrandingRow = {
  id: string;
  client_id: string;
  client_name: string | null;
  primary_color: string | null;
  secondary_color: string | null;
  tertiary_color: string | null;
  logo_url: string | null;
  favicon_url: string | null;
  use_custom_palette: boolean;
  use_custom_logo: boolean;
  use_custom_favicon: boolean;
  created_at: string;
  updated_at: string;
};

export type ClientBrandingInput = {
  client_name?: string | null;
  primary_color?: string | null;
  secondary_color?: string | null;
  tertiary_color?: string | null;
  logo_url?: string | null;
  favicon_url?: string | null;
  use_custom_palette?: boolean;
  use_custom_logo?: boolean;
  use_custom_favicon?: boolean;
};

export async function getBranding(clientId: string = BRANDING_CLIENT_ID): Promise<ClientBrandingRow | null> {
  const { data, error } = await supabase
    .from("client_branding_settings")
    .select("*")
    .eq("client_id", clientId)
    .maybeSingle();
  if (error) throw error;
  return data as ClientBrandingRow | null;
}

export async function upsertBranding(
  input: ClientBrandingInput,
  clientId: string = BRANDING_CLIENT_ID
): Promise<ClientBrandingRow> {
  const { data, error } = await supabase
    .from("client_branding_settings")
    .upsert(
      {
        client_id: clientId,
        ...input,
        updated_at: new Date().toISOString(),
      },
      { onConflict: "client_id" }
    )
    .select()
    .single();
  if (error) throw error;
  return data as ClientBrandingRow;
}

function getExtension(filename: string): string {
  const i = filename.lastIndexOf(".");
  return i >= 0 ? filename.slice(i + 1).toLowerCase() : "";
}

const LOGO_ACCEPT = ["png", "svg", "jpg", "jpeg", "webp", "ico"];
const FAVICON_ACCEPT = ["png", "ico", "svg", "jpg", "jpeg", "webp"];
const MAX_LOGO_BYTES = 2 * 1024 * 1024; // 2MB
const MAX_FAVICON_BYTES = 512 * 1024;   // 512KB

export function validateLogoFile(file: File): { ok: true } | { ok: false; error: string } {
  const ext = getExtension(file.name);
  if (!LOGO_ACCEPT.includes(ext)) {
    return { ok: false, error: `Formato não permitido. Use: ${LOGO_ACCEPT.join(", ")}` };
  }
  if (file.size > MAX_LOGO_BYTES) {
    return { ok: false, error: `Arquivo muito grande. Máximo ${MAX_LOGO_BYTES / 1024 / 1024} MB` };
  }
  return { ok: true };
}

export function validateFaviconFile(file: File): { ok: true } | { ok: false; error: string } {
  const ext = getExtension(file.name);
  if (!FAVICON_ACCEPT.includes(ext)) {
    return { ok: false, error: `Formato não permitido. Use: ${FAVICON_ACCEPT.join(", ")}` };
  }
  if (file.size > MAX_FAVICON_BYTES) {
    return { ok: false, error: `Arquivo muito grande. Máximo ${MAX_FAVICON_BYTES / 1024} KB` };
  }
  return { ok: true };
}

/** Faz upload da logo e retorna a URL pública. Substitui arquivo existente. */
export async function uploadLogo(file: File, clientId: string = BRANDING_CLIENT_ID): Promise<string> {
  const valid = validateLogoFile(file);
  if (!valid.ok) throw new Error(valid.error);
  const ext = getExtension(file.name);
  const path = `${LOGO_PATH(clientId)}/logo_${Date.now()}.${ext}`;
  const { error: uploadError } = await supabase.storage.from(BRANDING_BUCKET).upload(path, file, {
    upsert: true,
    contentType: file.type || (ext === "svg" ? "image/svg+xml" : ext === "ico" ? "image/x-icon" : "image/png"),
  });
  if (uploadError) throw uploadError;
  const { data: urlData } = supabase.storage.from(BRANDING_BUCKET).getPublicUrl(path);
  return urlData.publicUrl;
}

/** Faz upload da logo e usa a mesma URL para logo e favicon (ícone do site). */
export async function uploadLogoAndFavicon(file: File, clientId: string = BRANDING_CLIENT_ID): Promise<string> {
  const valid = validateLogoFile(file);
  if (!valid.ok) throw new Error(valid.error);
  const ext = getExtension(file.name);
  const path = `${LOGO_PATH(clientId)}/logo_${Date.now()}.${ext}`;
  const { error: uploadError } = await supabase.storage.from(BRANDING_BUCKET).upload(path, file, {
    upsert: true,
    contentType: file.type || (ext === "svg" ? "image/svg+xml" : ext === "ico" ? "image/x-icon" : "image/png"),
  });
  if (uploadError) throw uploadError;
  const { data: urlData } = supabase.storage.from(BRANDING_BUCKET).getPublicUrl(path);
  return urlData.publicUrl;
}

/** Faz upload do favicon e retorna a URL pública. (Mantido para compatibilidade; preferir uploadLogoAndFavicon.) */
export async function uploadFavicon(file: File, clientId: string = BRANDING_CLIENT_ID): Promise<string> {
  return uploadLogoAndFavicon(file, clientId);
}

/** Remove logo do storage e limpa logo_url e favicon_url no banco (mesmo arquivo para ambos). */
export async function removeLogo(clientId: string = BRANDING_CLIENT_ID): Promise<void> {
  const prefix = `${clientId}/logo/`;
  const { data: list } = await supabase.storage.from(BRANDING_BUCKET).list(clientId);
  const logoFolder = list?.find((e) => e.name === "logo");
  if (logoFolder) {
    const { data: files } = await supabase.storage.from(BRANDING_BUCKET).list(`${clientId}/logo`);
    if (files?.length) {
      await supabase.storage.from(BRANDING_BUCKET).remove(files.map((f) => `${clientId}/logo/${f.name}`));
    }
  }
  await upsertBranding(
    { use_custom_logo: false, use_custom_favicon: false, logo_url: null, favicon_url: null },
    clientId
  );
}

/** Remove favicon do storage e limpa favicon_url no banco. (Chama removeLogo que limpa logo+favicon.) */
export async function removeFavicon(clientId: string = BRANDING_CLIENT_ID): Promise<void> {
  await removeLogo(clientId);
}
