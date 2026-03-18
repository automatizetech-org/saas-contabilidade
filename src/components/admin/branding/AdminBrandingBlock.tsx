import { useState, useCallback, useEffect } from "react";
import { useBranding } from "@/contexts/BrandingContext";
import { BrandingSettingsCard } from "./BrandingSettingsCard";
import { ColorPaletteEditor, DEFAULT_PRIMARY, DEFAULT_SECONDARY, DEFAULT_TERTIARY } from "./ColorPaletteEditor";
import { LogoUploadField } from "./LogoUploadField";
import { ThemePreviewPanel } from "./ThemePreviewPanel";
import { RestoreDefaultButton } from "./RestoreDefaultButton";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Loader2, Palette } from "lucide-react";
import { toast } from "sonner";
import { normalizeHex, isValidHex } from "@/lib/brandingTheme";
import { uploadLogoAndFaviconAsset } from "@/services/brandingService";

export function AdminBrandingBlock() {
  const {
    branding,
    isLoading: brandingLoading,
    primaryColor,
    secondaryColor,
    tertiaryColor,
    saveBranding,
    refetch,
  } = useBranding();

  const [paletteOn, setPaletteOn] = useState(!!branding?.use_custom_palette);
  const [primary, setPrimary] = useState(primaryColor ?? DEFAULT_PRIMARY);
  const [secondary, setSecondary] = useState(secondaryColor ?? DEFAULT_SECONDARY);
  const [tertiary, setTertiary] = useState(tertiaryColor ?? DEFAULT_TERTIARY);
  const [clientName, setClientName] = useState(branding?.client_name ?? "");
  const [saving, setSaving] = useState(false);
  const [pendingLogoFile, setPendingLogoFile] = useState<File | null>(null);
  const [pendingLogoPreviewUrl, setPendingLogoPreviewUrl] = useState<string | null>(null);
  const [removeLogoOnSave, setRemoveLogoOnSave] = useState(false);

  const clearPendingLogo = useCallback(() => {
    setPendingLogoFile(null);
    setPendingLogoPreviewUrl((current) => {
      if (current) URL.revokeObjectURL(current);
      return null;
    });
  }, []);

  useEffect(() => {
    setPaletteOn(!!branding?.use_custom_palette);
    setPrimary(primaryColor ?? DEFAULT_PRIMARY);
    setSecondary(secondaryColor ?? DEFAULT_SECONDARY);
    setTertiary(tertiaryColor ?? DEFAULT_TERTIARY);
    setClientName(branding?.client_name ?? "");
    setRemoveLogoOnSave(false);
    clearPendingLogo();
  }, [branding?.updated_at, branding?.use_custom_palette, branding?.client_name, primaryColor, secondaryColor, tertiaryColor, clearPendingLogo]);

  useEffect(() => {
    return () => {
      if (pendingLogoPreviewUrl) URL.revokeObjectURL(pendingLogoPreviewUrl);
    };
  }, [pendingLogoPreviewUrl]);

  const hasPersistedLogo = !!branding?.use_custom_logo && !!branding?.logo_url;
  const displayedLogoUrl = removeLogoOnSave ? pendingLogoPreviewUrl : pendingLogoPreviewUrl ?? (hasPersistedLogo ? branding?.logo_url ?? null : null);

  const handlePaletteChange = useCallback((p: string, s: string, t: string) => {
    setPrimary(p);
    setSecondary(s);
    setTertiary(t);
  }, []);

  const handlePaletteToggle = useCallback((checked: boolean) => {
    setPaletteOn(checked);
    if (!checked) {
      setPrimary(DEFAULT_PRIMARY);
      setSecondary(DEFAULT_SECONDARY);
      setTertiary(DEFAULT_TERTIARY);
    }
  }, []);

  const handleRestorePalette = useCallback(() => {
    setPaletteOn(false);
    setPrimary(DEFAULT_PRIMARY);
    setSecondary(DEFAULT_SECONDARY);
    setTertiary(DEFAULT_TERTIARY);
  }, []);

  const handleLogoUpload = useCallback(async (file: File) => {
    clearPendingLogo();
    const previewUrl = URL.createObjectURL(file);
    setPendingLogoFile(file);
    setPendingLogoPreviewUrl(previewUrl);
    setRemoveLogoOnSave(false);
    toast.info("Imagem selecionada. Clique em Salvar para aplicar.");
  }, [clearPendingLogo]);

  const handleLogoRemove = useCallback(async () => {
    if (pendingLogoFile || pendingLogoPreviewUrl) {
      clearPendingLogo();
      if (hasPersistedLogo) {
        setRemoveLogoOnSave(true);
      } else {
        setRemoveLogoOnSave(false);
      }
      toast.info("Alteração da imagem atualizada. Clique em Salvar para aplicar.");
      return;
    }

    if (hasPersistedLogo) {
      setRemoveLogoOnSave(true);
      toast.info("Remoção da imagem marcada. Clique em Salvar para aplicar.");
    }
  }, [clearPendingLogo, hasPersistedLogo, pendingLogoFile, pendingLogoPreviewUrl]);

  const handleSaveAll = useCallback(async () => {
    if (paletteOn && !isValidHex(primary)) {
      toast.error("Cor primária inválida.");
      return;
    }

    setSaving(true);
    try {
      let logoPayload = {
        logo_path: branding?.logo_path ?? null,
        favicon_path: branding?.favicon_path ?? null,
        use_custom_logo: branding?.use_custom_logo ?? false,
        use_custom_favicon: branding?.use_custom_favicon ?? false,
      };

      if (pendingLogoFile) {
        const uploaded = await uploadLogoAndFaviconAsset(pendingLogoFile);
        logoPayload = {
          ...uploaded,
          use_custom_logo: true,
          use_custom_favicon: true,
        };
      } else if (removeLogoOnSave) {
        logoPayload = {
          logo_path: null,
          favicon_path: null,
          use_custom_logo: false,
          use_custom_favicon: false,
        };
      }

      await saveBranding({
        client_name: clientName.trim() || null,
        use_custom_palette: paletteOn,
        primary_color: paletteOn ? normalizeHex(primary) : null,
        secondary_color: paletteOn ? normalizeHex(secondary) : null,
        tertiary_color: paletteOn ? normalizeHex(tertiary) : null,
        ...logoPayload,
      });

      await refetch();
      setRemoveLogoOnSave(false);
      clearPendingLogo();
      toast.success("Customização salva.");
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Erro ao salvar.");
    } finally {
      setSaving(false);
    }
  }, [
    branding?.favicon_path,
    branding?.logo_path,
    branding?.use_custom_favicon,
    branding?.use_custom_logo,
    clientName,
    clearPendingLogo,
    paletteOn,
    pendingLogoFile,
    primary,
    refetch,
    removeLogoOnSave,
    saveBranding,
    secondary,
    tertiary,
  ]);

  if (brandingLoading && !branding) {
    return (
      <div className="flex items-center justify-center py-8">
        <Loader2 className="h-6 w-6 animate-spin text-primary-icon" />
      </div>
    );
  }

  return (
    <BrandingSettingsCard
      title="Customização da Interface"
      description="Nome da marca, paleta de cores e logo (sidebar, login e aba do navegador)."
    >
      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <div className="space-y-3">
          <div>
            <Label htmlFor="branding-client-name" className="text-xs font-medium">
              Nome da marca
            </Label>
            <Input
              id="branding-client-name"
              value={clientName}
              onChange={(e) => setClientName(e.target.value)}
              placeholder="Ex.: Contabilidade"
              disabled={saving}
              className="h-8 text-sm"
            />
          </div>

          <div className="flex items-center justify-between gap-2">
            <div className="flex items-center gap-1.5">
              <Palette className="h-3.5 w-3.5 text-primary-icon" />
              <Label htmlFor="branding-palette-toggle" className="text-xs font-medium">
                Paleta personalizada
              </Label>
            </div>
            <Switch
              id="branding-palette-toggle"
              checked={paletteOn}
              onCheckedChange={handlePaletteToggle}
              disabled={saving}
            />
          </div>

          <ColorPaletteEditor
            primary={primary}
            secondary={secondary}
            tertiary={tertiary}
            onChange={handlePaletteChange}
            disabled={!paletteOn || saving}
          />

          {branding?.use_custom_palette && (
            <RestoreDefaultButton onRestore={handleRestorePalette} disabled={saving} />
          )}
        </div>

        <div className="space-y-3">
          <LogoUploadField
            currentUrl={displayedLogoUrl}
            onUpload={handleLogoUpload}
            onRemove={handleLogoRemove}
            disabled={saving}
            unified
          />

          <ThemePreviewPanel
            primaryHex={paletteOn ? primary : null}
            secondaryHex={paletteOn ? secondary : null}
            tertiaryHex={paletteOn ? tertiary : null}
          />
        </div>
      </div>

      <div className="mt-4 flex justify-end border-t border-border pt-4">
        <Button onClick={handleSaveAll} disabled={saving}>
          {saving ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
          Salvar
        </Button>
      </div>
    </BrandingSettingsCard>
  );
}
