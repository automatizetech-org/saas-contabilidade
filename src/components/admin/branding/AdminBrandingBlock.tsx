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

export function AdminBrandingBlock() {
  const {
    branding,
    isLoading: brandingLoading,
    logoUrl,
    primaryColor,
    secondaryColor,
    tertiaryColor,
    useCustomPalette,
    saveBranding,
    uploadLogo,
    removeLogo,
    refetch,
  } = useBranding();

  const [paletteOn, setPaletteOn] = useState(!!branding?.use_custom_palette);
  const [primary, setPrimary] = useState(primaryColor ?? DEFAULT_PRIMARY);
  const [secondary, setSecondary] = useState(secondaryColor ?? DEFAULT_SECONDARY);
  const [tertiary, setTertiary] = useState(tertiaryColor ?? DEFAULT_TERTIARY);
  const [clientName, setClientName] = useState(branding?.client_name ?? "");
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    setPaletteOn(!!branding?.use_custom_palette);
    setPrimary(primaryColor ?? DEFAULT_PRIMARY);
    setSecondary(secondaryColor ?? DEFAULT_SECONDARY);
    setTertiary(tertiaryColor ?? DEFAULT_TERTIARY);
    setClientName(branding?.client_name ?? "");
  }, [branding?.use_custom_palette, branding?.client_name, primaryColor, secondaryColor, tertiaryColor]);

  const hasLogo = !!branding?.use_custom_logo && !!branding?.logo_url;

  const handlePaletteChange = useCallback((p: string, s: string, t: string) => {
    setPrimary(p);
    setSecondary(s);
    setTertiary(t);
  }, []);

  const handleSaveAll = useCallback(async () => {
    if (paletteOn && !isValidHex(primary)) {
      toast.error("Cor primária inválida.");
      return;
    }
    setSaving(true);
    try {
      await saveBranding({
        client_name: clientName.trim() || null,
        use_custom_palette: paletteOn,
        primary_color: paletteOn ? normalizeHex(primary) : null,
        secondary_color: paletteOn ? normalizeHex(secondary) : null,
        tertiary_color: paletteOn ? normalizeHex(tertiary) : null,
      });
      toast.success("Customização salva.");
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Erro ao salvar.");
    } finally {
      setSaving(false);
    }
  }, [clientName, paletteOn, primary, secondary, tertiary, saveBranding]);

  const handlePaletteToggle = useCallback(
    async (checked: boolean) => {
      setPaletteOn(checked);
      if (!checked) {
        setPrimary(DEFAULT_PRIMARY);
        setSecondary(DEFAULT_SECONDARY);
        setTertiary(DEFAULT_TERTIARY);
        setSaving(true);
        try {
          await saveBranding({
            use_custom_palette: false,
            primary_color: null,
            secondary_color: null,
            tertiary_color: null,
          });
          toast.success("Paleta desligada. Tema padrão restaurado.");
          refetch();
        } catch (e) {
          toast.error(e instanceof Error ? e.message : "Erro ao restaurar padrão.");
          setPaletteOn(true);
        } finally {
          setSaving(false);
        }
      }
    },
    [saveBranding, refetch]
  );

  const handleRestorePalette = useCallback(() => {
    setPaletteOn(false);
    setPrimary(DEFAULT_PRIMARY);
    setSecondary(DEFAULT_SECONDARY);
    setTertiary(DEFAULT_TERTIARY);
    saveBranding({
      use_custom_palette: false,
      primary_color: null,
      secondary_color: null,
      tertiary_color: null,
    }).then(() => {
      toast.success("Paleta padrão restaurada.");
      refetch();
    });
  }, [saveBranding, refetch]);

  const handleLogoUpload = useCallback(
    async (file: File) => {
      await uploadLogo(file);
      toast.success("Logo e ícone salvos.");
    },
    [uploadLogo]
  );

  const handleLogoRemove = useCallback(async () => {
    await removeLogo();
    toast.success("Logo e ícone removidos.");
  }, [removeLogo]);

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
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="space-y-3">
          <div>
            <Label htmlFor="branding-client-name" className="text-xs font-medium">Nome da marca</Label>
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
            disabled={!paletteOn}
          />
          {branding?.use_custom_palette && (
            <RestoreDefaultButton onRestore={handleRestorePalette} disabled={saving} />
          )}
        </div>

        <div className="space-y-3">
          <LogoUploadField
            currentUrl={hasLogo ? branding!.logo_url! : null}
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
      <div className="mt-4 pt-4 border-t border-border flex justify-end">
        <Button onClick={handleSaveAll} disabled={saving}>
          {saving ? <Loader2 className="h-4 w-4 animate-spin mr-2" /> : null}
          Salvar
        </Button>
      </div>
    </BrandingSettingsCard>
  );
}
