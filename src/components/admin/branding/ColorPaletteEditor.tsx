import { useState, useCallback } from "react";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { ColorSwatchPreview } from "./ColorSwatchPreview";
import {
  isValidHex,
  normalizeHex,
  hasGoodContrast,
  contrastRatio,
} from "@/lib/brandingTheme";
import { AlertCircle } from "lucide-react";

const DEFAULT_PRIMARY = "#2563EB";
const DEFAULT_SECONDARY = "#7C3AED";
const DEFAULT_TERTIARY = "#10B981";

type ColorPaletteEditorProps = {
  primary: string;
  secondary: string;
  tertiary: string;
  onChange: (primary: string, secondary: string, tertiary: string) => void;
  disabled?: boolean;
};

export function ColorPaletteEditor({
  primary,
  secondary,
  tertiary,
  onChange,
  disabled,
}: ColorPaletteEditorProps) {
  const [primaryInput, setPrimaryInput] = useState(primary || DEFAULT_PRIMARY);
  const [secondaryInput, setSecondaryInput] = useState(secondary || DEFAULT_SECONDARY);
  const [tertiaryInput, setTertiaryInput] = useState(tertiary || DEFAULT_TERTIARY);
  const [errors, setErrors] = useState<Record<string, string>>({});

  const validate = useCallback(() => {
    const next: Record<string, string> = {};
    if (!isValidHex(primaryInput)) next.primary = "Cor inválida. Use hexadecimal (ex: #2563EB).";
    if (!isValidHex(secondaryInput)) next.secondary = "Cor inválida.";
    if (!isValidHex(tertiaryInput)) next.tertiary = "Cor inválida.";
    setErrors(next);
    return Object.keys(next).length === 0;
  }, [primaryInput, secondaryInput, tertiaryInput]);

  const apply = useCallback(() => {
    if (!validate()) return;
    const p = normalizeHex(primaryInput);
    const s = normalizeHex(secondaryInput);
    const t = normalizeHex(tertiaryInput);
    onChange(p, s, t);
  }, [primaryInput, secondaryInput, tertiaryInput, onChange, validate]);

  const handlePrimaryBlur = () => {
    if (isValidHex(primaryInput)) onChange(normalizeHex(primaryInput), secondary, tertiary);
  };
  const handleSecondaryBlur = () => {
    if (isValidHex(secondaryInput)) onChange(primary, normalizeHex(secondaryInput), tertiary);
  };
  const handleTertiaryBlur = () => {
    if (isValidHex(tertiaryInput)) onChange(primary, secondary, normalizeHex(tertiaryInput));
  };

  const primaryContrastOk = isValidHex(primaryInput) && hasGoodContrast("#ffffff", primaryInput);
  const primaryContrastRatio = isValidHex(primaryInput)
    ? contrastRatio("#ffffff", primaryInput).toFixed(1)
    : null;

  return (
    <div className="space-y-4">
      <div className="grid gap-4 sm:grid-cols-3">
        <div className="space-y-2">
          <Label className="text-xs font-medium text-muted-foreground">Cor primária</Label>
          <div className="flex gap-2">
            <input
              type="color"
              value={primaryInput.startsWith("#") ? primaryInput : `#${primaryInput}`}
              onChange={(e) => {
                setPrimaryInput(e.target.value);
                onChange(e.target.value, secondary, tertiary);
              }}
              disabled={disabled}
              className="h-10 w-14 cursor-pointer rounded-lg border border-input bg-transparent p-0"
            />
            <Input
              value={primaryInput}
              onChange={(e) => setPrimaryInput(e.target.value)}
              onBlur={handlePrimaryBlur}
              placeholder="#2563EB"
              maxLength={7}
              disabled={disabled}
              className="font-mono text-sm"
            />
          </div>
          {errors.primary && (
            <p className="text-xs text-destructive flex items-center gap-1">
              <AlertCircle className="h-3 w-3" />
              {errors.primary}
            </p>
          )}
          {primaryContrastRatio && !primaryContrastOk && (
            <p className="text-xs text-amber-600 dark:text-amber-400">
              Contraste com branco: {primaryContrastRatio}:1 (recomendado ≥4.5 para texto)
            </p>
          )}
        </div>
        <div className="space-y-2">
          <Label className="text-xs font-medium text-muted-foreground">Cor secundária</Label>
          <div className="flex gap-2">
            <input
              type="color"
              value={secondaryInput.startsWith("#") ? secondaryInput : `#${secondaryInput}`}
              onChange={(e) => {
                setSecondaryInput(e.target.value);
                onChange(primary, e.target.value, tertiary);
              }}
              disabled={disabled}
              className="h-10 w-14 cursor-pointer rounded-lg border border-input bg-transparent p-0"
            />
            <Input
              value={secondaryInput}
              onChange={(e) => setSecondaryInput(e.target.value)}
              onBlur={handleSecondaryBlur}
              placeholder="#7C3AED"
              maxLength={7}
              disabled={disabled}
              className="font-mono text-sm"
            />
          </div>
          {errors.secondary && (
            <p className="text-xs text-destructive flex items-center gap-1">
              <AlertCircle className="h-3 w-3" />
              {errors.secondary}
            </p>
          )}
        </div>
        <div className="space-y-2">
          <Label className="text-xs font-medium text-muted-foreground">Cor terciária</Label>
          <div className="flex gap-2">
            <input
              type="color"
              value={tertiaryInput.startsWith("#") ? tertiaryInput : `#${tertiaryInput}`}
              onChange={(e) => {
                setTertiaryInput(e.target.value);
                onChange(primary, secondary, e.target.value);
              }}
              disabled={disabled}
              className="h-10 w-14 cursor-pointer rounded-lg border border-input bg-transparent p-0"
            />
            <Input
              value={tertiaryInput}
              onChange={(e) => setTertiaryInput(e.target.value)}
              onBlur={handleTertiaryBlur}
              placeholder="#10B981"
              maxLength={7}
              disabled={disabled}
              className="font-mono text-sm"
            />
          </div>
          {errors.tertiary && (
            <p className="text-xs text-destructive flex items-center gap-1">
              <AlertCircle className="h-3 w-3" />
              {errors.tertiary}
            </p>
          )}
        </div>
      </div>
      <div className="flex items-center gap-3">
        <ColorSwatchPreview colors={[primary || primaryInput, secondary || secondaryInput, tertiary || tertiaryInput]} />
        <span className="text-xs text-muted-foreground">Preview da paleta</span>
      </div>
    </div>
  );
}

export { DEFAULT_PRIMARY, DEFAULT_SECONDARY, DEFAULT_TERTIARY };
