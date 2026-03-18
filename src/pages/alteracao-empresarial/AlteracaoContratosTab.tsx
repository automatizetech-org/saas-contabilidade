import { useState, useEffect } from "react";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { FileText, Loader2, AlertCircle, Download } from "lucide-react";
import { toast } from "sonner";
import {
  getRedPlaceholdersFromDocx,
  fillDocxWithReplacements,
} from "@/services/contratosDocxService";

const CONTRATO_URL = "/contratos/Contrato.docx";

export function AlteracaoContratosTab() {
  const [status, setStatus] = useState<"idle" | "loading" | "empty" | "error" | "ready">("idle");
  const [placeholders, setPlaceholders] = useState<string[]>([]);
  const [form, setForm] = useState<Record<string, string>>({});
  const [docxBuffer, setDocxBuffer] = useState<ArrayBuffer | null>(null);
  const [generateLoading, setGenerateLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");

  useEffect(() => {
    let cancelled = false;
    setStatus("loading");
    setErrorMessage("");
    fetch(CONTRATO_URL)
      .then((res) => {
        if (!res.ok) throw new Error("Arquivo Contrato.docx não encontrado. Coloque-o em public/contratos/.");
        return res.arrayBuffer();
      })
      .then(async (buffer) => {
        if (cancelled) return;
        try {
          const list = await getRedPlaceholdersFromDocx(buffer);
          if (!list.length) {
            setStatus("empty");
            return;
          }
          setPlaceholders(list);
          setDocxBuffer(buffer);
          setForm(
            list.reduce<Record<string, string>>((acc, p) => {
              acc[p] = "";
              return acc;
            }, {})
          );
          setStatus("ready");
        } catch (e) {
          setStatus("error");
          setErrorMessage(e instanceof Error ? e.message : "Erro ao ler DOCX");
        }
      })
      .catch((e) => {
        if (!cancelled) {
          setStatus("error");
          setErrorMessage(e instanceof Error ? e.message : "Falha ao carregar o contrato");
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const handleGenerate = async () => {
    if (!docxBuffer || status !== "ready") return;
    setGenerateLoading(true);
    try {
      const replacements: Record<string, string> = {};
      placeholders.forEach((p) => {
        replacements[p] = (form[p] ?? "").trim() || p;
      });
      const blob = await fillDocxWithReplacements(docxBuffer, replacements);
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = "Contrato-preenchido.docx";
      a.click();
      URL.revokeObjectURL(url);
      toast.success("Contrato gerado. Download iniciado.");
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "Erro ao gerar DOCX");
    } finally {
      setGenerateLoading(false);
    }
  };

  if (status === "loading") {
    return (
      <GlassCard className="p-8">
        <div className="flex flex-col items-center gap-3 text-muted-foreground">
          <Loader2 className="h-10 w-10 animate-spin" />
          <p className="text-sm">Carregando contrato e identificando campos...</p>
        </div>
      </GlassCard>
    );
  }

  if (status === "error") {
    return (
      <GlassCard className="p-8">
        <div className="flex flex-col items-center gap-3 text-destructive">
          <AlertCircle className="h-10 w-10" />
          <p className="text-sm">{errorMessage}</p>
          <p className="text-xs text-muted-foreground">Coloque o arquivo Contrato.docx na pasta public/contratos/ do projeto.</p>
        </div>
      </GlassCard>
    );
  }

  if (status === "empty") {
    return (
      <GlassCard className="p-8">
        <div className="flex flex-col items-center gap-3 text-muted-foreground">
          <FileText className="h-10 w-10" />
          <p className="text-sm">Nenhum trecho em vermelho encontrado no documento. Os campos dinâmicos devem estar em fonte vermelha.</p>
        </div>
      </GlassCard>
    );
  }

  return (
    <div className="space-y-6">
      <GlassCard className="p-6">
        <h3 className="text-sm font-semibold font-display mb-2">Campos do contrato</h3>
        <p className="text-xs text-muted-foreground mb-4">
          Preencha os campos abaixo (correspondentes aos trechos em vermelho no Contrato.docx). Depois clique em Gerar para baixar o contrato preenchido.
        </p>
        <div className="space-y-4">
          {placeholders.map((key) => (
            <div key={key} className="space-y-2">
              <Label className="text-xs break-all">{key}</Label>
              <Input
                value={form[key] ?? ""}
                onChange={(e) => setForm((p) => ({ ...p, [key]: e.target.value }))}
                placeholder={key}
                className="text-sm"
              />
            </div>
          ))}
        </div>
        <div className="mt-6 flex gap-2">
          <Button onClick={handleGenerate} disabled={generateLoading}>
            {generateLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Download className="h-4 w-4" />}
            {generateLoading ? "Gerando..." : "Gerar e baixar"}
          </Button>
        </div>
      </GlassCard>
    </div>
  );
}
