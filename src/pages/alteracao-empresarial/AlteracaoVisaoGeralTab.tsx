import { useState, useRef, useEffect } from "react";
import { useQuery } from "@tanstack/react-query";
import { GlassCard } from "@/components/dashboard/GlassCard";
import { StatsCard } from "@/components/dashboard/StatsCard";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Textarea } from "@/components/ui/textarea";
import { FileText, Search, Loader2, AlertCircle, QrCode, Send, Link, Unlink, RefreshCw, ChevronsUpDown, Plus, Trash2, Upload, X } from "lucide-react";
import { toast } from "sonner";
import { fetchCnpjPublica, CnpjFormData } from "@/services/cnpjPublicaService";
import { OPCOES_PARCELAMENTO } from "@/constants/parcelamentoOpcoes";
import { formatCpf, getAccountants } from "@/services/accountantsService";
import {
  formatAlteracaoMessage,
  getConnectionStatus,
  getQrImage,
  getGroups,
  sendToGroup,
  connectWhatsApp,
  disconnectWhatsApp,
} from "@/services/whatsapp";
import {
  onlyDigits,
  validateCNPJ,
  validateCPF,
  validateEmail,
  formatCNPJ,
  formatCPF,
  formatCNPJOrCPF,
  formatCompetencia,
  formatDataDDMMAAAA,
  formatCurrencyBRL,
  currencyToDigits,
  formatTelefoneInput,
} from "@/lib/validators";
import { extractPdfFormFields } from "@/lib/extractPdfFormFields";
import {
  fetchSalarioMinimoBCB,
  qualificacaoFromHonorario,
  QUALIFICACAO_DISPLAY,
  type QualificacaoPlano,
} from "@/services/bcbSalarioMinimoService";
import { useProfile } from "@/hooks/useProfile";

const SIM_NAO = [
  { value: "sim", label: "Sim" },
  { value: "nao", label: "Não" },
  { value: "nao_informado", label: "Não informado" },
];

const TIPO_CONTABILIDADE = [
  { value: "Planilha", label: "Planilha" },
  { value: "Documentos", label: "Documentos" },
];

const TIPO_FORMULARIO = [
  { value: "abertura", label: "ABERTURA" },
  { value: "alteracao_contratual", label: "ALTERAÇÃO CONTRATUAL" },
  { value: "suspensao", label: "SUSPENSÃO" },
  { value: "baixa", label: "BAIXA" },
];

const INITIAL_FORM = {
  tipo_formulario: "",
  razao_social: "",
  cnpj: "",
  qualificacao_plano: "",
  data_abertura: "",
  tipo_atividade: "",
  inscricao_estadual: "",
  inscricao_municipal: "",
  competencia_inicial: "",
  tributacao: "",
  possui_st: "nao_informado",
  possui_retencao_impostos: "nao_informado",
  socios: [{ nome_socio: "", cpf_socio: "" }],
  contatos: [{ nome_contato: "", email_contato: "", telefone_contato: "" }],
  possui_prolabore: "nao_informado",
  valor_prolabore: "",
  possui_empregados: "nao_informado",
  possui_contabilidade: "nao_informado",
  tipo_contabilidade: "",
  regime_contabil: "",
  possui_parcelamento: "nao_informado",
  tipos_parcelamento: [""],
  contador_responsavel_cpf: "",
  contador_responsavel_nome: "",
  valor_honorario: "",
  vencimento_honorario: "",
  data_primeiro_honorario: "",
  observacao: "",
};

export function AlteracaoVisaoGeralTab() {
  const { officeId } = useProfile();
  const waGroupStorageKey = officeId
    ? `alteracao-empresarial-wa-group-${officeId}`
    : "alteracao-empresarial-wa-group-id";

  const [cnpjBusca, setCnpjBusca] = useState("");
  const [cnpjError, setCnpjError] = useState("");
  const [loadingCnpj, setLoadingCnpj] = useState(false);
  const cacheRef = useRef<Record<string, CnpjFormData | null>>({});

  const [waConnected, setWaConnected] = useState<boolean | null>(null);
  const [waQr, setWaQr] = useState<string | null>(null);
  const [waApiError, setWaApiError] = useState<string | null>(null);
  const [waGroups, setWaGroups] = useState<{ id: string; name: string }[]>([]);
  const [waGroupId, setWaGroupId] = useState("");
  const [waLoading, setWaLoading] = useState(false);
  const [waError, setWaError] = useState("");
  const [waConnecting, setWaConnecting] = useState(false);
  const [waDisconnecting, setWaDisconnecting] = useState(false);
  const [waGroupsLoading, setWaGroupsLoading] = useState(false);
  /** Um auto-connect por montagem (ou após reset explícito ao mudar office). */
  const waAutoConnectOnce = useRef(false);
  const lastQrFetchTime = useRef(0);
  const waQrRef = useRef<string | null>(null);
  const waGroupsFilledRef = useRef(false);
  const [anexos, setAnexos] = useState<File[]>([]);
  const [uploadDragOver, setUploadDragOver] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [qualificacaoLoading, setQualificacaoLoading] = useState(false);
  const { data: accountants = [] } = useQuery({
    queryKey: ["accountants"],
    queryFn: () => getAccountants(true),
    staleTime: 30000,
  });

  const [form, setForm] = useState(INITIAL_FORM);

  const update = (key: string, value: string) => setForm((p) => ({ ...p, [key]: value }));

  const updateSocio = (index: number, field: "nome_socio" | "cpf_socio", value: string) => {
    setForm((p) => {
      const next = [...p.socios];
      next[index] = { ...next[index], [field]: value };
      return { ...p, socios: next };
    });
  };
  const addSocio = () => setForm((p) => ({ ...p, socios: [...p.socios, { nome_socio: "", cpf_socio: "" }] }));
  const removeSocio = (index: number) => {
    setForm((p) => ({ ...p, socios: p.socios.filter((_, i) => i !== index) }));
  };

  const updateContato = (index: number, field: "nome_contato" | "email_contato" | "telefone_contato", value: string) => {
    setForm((p) => {
      const next = [...p.contatos];
      next[index] = { ...next[index], [field]: value };
      return { ...p, contatos: next };
    });
  };
  const addContato = () => setForm((p) => ({ ...p, contatos: [...p.contatos, { nome_contato: "", email_contato: "", telefone_contato: "" }] }));
  const removeContato = (index: number) => {
    setForm((p) => ({ ...p, contatos: p.contatos.filter((_, i) => i !== index) }));
  };

  const updateParcelamento = (index: number, value: string) => {
    setForm((p) => {
      const next = [...p.tipos_parcelamento];
      next[index] = value;
      return { ...p, tipos_parcelamento: next };
    });
  };
  const addParcelamento = () => setForm((p) => ({ ...p, tipos_parcelamento: [...p.tipos_parcelamento, ""] }));
  const removeParcelamento = (index: number) => {
    setForm((p) => ({
      ...p,
      tipos_parcelamento: p.tipos_parcelamento.filter((_, i) => i !== index),
    }));
  };

  useEffect(() => {
    waQrRef.current = waQr;
  }, [waQr]);

  // Uma sequência na montagem / troca de escritório: status + um único POST connect (sem rajadas de timers).
  useEffect(() => {
    let cancelled = false;
    waAutoConnectOnce.current = false;
    void (async () => {
      try {
        const s = await getConnectionStatus();
        if (cancelled) return;
        if (s.sessionUnauthorized) {
          setWaApiError(
            "Sessão não autorizada na API. Confirme login, atualize a página ou verifique se o .env do front aponta para o mesmo projeto Supabase da Edge.",
          );
          setWaConnected(false);
          return;
        }
        setWaConnected(s.connected);
        if (!s.connected && !waAutoConnectOnce.current) {
          waAutoConnectOnce.current = true;
          await connectWhatsApp().catch(() => {});
        }
      } catch {
        if (!cancelled) {
          setWaConnected(false);
          if (!waAutoConnectOnce.current) {
            waAutoConnectOnce.current = true;
            await connectWhatsApp().catch(() => {});
          }
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [officeId]);

  useEffect(() => {
    if (waConnected === true) {
      waAutoConnectOnce.current = false;
      setWaQr(null);
      setWaApiError(null);
      setWaGroupsLoading(true);
      waGroupsFilledRef.current = false;
      const load = () =>
        getGroups(false).then((g) => {
          if (g.length > 0) waGroupsFilledRef.current = true;
          setWaGroups(g);
          setWaGroupsLoading(false);
          const saved = localStorage.getItem(waGroupStorageKey);
          if (saved && g.some((gr) => gr.id === saved)) setWaGroupId(saved);
        });
      load();
      const t1 = setTimeout(() => {
        if (!waGroupsFilledRef.current) getGroups(true).then((g) => {
          if (g.length > 0) waGroupsFilledRef.current = true;
          setWaGroups(g);
          setWaGroupsLoading(false);
        });
      }, 2000);
      const t2 = setTimeout(() => {
        if (!waGroupsFilledRef.current) getGroups(true).then((g) => {
          if (g.length > 0) waGroupsFilledRef.current = true;
          setWaGroups(g);
          setWaGroupsLoading(false);
        });
      }, 6000);
      return () => {
        clearTimeout(t1);
        clearTimeout(t2);
      };
    }
    if (waConnected !== false) return undefined;

    setWaGroups([]);
    setWaGroupId("");

    let cancelled = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let tickBusy = false;

    const schedule = (delayMs: number) => {
      if (cancelled) return;
      if (timer !== undefined) window.clearTimeout(timer);
      timer = window.setTimeout(() => void tick(), delayMs);
    };

    async function tick() {
      if (cancelled || tickBusy) return;
      tickBusy = true;
      let stillDisconnected = true;
      try {
        const status = await getConnectionStatus();
        if (cancelled) return;
        if (status.sessionUnauthorized) {
          setWaApiError(
            "Sessão não autorizada na API. Confirme login, atualize a página ou verifique se o .env do front aponta para o mesmo projeto Supabase da Edge.",
          );
          setWaConnected(false);
          setWaQr(null);
          stillDisconnected = false;
          return;
        }
        setWaApiError(null);
        setWaConnected(status.connected);
        if (status.connected) {
          setWaQr(null);
          stillDisconnected = false;
          return;
        }
        const now = Date.now();
        const hasQr = waQrRef.current != null && waQrRef.current.length > 0;
        const qrExpired = hasQr && now - lastQrFetchTime.current >= 55000;
        const needQr = !hasQr || qrExpired;
        if (needQr) {
          const qr = await getQrImage();
          if (cancelled) return;
          if (qr) {
            lastQrFetchTime.current = now;
            setWaQr(qr);
          } else {
            setWaQr(null);
          }
        }
      } catch {
        if (!cancelled) {
          setWaApiError(
            "WhatsApp inacessível. Com escritório no SaaS: VM com CONNECTOR_SECRET, Edge office-server. Modo legado: WHATSAPP_API e VITE_WHATSAPP_VIA_OFFICE_SERVER=false.",
          );
          setWaConnected(false);
          setWaQr(null);
        }
      } finally {
        tickBusy = false;
        if (!cancelled && stillDisconnected) schedule(22000);
      }
    }

    schedule(9000);

    return () => {
      cancelled = true;
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, [waConnected, waGroupStorageKey]);

  // QR só via /qr (base64); sem polling de qr.png para evitar infinitas requisições e bloqueio por ad blocker

  const handleBuscarCnpj = async () => {
    const digits = onlyDigits(cnpjBusca);
    setCnpjError("");
    if (digits.length === 11) {
      toast.info("Consulta na Receita disponível apenas para CNPJ (14 dígitos). O CPF foi aceito no campo.");
      return;
    }
    if (digits.length !== 14) {
      setCnpjError("Informe CNPJ (14 dígitos) ou CPF (11 dígitos).");
      return;
    }
    if (!validateCNPJ(digits)) {
      setCnpjError("CNPJ inválido (dígitos verificadores).");
      return;
    }
    if (cacheRef.current[digits]) {
      applyCnpjData(cacheRef.current[digits]!);
      return;
    }
    setLoadingCnpj(true);
    try {
      const data = await Promise.race([
        fetchCnpjPublica(digits),
        new Promise<CnpjFormData | null>((_, rej) => setTimeout(() => rej(new Error("Timeout")), 15000)),
      ]);
      if (data) {
        cacheRef.current[digits] = data;
        applyCnpjData(data);
        toast.success("Dados preenchidos pela Receita.");
      } else {
        setCnpjError("Resposta da API sem dados utilizáveis.");
      }
    } catch (e) {
      const msg = e instanceof Error ? e.message : "Erro ao consultar CNPJ.";
      setCnpjError(msg);
      toast.error(msg);
    } finally {
      setLoadingCnpj(false);
    }
  };

  const applyCnpjData = (data: CnpjFormData) => {
    if (!data) return;
    const apiSocios = data.socios?.length ? data.socios.map((s) => ({ nome_socio: s.nome, cpf_socio: s.cpf_socio })) : [{ nome_socio: "", cpf_socio: "" }];
    setForm((p) => {
      const nextContatos = [...p.contatos];
      if (nextContatos.length > 0) {
        nextContatos[0] = {
          ...nextContatos[0],
          email_contato: nextContatos[0].email_contato?.trim() ? nextContatos[0].email_contato : (data.email ?? nextContatos[0].email_contato),
          telefone_contato: nextContatos[0].telefone_contato?.trim() ? nextContatos[0].telefone_contato : (data.telefone ?? nextContatos[0].telefone_contato),
        };
      }
      // Usar todos os sócios retornados pela API; preservar valores já preenchidos no formulário quando existirem
      const socios =
        data.socios && data.socios.length > 0
          ? data.socios.map((_, i) => ({
              nome_socio: p.socios[i]?.nome_socio?.trim() ? p.socios[i].nome_socio : (apiSocios[i]?.nome_socio ?? ""),
              cpf_socio: p.socios[i]?.cpf_socio?.trim() ? p.socios[i].cpf_socio : (apiSocios[i]?.cpf_socio ?? ""),
            }))
          : p.socios.map((s, i) => ({
              nome_socio: s.nome_socio?.trim() ? s.nome_socio : (apiSocios[i]?.nome_socio ?? s.nome_socio),
              cpf_socio: s.cpf_socio?.trim() ? s.cpf_socio : (apiSocios[i]?.cpf_socio ?? s.cpf_socio),
            }));
      return {
        ...p,
        razao_social: p.razao_social?.trim() ? p.razao_social : (data.razao_social || p.razao_social),
        cnpj: p.cnpj?.trim() ? p.cnpj : (data.cnpj || p.cnpj),
        qualificacao_plano: p.qualificacao_plano?.trim() ? p.qualificacao_plano : (data.natureza_juridica || p.qualificacao_plano),
        data_abertura: p.data_abertura?.trim() ? p.data_abertura : (data.data_abertura || p.data_abertura),
        tipo_atividade: p.tipo_atividade?.trim() ? p.tipo_atividade : (data.tipo_atividade || p.tipo_atividade),
        inscricao_estadual: p.inscricao_estadual?.trim() ? p.inscricao_estadual : (data.inscricao_estadual || p.inscricao_estadual),
        tributacao: p.tributacao?.trim() ? p.tributacao : (data.tributacao || p.tributacao),
        socios,
        contatos: nextContatos,
      };
    });
  };

  const handleFinalizado = async () => {
    const companyDocument = onlyDigits(form.cnpj);
    if (companyDocument) {
      if (companyDocument.length === 11 && !validateCPF(companyDocument)) {
        toast.error("CPF da empresa invalido.");
        return;
      }
      if (companyDocument.length === 14 && !validateCNPJ(companyDocument)) {
        toast.error("CNPJ da empresa invalido.");
        return;
      }
      if (companyDocument.length !== 11 && companyDocument.length !== 14) {
        toast.error("Informe um CPF ou CNPJ valido para a empresa.");
        return;
      }
    }

    const accountantCpf = onlyDigits(form.contador_responsavel_cpf);
    if (accountantCpf && !validateCPF(accountantCpf)) {
      toast.error("CPF do contador responsavel invalido.");
      return;
    }

    for (let i = 0; i < form.socios.length; i++) {
      const s = form.socios[i];
      const d = onlyDigits(s.cpf_socio);
      if (d.length === 11 && !validateCPF(s.cpf_socio)) {
        toast.error(`CPF do sócio ${form.socios.length > 1 ? i + 1 : ""} inválido.`);
        return;
      }
    }
    for (const c of form.contatos) {
      const email = (c.email_contato ?? "").trim();
      if (email && !validateEmail(email)) {
        toast.error("E-mail do contato inválido.");
        return;
      }
    }
    const message = formatAlteracaoMessage(form);
    if (waConnected && waGroupId) {
      setWaError("");
      setWaLoading(true);
      try {
        let attachments: { filename: string; mimetype: string; dataBase64: string }[] | undefined;
        if (anexos.length > 0) {
          attachments = await Promise.all(
            anexos.map(async (file) => {
              const dataBase64 = await new Promise<string>((resolve, reject) => {
                const r = new FileReader();
                r.onloadend = () => {
                  const result = r.result;
                  if (typeof result === "string") {
                    const base64 = result.includes(",") ? result.split(",")[1] : result;
                    resolve(base64 ?? "");
                  } else resolve("");
                };
                r.onerror = () => reject(new Error("Falha ao ler arquivo"));
                r.readAsDataURL(file);
              });
              return {
                filename: file.name,
                mimetype: file.type || "application/octet-stream",
                dataBase64,
              };
            })
          );
          attachments = attachments.filter((a) => a.dataBase64 && a.dataBase64.length > 0);
          if (attachments.length === 0) attachments = undefined;
        }
        const result = await sendToGroup(waGroupId, message, attachments);
        if (result.ok) {
          toast.success("Mensagem e documentos enviados ao grupo no WhatsApp.");
          setAnexos([]);
          setForm({
            ...INITIAL_FORM,
            socios: INITIAL_FORM.socios.map((s) => ({ ...s })),
            contatos: INITIAL_FORM.contatos.map((c) => ({ ...c })),
            tipos_parcelamento: [...INITIAL_FORM.tipos_parcelamento],
          });
        } else {
          setWaError(result.error ?? "Falha ao enviar");
          toast.error(result.error ?? "Falha ao enviar");
        }
      } finally {
        setWaLoading(false);
      }
    } else {
      toast.success("Formulário validado. Conecte o WhatsApp e selecione um grupo para enviar.");
      setForm({
        ...INITIAL_FORM,
        socios: INITIAL_FORM.socios.map((s) => ({ ...s })),
        contatos: INITIAL_FORM.contatos.map((c) => ({ ...c })),
        tipos_parcelamento: [...INITIAL_FORM.tipos_parcelamento],
      });
      setAnexos([]);
    }
  };

  const addAnexos = (files: FileList | null) => {
    if (!files?.length) return;
    const newFiles: File[] = [];
    for (let i = 0; i < files.length; i++) {
      const f = files[i];
      if (f && !anexos.some((x) => x.name === f.name && x.size === f.size)) newFiles.push(f);
    }
    if (newFiles.length === 0) return;

    setAnexos((prev) => {
      const next = [...prev, ...newFiles];
      const pdfFiles = next.filter(
        (f) =>
          f.type === "application/pdf" ||
          f.name.toLowerCase().endsWith(".pdf")
      );
      if (pdfFiles.length > 0) {
        (async () => {
          let lastCnpjOuCpfEmpresa = "";
          let lastInscricaoMunicipal = "";
          const allCpfs: string[] = [];
          let lastTipoAtividade = "";
          let lastEmailContato = "";
          let lastTelefoneContato = "";
          for (const file of pdfFiles) {
            const extracted = await extractPdfFormFields(file);
            if (!extracted) continue;
            if (extracted.cnpjOuCpfEmpresa) lastCnpjOuCpfEmpresa = extracted.cnpjOuCpfEmpresa;
            if (extracted.inscricaoMunicipal)
              lastInscricaoMunicipal = extracted.inscricaoMunicipal;
            allCpfs.push(...extracted.cpfsSocio);
            if (extracted.tipoAtividade) lastTipoAtividade = extracted.tipoAtividade;
            if (extracted.emailContato) lastEmailContato = extracted.emailContato;
            if (extracted.telefoneContato) lastTelefoneContato = extracted.telefoneContato;
          }
          const hasAny =
            lastCnpjOuCpfEmpresa ||
            lastInscricaoMunicipal ||
            allCpfs.length > 0 ||
            lastTipoAtividade ||
            lastEmailContato ||
            lastTelefoneContato;
          if (!hasAny) return;

          // Buscar dados da Receita ANTES de atualizar o form, para aplicar tudo numa única atualização (evita race e garante todos os sócios)
          let cnpjApiData: CnpjFormData | null = null;
          if (lastCnpjOuCpfEmpresa && lastCnpjOuCpfEmpresa.length === 14) {
            try {
              cnpjApiData = await fetchCnpjPublica(lastCnpjOuCpfEmpresa);
              if (cnpjApiData) cacheRef.current[lastCnpjOuCpfEmpresa] = cnpjApiData;
            } catch {
              // Falha na Receita não impede preencher o que veio do PDF
            }
          }

          // Uma única atualização: dados do PDF (só vazios) + dados da API (incluindo TODOS os sócios)
          setForm((p) => {
            const nextForm = { ...p };

            // PDF: só preenche campos vazios
            if (lastCnpjOuCpfEmpresa && !p.cnpj?.trim()) nextForm.cnpj = lastCnpjOuCpfEmpresa;
            if (lastInscricaoMunicipal && !p.inscricao_municipal?.trim()) nextForm.inscricao_municipal = lastInscricaoMunicipal;
            if (lastTipoAtividade && !p.tipo_atividade?.trim()) nextForm.tipo_atividade = lastTipoAtividade;
            if (lastEmailContato || lastTelefoneContato) {
              const nextContatos = [...nextForm.contatos];
              if (nextContatos.length > 0) {
                nextContatos[0] = {
                  ...nextContatos[0],
                  ...(lastEmailContato && !nextContatos[0].email_contato?.trim() && { email_contato: lastEmailContato }),
                  ...(lastTelefoneContato && !nextContatos[0].telefone_contato?.trim() && { telefone_contato: lastTelefoneContato }),
                };
              }
              nextForm.contatos = nextContatos;
            }

            // Sócios do PDF: só preenche linhas ainda vazias (não sobrescreve se já vieram da Receita)
            if (allCpfs.length > 0 && nextForm.socios.length > 0 && !cnpjApiData?.socios?.length) {
              nextForm.socios = nextForm.socios.map((s, i) => {
                const jaPreenchido = !!(s.nome_socio?.trim() || s.cpf_socio?.trim());
                if (jaPreenchido) return s;
                const cpfPdf = i < allCpfs.length ? allCpfs[i] : undefined;
                return cpfPdf ? { ...s, cpf_socio: cpfPdf } : s;
              });
            }

            // API da Receita: preenche campos vazios e SEMPRE usa a lista completa de sócios da API
            if (cnpjApiData) {
              const apiSocios = cnpjApiData.socios?.length
                ? cnpjApiData.socios.map((s) => ({ nome_socio: s.nome, cpf_socio: s.cpf_socio }))
                : [];
              if (apiSocios.length > 0) {
                nextForm.socios = apiSocios.map((api, i) => ({
                  nome_socio: nextForm.socios[i]?.nome_socio?.trim() ? nextForm.socios[i].nome_socio : (api.nome_socio ?? ""),
                  cpf_socio: nextForm.socios[i]?.cpf_socio?.trim() ? nextForm.socios[i].cpf_socio : (api.cpf_socio ?? ""),
                }));
              }
              if (!nextForm.razao_social?.trim()) nextForm.razao_social = cnpjApiData.razao_social || nextForm.razao_social;
              if (!nextForm.qualificacao_plano?.trim()) nextForm.qualificacao_plano = cnpjApiData.natureza_juridica || nextForm.qualificacao_plano;
              if (!nextForm.data_abertura?.trim()) nextForm.data_abertura = cnpjApiData.data_abertura || nextForm.data_abertura;
              if (!nextForm.tipo_atividade?.trim()) nextForm.tipo_atividade = cnpjApiData.tipo_atividade || nextForm.tipo_atividade;
              if (!nextForm.inscricao_estadual?.trim()) nextForm.inscricao_estadual = cnpjApiData.inscricao_estadual || nextForm.inscricao_estadual;
              if (!nextForm.tributacao?.trim()) nextForm.tributacao = cnpjApiData.tributacao || nextForm.tributacao;
              if (nextForm.contatos.length > 0) {
                nextForm.contatos[0] = {
                  ...nextForm.contatos[0],
                  email_contato: nextForm.contatos[0].email_contato?.trim() || cnpjApiData.email || nextForm.contatos[0].email_contato,
                  telefone_contato: nextForm.contatos[0].telefone_contato?.trim() || cnpjApiData.telefone || nextForm.contatos[0].telefone_contato,
                };
              }
            }

            return nextForm;
          });

          if (cnpjApiData) {
            toast.success("CNPJ encontrado no PDF. Dados preenchidos pela Receita (apenas campos vazios).");
          }

          const parts: string[] = [];
          if (lastCnpjOuCpfEmpresa) parts.push("CNPJ/CPF da empresa");
          if (lastInscricaoMunicipal) parts.push("Inscrição Municipal");
          if (allCpfs.length > 0) parts.push(`${allCpfs.length} CPF(s) de sócio(s)`);
          if (lastTipoAtividade) parts.push("Atividade principal");
          if (lastEmailContato) parts.push("E-mail");
          if (lastTelefoneContato) parts.push("Telefone");
          toast.success(`PDF(s) analisado(s): ${parts.join(", ")} preenchidos.`);
        })();
      }
      return next;
    });
  };

  const removeAnexo = (index: number) => setAnexos((p) => p.filter((_, i) => i !== index));

  /** Ao desfocar o valor do honorário: busca salário mínimo no BCB e define qualificação do plano (BRONZE/PRATA/OURO/DIAMANTE). */
  const handleHonorarioBlur = async () => {
    const digits = form.valor_honorario?.replace(/\D/g, "") || "0";
    const valorReais = parseInt(digits, 10) / 100;
    if (valorReais <= 0) return;
    setQualificacaoLoading(true);
    try {
      const sm = await fetchSalarioMinimoBCB();
      if (sm != null) {
        const q = qualificacaoFromHonorario(valorReais, sm);
        setForm((p) => ({ ...p, qualificacao_plano: q }));
        toast.success(`Qualificação definida: ${QUALIFICACAO_DISPLAY[q].emoji} ${q} (${(valorReais / sm * 100).toFixed(1)}% do salário mínimo).`);
      } else {
        toast.error("Não foi possível obter o salário mínimo (BCB). Defina a qualificação manualmente.");
      }
    } finally {
      setQualificacaoLoading(false);
    }
  };

  const qualificacaoDisplay = (QUALIFICACAO_DISPLAY[form.qualificacao_plano as QualificacaoPlano] ?? null);

  return (
    <div className="space-y-6">
      {/* Métricas placeholder */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        <StatsCard title="Contratos" value="—" icon={FileText} />
        <StatsCard title="Formulários" value="—" icon={FileText} />
        <StatsCard title="Pendências" value="—" icon={FileText} />
      </div>

      {/* Formulário principal */}
      <GlassCard className="p-6 space-y-8">
        <div className="flex flex-wrap items-end gap-2">
          <div className="flex-1 min-w-[200px] space-y-2">
            <Label>Buscar por CNPJ/CPF</Label>
            <Input
              placeholder="CNPJ 00.000.000/0001-00 ou CPF 000.000.000-00"
              value={formatCNPJOrCPF(cnpjBusca)}
              onChange={(e) => setCnpjBusca(onlyDigits(e.target.value).slice(0, 14))}
              className="font-mono"
              maxLength={18}
            />
            {cnpjError && (
              <p className="text-xs text-destructive flex items-center gap-1">
                <AlertCircle className="h-3 w-3" /> {cnpjError}
              </p>
            )}
          </div>
          <Button type="button" variant="outline" onClick={handleBuscarCnpj} disabled={loadingCnpj}>
            {loadingCnpj ? <Loader2 className="h-4 w-4 animate-spin" /> : <Search className="h-4 w-4" />}
            {loadingCnpj ? "Buscando..." : "Buscar"}
          </Button>
        </div>

        {/* Documentos para enviar no WhatsApp (após a mensagem) */}
        <section className="space-y-4">
          <h3 className="text-sm font-semibold font-display border-b-2 border-primary-icon/30 pb-2">* Documentos para enviar no WhatsApp *</h3>
          <div className="space-y-2">
            <Label>Anexar documentos (serão enviados após a mensagem do formulário)</Label>
            <input
              ref={fileInputRef}
              type="file"
              multiple
              className="sr-only"
              accept=".pdf,.doc,.docx,.xls,.xlsx,.png,.jpg,.jpeg"
              onChange={(e) => {
                addAnexos(e.target.files);
                e.target.value = "";
              }}
            />
            <div
              role="button"
              tabIndex={0}
              onDragOver={(e) => { e.preventDefault(); setUploadDragOver(true); }}
              onDragLeave={() => setUploadDragOver(false)}
              onDrop={(e) => {
                e.preventDefault();
                setUploadDragOver(false);
                addAnexos(e.dataTransfer.files);
              }}
              onClick={() => fileInputRef.current?.click()}
              onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); fileInputRef.current?.click(); } }}
              className={`flex flex-col items-center justify-center gap-2 rounded-lg border-2 border-dashed p-6 transition-colors cursor-pointer min-h-[120px] ${
                uploadDragOver
                  ? "border-primary-icon bg-primary/10"
                  : "border-border hover:border-primary-icon/50 hover:bg-muted/30"
              }`}
            >
              <Upload className="h-10 w-10 text-muted-foreground" />
              <p className="text-sm text-muted-foreground text-center">
                Arraste os arquivos aqui ou <span className="text-primary-icon font-medium">clique para abrir o explorador</span>
              </p>
              <p className="text-xs text-muted-foreground">PDF, DOC, XLS, imagens (PNG, JPG)</p>
            </div>
            {anexos.length > 0 && (
              <ul className="space-y-1 mt-2">
                {anexos.map((file, idx) => (
                  <li key={`${file.name}-${idx}`} className="flex items-center justify-between gap-2 py-1.5 px-2 rounded bg-muted/50 text-sm">
                    <span className="truncate">{file.name}</span>
                    <Button
                      type="button"
                      variant="ghost"
                      size="icon"
                      className="h-7 w-7 shrink-0 text-destructive hover:text-destructive"
                      onClick={(e) => { e.stopPropagation(); removeAnexo(idx); }}
                      title="Remover"
                    >
                      <X className="h-4 w-4" />
                    </Button>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </section>

        {/* Tipo de formulário */}
        <section className="space-y-4">
          <h3 className="text-sm font-semibold font-display border-b-2 border-primary-icon/30 pb-2">Tipo de formulário</h3>
          <div className="max-w-xs">
            <Select value={form.tipo_formulario || undefined} onValueChange={(v) => update("tipo_formulario", v)}>
              <SelectTrigger>
                <SelectValue placeholder="Selecione o tipo" />
              </SelectTrigger>
              <SelectContent>
                {TIPO_FORMULARIO.map((o) => (
                  <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </section>

        {/* 1) Identificação da Empresa */}
        <section className="space-y-4">
          <h3 className="text-sm font-semibold font-display border-b-2 border-primary-icon/30 pb-2">* 1. Identificação da Empresa *</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label>Razão Social</Label>
              <Input value={form.razao_social} onChange={(e) => update("razao_social", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label>CNPJ/CPF</Label>
              <Input
                value={formatCNPJOrCPF(form.cnpj)}
                onChange={(e) => update("cnpj", onlyDigits(e.target.value).slice(0, 14))}
                placeholder="00.000.000/0001-00 ou 000.000.000-00"
                className="font-mono"
              />
            </div>
            <div className="space-y-2">
              <Label>Qualificação do Plano</Label>
              <div className="flex items-center gap-2">
                {qualificacaoDisplay && (
                  <span className="text-xl shrink-0" title={qualificacaoDisplay.label}>
                    {qualificacaoDisplay.emoji}
                  </span>
                )}
                <Input
                  value={form.qualificacao_plano}
                  onChange={(e) => update("qualificacao_plano", e.target.value.toUpperCase())}
                  placeholder="BRONZE, PRATA, OURO ou DIAMANTE (preencha o honorário e dê Tab)"
                  className={qualificacaoDisplay ? qualificacaoDisplay.className + " font-semibold" : ""}
                />
              </div>
            </div>
            <div className="space-y-2">
              <Label>Data de Abertura</Label>
              <Input
                value={form.data_abertura}
                onChange={(e) => update("data_abertura", formatDataDDMMAAAA(e.target.value))}
                placeholder="DD/MM/AAAA"
                className="font-mono"
                maxLength={10}
              />
            </div>
            <div className="md:col-span-2 space-y-2">
              <Label>Tipo de Atividade</Label>
              <Input value={form.tipo_atividade} onChange={(e) => update("tipo_atividade", e.target.value)} />
            </div>
          </div>
        </section>

        {/* 2) Inscrições e Enquadramento */}
        <section className="space-y-4">
          <h3 className="text-sm font-semibold font-display border-b-2 border-primary-icon/30 pb-2">* 2. Inscrições e Enquadramento *</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label>Inscrição Estadual</Label>
              <Input value={form.inscricao_estadual} onChange={(e) => update("inscricao_estadual", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label>Inscrição Municipal</Label>
              <Input value={form.inscricao_municipal} onChange={(e) => update("inscricao_municipal", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label>Competência Inicial</Label>
              <Input
                value={form.competencia_inicial}
                onChange={(e) => update("competencia_inicial", formatCompetencia(e.target.value))}
                placeholder="MM/AAAA"
                className="font-mono"
                maxLength={7}
              />
            </div>
            <div className="space-y-2">
              <Label>Tributação</Label>
              <Input value={form.tributacao} onChange={(e) => update("tributacao", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label>Possui Substituição Tributária</Label>
              <Select value={form.possui_st} onValueChange={(v) => update("possui_st", v)}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  {SIM_NAO.map((o) => (
                    <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>Possui Retenção de Impostos</Label>
              <Select value={form.possui_retencao_impostos} onValueChange={(v) => update("possui_retencao_impostos", v)}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  {SIM_NAO.map((o) => (
                    <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        </section>

        {/* 3) Dados Societários e Contato */}
        <section className="space-y-4">
          <h3 className="text-sm font-semibold font-display border-b-2 border-primary-icon/30 pb-2">* 3. Dados Societários e Contato *</h3>
          <div className="space-y-4">
            {form.socios.map((socio, idx) => (
              <div key={idx} className="grid grid-cols-1 md:grid-cols-2 gap-4 p-3 rounded-lg border border-border/60 bg-muted/20">
                {form.socios.length > 1 && (
                  <div className="md:col-span-2 flex justify-between items-center">
                    <span className="text-xs font-medium text-muted-foreground">Sócio {idx + 1}</span>
                    <Button type="button" variant="ghost" size="sm" onClick={() => removeSocio(idx)} className="text-destructive hover:text-destructive">
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  </div>
                )}
                <div className="space-y-2">
                  <Label>Nome do Sócio</Label>
                  <Input value={socio.nome_socio} onChange={(e) => updateSocio(idx, "nome_socio", e.target.value)} />
                </div>
                <div className="space-y-2">
                  <Label>CPF do Sócio</Label>
                  <Input
                    value={socio.cpf_socio}
                    onChange={(e) => updateSocio(idx, "cpf_socio", formatCPF(onlyDigits(e.target.value).slice(0, 11)))}
                    placeholder="000.000.000-00"
                    className="font-mono"
                  />
                </div>
              </div>
            ))}
            <Button type="button" variant="outline" size="sm" onClick={addSocio} className="gap-1">
              <Plus className="h-4 w-4" /> Adicionar sócio
            </Button>
          </div>
          <div className="space-y-4 pt-2">
            <Label>Contatos</Label>
            {form.contatos.map((contato, idx) => (
              <div key={idx} className="rounded-lg border border-border/60 bg-muted/30 p-4 space-y-3">
                <div className="flex items-center justify-between gap-2">
                  <span className="text-xs font-medium text-muted-foreground">Contato {form.contatos.length > 1 ? idx + 1 : ""}</span>
                  {form.contatos.length > 1 && (
                    <Button type="button" variant="ghost" size="icon" onClick={() => removeContato(idx)} title="Remover contato" className="text-destructive hover:text-destructive h-8 w-8">
                      <Trash2 className="h-4 w-4" />
                    </Button>
                  )}
                </div>
                <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                  <div className="space-y-2 md:col-span-2">
                    <Label>Nome do Contato Responsável</Label>
                    <div className="flex gap-2 items-center">
                      <Input
                        value={contato.nome_contato}
                        onChange={(e) => updateContato(idx, "nome_contato", e.target.value)}
                        placeholder="Nome do responsável"
                        className="flex-1"
                      />
                      {idx === form.contatos.length - 1 && (
                        <Button type="button" variant="outline" size="icon" onClick={addContato} title="Adicionar contato">
                          <Plus className="h-4 w-4" />
                        </Button>
                      )}
                    </div>
                  </div>
                  <div className="space-y-2">
                    <Label>E-mail(s) do Contato</Label>
                    <Input
                      type="email"
                      value={contato.email_contato}
                      onChange={(e) => updateContato(idx, "email_contato", e.target.value)}
                      placeholder="email@exemplo.com"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>Telefone(s) do Contato</Label>
                    <Input
                      value={contato.telefone_contato}
                      onChange={(e) => updateContato(idx, "telefone_contato", formatTelefoneInput(e.target.value))}
                      placeholder="(00) 00000-0000"
                      className="font-mono"
                    />
                  </div>
                </div>
              </div>
            ))}
            {form.contatos.length <= 1 && (
              <Button type="button" variant="outline" size="sm" onClick={addContato} className="gap-1">
                <Plus className="h-4 w-4" /> Adicionar contato
              </Button>
            )}
          </div>
        </section>

        {/* 4) Kits de obrigações */}
        <section className="space-y-4">
          <h3 className="text-sm font-semibold font-display border-b-2 border-primary-icon/30 pb-2">* 4. Kits de obrigações *</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label>Possui Pró-labore</Label>
              <Select value={form.possui_prolabore} onValueChange={(v) => update("possui_prolabore", v)}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  {SIM_NAO.map((o) => (
                    <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            {form.possui_prolabore === "sim" && (
              <div className="space-y-2">
                <Label>Valor do Pró-labore (R$)</Label>
                <Input
                  value={formatCurrencyBRL(form.valor_prolabore)}
                  onChange={(e) => update("valor_prolabore", currencyToDigits(e.target.value))}
                  placeholder="0,00"
                  className="font-mono"
                />
              </div>
            )}
            <div className="space-y-2">
              <Label>Possui Empregados</Label>
              <Select value={form.possui_empregados} onValueChange={(v) => update("possui_empregados", v)}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  {SIM_NAO.map((o) => (
                    <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label>Possui Contabilidade</Label>
              <Select value={form.possui_contabilidade} onValueChange={(v) => update("possui_contabilidade", v)}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  {SIM_NAO.map((o) => (
                    <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            {form.possui_contabilidade === "sim" && (
              <div className="space-y-2">
                <Label>Tipo de Contabilidade</Label>
                <Select value={form.tipo_contabilidade} onValueChange={(v) => update("tipo_contabilidade", v)}>
                  <SelectTrigger><SelectValue placeholder="Selecione" /></SelectTrigger>
                  <SelectContent>
                    {TIPO_CONTABILIDADE.map((o) => (
                      <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            )}
            <div className="space-y-2">
              <Label>Regime Contábil</Label>
              <Input value={form.regime_contabil} onChange={(e) => update("regime_contabil", e.target.value)} />
            </div>
            <div className="space-y-2">
              <Label>Possui Parcelamento</Label>
              <Select value={form.possui_parcelamento} onValueChange={(v) => update("possui_parcelamento", v)}>
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  {SIM_NAO.map((o) => (
                    <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            {form.possui_parcelamento === "sim" && (
              <div className="space-y-2 md:col-span-2">
                <Label>Tipos de Parcelamento</Label>
                {form.tipos_parcelamento.map((valor, idx) => (
                  <div key={idx} className="flex flex-wrap items-center gap-2">
                    <Popover>
                      <PopoverTrigger asChild>
                        <Button
                          variant="outline"
                          role="combobox"
                          className="flex-1 min-w-[200px] justify-between font-normal"
                        >
                          {valor || "Buscar ou selecionar..."}
                          <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                        </Button>
                      </PopoverTrigger>
                      <PopoverContent className="w-[var(--radix-popover-trigger-width)] p-0" align="start">
                        <Command>
                          <CommandInput placeholder="Buscar parcelamento..." />
                          <CommandList>
                            <CommandEmpty>Nenhum encontrado.</CommandEmpty>
                            <CommandGroup>
                              {OPCOES_PARCELAMENTO.map((o) => (
                                <CommandItem
                                  key={o.value}
                                  value={o.label}
                                  onSelect={() => updateParcelamento(idx, o.label)}
                                >
                                  {o.label}
                                </CommandItem>
                              ))}
                            </CommandGroup>
                          </CommandList>
                        </Command>
                      </PopoverContent>
                    </Popover>
                    {form.tipos_parcelamento.length > 1 && (
                      <Button
                        type="button"
                        variant="ghost"
                        size="icon"
                        className="shrink-0 text-destructive hover:text-destructive"
                        onClick={() => removeParcelamento(idx)}
                        title="Remover"
                      >
                        <Trash2 className="h-4 w-4" />
                      </Button>
                    )}
                  </div>
                ))}
                <Button type="button" variant="outline" size="sm" onClick={addParcelamento} className="gap-1">
                  <Plus className="h-4 w-4" /> Adicionar parcelamento
                </Button>
                <p className="text-xs text-muted-foreground">Digite para buscar ou escolha uma opcao.</p>
              </div>
            )}
            <div className="space-y-2">
              <Label>Contador Responsavel</Label>
              <Select
                value={form.contador_responsavel_cpf || "none"}
                onValueChange={(v) => {
                  const cpf = v === "none" ? "" : v;
                  const accountant = accountants.find((row) => row.cpf === cpf);
                  update("contador_responsavel_cpf", cpf);
                  update("contador_responsavel_nome", accountant?.name ?? "");
                }}
              >
                <SelectTrigger className="min-h-10 [&>span]:line-clamp-none [&>span]:whitespace-normal [&>span]:text-left py-2">
                  <SelectValue placeholder="Selecione o contador" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">Nenhum</SelectItem>
                  {accountants.map((contador) => (
                    <SelectItem key={contador.cpf} value={contador.cpf}>
                      {contador.name} - CPF {formatCpf(contador.cpf)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        </section>

        {/* 5) Honorários */}
        <section className="space-y-4">
          <h3 className="text-sm font-semibold font-display border-b-2 border-primary-icon/30 pb-2">* 5. Honorários *</h3>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label>Valor do Honorário Mensal (R$)</Label>
              <Input
                value={formatCurrencyBRL(form.valor_honorario)}
                onChange={(e) => update("valor_honorario", currencyToDigits(e.target.value))}
                onBlur={handleHonorarioBlur}
                placeholder="0,00"
                className="font-mono"
              />
              {qualificacaoLoading && (
                <p className="text-xs text-muted-foreground flex items-center gap-1">
                  <Loader2 className="h-3 w-3 animate-spin" /> Consultando salário mínimo (BCB)...
                </p>
              )}
            </div>
            <div className="space-y-2">
              <Label>Data de Vencimento do Honorário (dia)</Label>
              <Input value={form.vencimento_honorario} onChange={(e) => update("vencimento_honorario", e.target.value)} placeholder="Ex: 20" />
            </div>
            <div className="space-y-2">
              <Label>Data do Primeiro Honorário</Label>
              <Input
                value={form.data_primeiro_honorario}
                onChange={(e) => update("data_primeiro_honorario", formatDataDDMMAAAA(e.target.value))}
                placeholder="DD/MM/AAAA"
                className="font-mono"
                maxLength={10}
              />
            </div>
          </div>
        </section>

        {/* Observação */}
        <section className="space-y-4">
          <h3 className="text-sm font-semibold font-display border-b-2 border-primary-icon/30 pb-2">* Observação *</h3>
          <div className="space-y-2">
            <Label>Observação (texto livre)</Label>
            <Textarea
              value={form.observacao}
              onChange={(e) => update("observacao", e.target.value)}
              placeholder="Informações adicionais em texto livre..."
              className="min-h-[100px]"
            />
          </div>
        </section>

        <div className="pt-4 border-t flex flex-wrap items-center gap-2">
          <Button type="button" onClick={handleFinalizado} disabled={waLoading}>
            {waLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Send className="h-4 w-4" />}
            Finalizado
          </Button>
        </div>
      </GlassCard>

      {/* Seção WhatsApp: conexão, QR, grupos, envio ao clicar Finalizado */}
      <GlassCard className="p-6 space-y-4">
        <h3 className="text-sm font-semibold font-display">WhatsApp</h3>
        {waConnected === null && (
          <p className="text-sm text-muted-foreground">Verificando conexão...</p>
        )}
        {waConnected === false && (
          <div className="space-y-2">
            {waApiError && (
              <p className="text-sm text-amber-600 dark:text-amber-400 flex items-center gap-1">
                <AlertCircle className="h-4 w-4 shrink-0" /> {waApiError}
              </p>
            )}
            <div className="flex flex-wrap items-center gap-2">
              <Button
                type="button"
                variant="default"
                size="sm"
                disabled={waConnecting}
                onClick={async () => {
                  setWaConnecting(true);
                  setWaError("");
                  try {
                    const r = await connectWhatsApp();
                    if (r.ok) {
                      const s = await getConnectionStatus();
                      setWaConnected(s.connected);
                      if (s.connected) setWaQr(null);
                      if (!s.connected) toast.info("Cliente iniciando. Aguarde o QR ou a reconexão pela sessão.");
                    } else toast.error(r.error || "Falha ao conectar");
                  } finally {
                    setWaConnecting(false);
                  }
                }}
              >
                {waConnecting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Link className="h-4 w-4" />}
                Conectar
              </Button>
            </div>
            <p className="text-sm text-muted-foreground">
              Escaneie o QR Code com o WhatsApp (Dispositivos conectados). O QR é renovado automaticamente quando expirar. Ao clicar em Desconectar, um novo QR será gerado para reconectar.
            </p>
            {waQr ? (
              <img
                src={waQr}
                alt="QR Code WhatsApp Web"
                className="w-[280px] h-[280px] border border-border rounded-lg bg-white object-contain p-2"
                style={{ imageRendering: "crisp-edges" }}
              />
            ) : (
              <div className="w-64 h-64 border border-dashed border-border rounded-lg flex flex-col items-center justify-center gap-2 bg-muted/30 p-4">
                <QrCode className="h-12 w-12 text-muted-foreground" />
                <p className="text-xs text-muted-foreground text-center">Aguardando QR. O backend renova o QR a cada ~50s. Se acabou de conectar, aguarde; se a sessão já existia, a conexão pode aparecer em instantes.</p>
              </div>
            )}
          </div>
        )}
        {waConnected === true && (
          <div className="space-y-2">
            <div className="flex flex-wrap items-center gap-2">
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={waDisconnecting}
                onClick={async () => {
                  setWaDisconnecting(true);
                  setWaError("");
                  try {
                    const r = await disconnectWhatsApp();
                    if (r.ok) {
                      setWaConnected(false);
                      setWaQr(null);
                      setWaGroups([]);
                      setWaGroupId("");
                      lastQrFetchTime.current = 0;
                      waQrRef.current = null;
                      toast.success("Desconectado. Gerando novo QR para reconectar...");
                      // Solicita novo QR ao backend (ou aguarda restart do PM2); o poll vai exibir o QR em seguida
                      setTimeout(() => {
                        connectWhatsApp().then(() => {});
                      }, 2000);
                    } else toast.error(r.error || "Falha ao desconectar");
                  } finally {
                    setWaDisconnecting(false);
                  }
                }}
              >
                {waDisconnecting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Unlink className="h-4 w-4" />}
                Desconectar
              </Button>
            </div>
            <p className="text-sm text-muted-foreground">Conectado. Selecione o grupo e clique em &quot;Finalizado&quot; para enviar o formulário.</p>
            <div className="flex flex-wrap items-center gap-2">
              <Select
                value={waGroupId}
                onValueChange={(id) => {
                  setWaGroupId(id);
                  if (id) localStorage.setItem(waGroupStorageKey, id);
                }}
              >
                <SelectTrigger className="w-[280px]"><SelectValue placeholder="Selecione um grupo" /></SelectTrigger>
                <SelectContent>
                  {waGroups.length === 0 ? (
                    <SelectItem value="_empty" disabled>Nenhum grupo disponível</SelectItem>
                  ) : (
                    waGroups.map((g) => (
                      <SelectItem key={g.id} value={g.id}>{g.name}</SelectItem>
                    ))
                  )}
                </SelectContent>
              </Select>
              <Button
                type="button"
                variant="outline"
                size="sm"
                disabled={waGroupsLoading}
                onClick={async () => {
                  setWaGroupsLoading(true);
                  try {
                    const g = await getGroups(true);
                    setWaGroups(g);
                    const saved = localStorage.getItem(waGroupStorageKey);
                    if (saved && g.some((gr) => gr.id === saved)) setWaGroupId(saved);
                    if (g.length > 0) toast.success(`${g.length} grupo(s) carregado(s).`);
                    else toast.info("Nenhum grupo encontrado. Aguarde a sincronização ou tente novamente.");
                  } finally {
                    setWaGroupsLoading(false);
                  }
                }}
              >
                {waGroupsLoading ? <Loader2 className="h-4 w-4 animate-spin" /> : <RefreshCw className="h-4 w-4" />}
                Atualizar grupos
              </Button>
            </div>
            {waError && (
              <p className="text-xs text-destructive flex items-center gap-1">
                <AlertCircle className="h-3 w-3" /> {waError}
              </p>
            )}
          </div>
        )}
      </GlassCard>
    </div>
  );
}
