import { useState, useMemo, useRef, useEffect } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { Link, useSearchParams } from "react-router-dom"
import { getCompaniesForUser, updateCompany, getCompanyRobotConfigs, upsertCompanyRobotConfig, type RobotCompanyConfigInput } from "@/services/companiesService"
import { supabase } from "@/services/supabaseClient"
import { findAccountantByCpf, formatCpf, getAccountants, createAccountant, updateAccountant, deleteAccountant } from "@/services/accountantsService"
import type { Company } from "@/services/profilesService"
import { GlassCard } from "@/components/dashboard/GlassCard"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from "@/components/ui/dialog"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Search, Pencil, Plus, Building2, Loader2, Upload, Users, ArrowRightLeft, Trash2 } from "lucide-react"
import { cn } from "@/utils"
import { getPfxInfo } from "@/lib/validatePfxPassword"
import { toast } from "sonner"
import { getRobots } from "@/services/robotsService"
import { CompanyRobotsEditor } from "@/components/companies/CompanyRobotsEditor"
import { sanitizeRobotConfigForCompany } from "@/lib/companyRobotRequirements"
import { getBrazilStates, getCitiesByState } from "@/services/ibgeLocationsService"
import type { IbgeCity } from "@/services/ibgeLocationsService"
import { DataPagination } from "@/components/common/DataPagination"
import { Progress } from "@/components/ui/progress"
import { createCompany } from "@/services/companiesService"
import { fetchCnpjPublica } from "@/services/cnpjPublicaService"
import * as XLSX from "xlsx"

const BULK_API_DELAY_MS = 1800

type FilterStatus = "active" | "inactive" | "all"

type CompanyWithCert = Company & { auth_mode?: string | null; cert_blob_b64?: string | null; cert_password?: string | null; cert_valid_until?: string | null; contador_nome?: string | null; contador_cpf?: string | null; state_registration?: string | null; state_code?: string | null; city_name?: string | null }

function onlyDigits(s: string) {
  return s.replace(/\D/g, "")
}

/** Aceita texto com ou sem acento (ex.: cidade); retorna normalizado para uso interno (trim). */
function normalizeTextForImport(s: string): string {
  return s.trim()
}

function formatCnpjDigits(d: string) {
  if (d.length !== 14) return d
  return `${d.slice(0, 2)}.${d.slice(2, 5)}.${d.slice(5, 8)}/${d.slice(8, 12)}-${d.slice(12)}`
}

function fileToBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader()
    reader.onload = () => {
      const result = reader.result
      if (result instanceof ArrayBuffer) {
        const bytes = new Uint8Array(result)
        let binary = ""
        for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i])
        resolve(btoa(binary))
      } else reject(new Error("Leitura do arquivo falhou"))
    }
    reader.onerror = () => reject(reader.error)
    reader.readAsArrayBuffer(file)
  })
}

const BULK_CSV_HEADERS = ["nome", "cnpj", "ie", "cae", "estado", "municipio", "ativo", "contador_cpf"] as const
/** Primeira linha de exemplo no modelo CSV (após o cabeçalho) para o cliente saber como preencher. */
const BULK_CSV_EXAMPLE_ROW = "Exemplo Empresa Ltda;00.000.000/0001-00;123456789;12345;SP;São Paulo;S;000.000.000-00"
const BULK_CSV_TEMPLATE = [BULK_CSV_HEADERS.join(";"), BULK_CSV_EXAMPLE_ROW].join("\n")

function downloadBulkExcelTemplate() {
  const wb = XLSX.utils.book_new()
  const headers = [...BULK_CSV_HEADERS]
  const exampleRow = ["Exemplo Empresa Ltda", "00000000000100", "123456789", "12345", "SP", "Sao Paulo", "S", "00000000000"]
  const ws = XLSX.utils.aoa_to_sheet([headers, exampleRow])
  XLSX.utils.book_append_sheet(wb, ws, "Empresas")
  XLSX.writeFile(wb, "modelo_empresas.xlsx")
}

/** Remove acentos para comparação (ex.: Goiânia === Goiania). */
function normalizeForMatch(s: string): string {
  return s
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .toLowerCase()
    .trim()
}

/** Encontra nome oficial da cidade na lista IBGE (aceita sem acento). */
function matchCityIbge(cityName: string, ibgeCities: Array<{ name: string }>): string | null {
  if (!cityName.trim()) return null
  const normalized = normalizeForMatch(cityName)
  const found = ibgeCities.find((c) => normalizeForMatch(c.name) === normalized)
  if (found) return found.name
  const partial = ibgeCities.find((c) => normalizeForMatch(c.name).includes(normalized) || normalized.includes(normalizeForMatch(c.name)))
  return partial ? partial.name : null
}

function parseCsvToRows(text: string): Record<string, string>[] {
  const lines = text.split(/\r?\n/).map((l) => l.trim()).filter(Boolean)
  if (lines.length < 2) return []
  const headerLine = lines[0]
  const sep = headerLine.includes(";") ? ";" : ","
  const headers = parseCsvLine(headerLine, sep)
  const rows: Record<string, string>[] = []
  for (let i = 1; i < lines.length; i++) {
    const values = parseCsvLine(lines[i], sep)
    const row: Record<string, string> = {}
    headers.forEach((h, j) => {
      row[normalizeHeaderKey(h)] = values[j]?.trim() ?? ""
    })
    rows.push(row)
  }
  return rows
}

function parseCsvLine(line: string, sep: string): string[] {
  const out: string[] = []
  let cur = ""
  let inQuotes = false
  for (let i = 0; i < line.length; i++) {
    const c = line[i]
    if (c === '"') {
      inQuotes = !inQuotes
    } else if (inQuotes) {
      cur += c
    } else if (c === sep) {
      out.push(cur)
      cur = ""
    } else {
      cur += c
    }
  }
  out.push(cur)
  return out
}

function normalizeHeaderKey(h: string): string {
  return String(h ?? "").toLowerCase().replace(/\s+/g, "_").replace(/[^a-z0-9_]/g, "")
}

async function parseExcelToRows(file: File): Promise<Record<string, string>[]> {
  const buf = await file.arrayBuffer()
  const wb = XLSX.read(buf, { type: "array" })
  const firstSheetName = wb.SheetNames[0]
  if (!firstSheetName) return []
  const sheet = wb.Sheets[firstSheetName]
  const data = XLSX.utils.sheet_to_json<string[]>(sheet, { header: 1, defval: "" }) as string[][]
  if (data.length < 2) return []
  const headers = (data[0] ?? []).map((h) => normalizeHeaderKey(String(h)))
  const rows: Record<string, string>[] = []
  for (let i = 1; i < data.length; i++) {
    const values = data[i] ?? []
    const row: Record<string, string> = {}
    headers.forEach((h, j) => {
      row[h] = String(values[j] ?? "").trim()
    })
    rows.push(row)
  }
  return rows
}

export type BulkCompanyPayload = {
  name: string
  document?: string | null
  state_registration?: string | null
  state_code?: string | null
  city_name?: string | null
  cae?: string | null
  contador_cpf?: string | null
  active?: boolean
}

/** Aceita CPF/CNPJ sem pontuação (só dígitos). Nome pode vir vazio se houver CNPJ (14 dígitos) — a importação buscará na Receita. */
function bulkRowToCompanyPayload(row: Record<string, string>): BulkCompanyPayload | null {
  const nome = (row.nome ?? row.name ?? row.razao_social ?? "").trim()
  const doc = onlyDigits(row.cnpj ?? row.document ?? "")
  if (!nome && doc.length !== 14) return null
  const ie = (row.ie ?? row.state_registration ?? "").trim() || undefined
  const cae = (row.cae ?? "").trim() || undefined
  const estado = (row.estado ?? row.state_code ?? "").trim().toUpperCase().slice(0, 2) || undefined
  const municipioRaw = (row.municipio ?? row.city_name ?? row.cidade ?? "").trim()
  const municipio = municipioRaw ? normalizeTextForImport(municipioRaw) : undefined
  const ativoRaw = (row.ativo ?? row.active ?? "S").trim().toUpperCase()
  const active = ativoRaw !== "N" && ativoRaw !== "0" && ativoRaw !== "FALSE" && ativoRaw !== "NAO"
  const contadorCpfRaw = onlyDigits(row.contador_cpf ?? "")
  const contadorCpf = contadorCpfRaw.length === 11 ? contadorCpfRaw : undefined
  return {
    name: nome,
    document: doc.length === 14 ? doc : undefined,
    state_registration: ie || undefined,
    state_code: estado,
    city_name: municipio,
    cae,
    contador_cpf: contadorCpf ?? undefined,
    active,
  }
}

export default function EmpresasPage() {
  const queryClient = useQueryClient()
  const [searchParams, setSearchParams] = useSearchParams()
  const [filter, setFilter] = useState<FilterStatus>("active")
  const [search, setSearch] = useState("")
  const [editingCompany, setEditingCompany] = useState<Company | null>(null)
  const [editName, setEditName] = useState("")
  const [editDocument, setEditDocument] = useState("")
  const [editStateRegistration, setEditStateRegistration] = useState("")
  const [editStateCode, setEditStateCode] = useState("")
  const [editCityName, setEditCityName] = useState("")
  const [editCae, setEditCae] = useState("")
  const [editActive, setEditActive] = useState(true)
  const [editUseCertificate, setEditUseCertificate] = useState(false)
  const [editCertReplacing, setEditCertReplacing] = useState(false)
  const [editCertFile, setEditCertFile] = useState<File | null>(null)
  const [editCertPassword, setEditCertPassword] = useState("")
  const editCertInputRef = useRef<HTMLInputElement>(null)
  const [editContadorCpf, setEditContadorCpf] = useState<string>("")
  const [editSaving, setEditSaving] = useState(false)
  const [editError, setEditError] = useState("")
  const [selectedRobotTechnicalId, setSelectedRobotTechnicalId] = useState("")
  const [editRobotConfigs, setEditRobotConfigs] = useState<Record<string, RobotCompanyConfigInput>>({})
  const [tablePageSize, setTablePageSize] = useState(10)
  const [tablePage, setTablePage] = useState(1)
  const [bulkOpen, setBulkOpen] = useState(false)
  const [bulkFile, setBulkFile] = useState<File | null>(null)
  const [bulkRows, setBulkRows] = useState<Record<string, string>[]>([])
  const [bulkLoading, setBulkLoading] = useState(false)
  const [bulkError, setBulkError] = useState("")
  const [bulkResult, setBulkResult] = useState<{
    created: number
    errors: number
    messages: string[]
    duplicates: Array<{ cnpj: string; name?: string }>
    cnpjNotFound: string[]
    cpfContadorNotCadastrado: Array<{ cpf: string; empresa: string }>
  } | null>(null)
  const [bulkProgress, setBulkProgress] = useState<{
    current: number
    total: number
    phase: "fetching" | "inserting"
    log: Array<{ type: "ok" | "error"; msg: string }>
  } | null>(null)
  const bulkInputRef = useRef<HTMLInputElement>(null)
  const [contadoresOpen, setContadoresOpen] = useState(false)
  const [contadorNewName, setContadorNewName] = useState("")
  const [contadorNewCpf, setContadorNewCpf] = useState("")
  const [contadorSaving, setContadorSaving] = useState(false)
  const [contadorError, setContadorError] = useState("")
  const [contadorEditingId, setContadorEditingId] = useState<string | null>(null)
  const [contadorEditName, setContadorEditName] = useState("")
  const [contadorEditActive, setContadorEditActive] = useState(true)
  const [contadorExcludeId, setContadorExcludeId] = useState<string | null>(null)
  const [moveStep, setMoveStep] = useState<null | "source" | "companies" | "dest">(null)
  const [moveSourceCpf, setMoveSourceCpf] = useState<string | null>(null)
  const [moveSelectedIds, setMoveSelectedIds] = useState<Set<string>>(new Set())
  const [moveCompaniesSearch, setMoveCompaniesSearch] = useState("")
  const [moveLastClickedIndex, setMoveLastClickedIndex] = useState<number | null>(null)
  const [moveDestCpf, setMoveDestCpf] = useState<string | null>(null)
  const [moveSaving, setMoveSaving] = useState(false)
  const moveCompaniesListRef = useRef<HTMLDivElement>(null)

  const { data: companies = [], isLoading } = useQuery({
    queryKey: ["companies-list", filter],
    queryFn: () => getCompaniesForUser(filter === "all" ? "all" : filter === "active" ? "active" : "inactive"),
  })

  const { data: robots = [] } = useQuery({
    queryKey: ["admin-robots"],
    queryFn: getRobots,
  })
  const { data: accountants = [] } = useQuery({
    queryKey: ["accountants"],
    queryFn: () => getAccountants(true),
    staleTime: 30000,
  })
  const { data: accountantsAll = [], isLoading: accountantsAllLoading } = useQuery({
    queryKey: ["accountants", "all"],
    queryFn: () => getAccountants(false),
    enabled: contadoresOpen,
  })
  const { data: companiesForMove = [] } = useQuery({
    queryKey: ["companies-list", "all"],
    queryFn: () => getCompaniesForUser("all"),
    enabled: contadoresOpen && moveStep !== null,
  })
  const { data: states = [] } = useQuery({
    queryKey: ["ibge-states"],
    queryFn: getBrazilStates,
    staleTime: 24 * 60 * 60 * 1000,
  })
  const { data: cities = [] } = useQuery({
    queryKey: ["ibge-cities", editStateCode],
    queryFn: () => getCitiesByState(editStateCode),
    enabled: !!editStateCode,
    staleTime: 24 * 60 * 60 * 1000,
  })

  useEffect(() => {
    if (searchParams.get("editCompany") && filter !== "all") setFilter("all")
  }, [searchParams, filter])

  useEffect(() => {
    if (!selectedRobotTechnicalId && robots.length > 0) {
      setSelectedRobotTechnicalId(robots[0].technical_id)
    }
  }, [robots, selectedRobotTechnicalId])

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase()
    if (!q) return companies
    const digits = q.replace(/\D/g, "")
    return companies.filter(
      (c) =>
        c.name.toLowerCase().includes(q) ||
        (!!digits && (c.document ?? "").replace(/\D/g, "").includes(digits)) ||
        String((c as CompanyWithCert).state_registration ?? "").toLowerCase().includes(q) ||
        String((c as CompanyWithCert).contador_nome ?? "").toLowerCase().includes(q) ||
        String((c as CompanyWithCert).state_code ?? "").toLowerCase().includes(q) ||
        String((c as CompanyWithCert).city_name ?? "").toLowerCase().includes(q)
    )
  }, [companies, search])

  const tablePagination = useMemo(() => {
    const total = filtered.length
    const totalPages = Math.max(1, Math.ceil(total / tablePageSize))
    const page = Math.min(tablePage, totalPages)
    const fromIndex = (page - 1) * tablePageSize
    const toIndex = Math.min(fromIndex + tablePageSize, total)
    return {
      total,
      totalPages,
      currentPage: page,
      from: total ? fromIndex + 1 : 0,
      to: toIndex,
      list: filtered.slice(fromIndex, toIndex),
    }
  }, [filtered, tablePageSize, tablePage])

  useEffect(() => {
    setTablePage(1)
  }, [filter, search])

  const openEdit = async (c: Company, options?: { focusCertificate?: boolean; renewMode?: boolean }) => {
    // Sempre busca o registro completo no banco ao abrir o modal.
    // Isso evita UI ficar com snapshot antigo (ex.: depois de imports via SQL Editor).
    let row = c as CompanyWithCert
    try {
      const { data, error } = await supabase
        .from("companies")
        .select("*")
        .eq("id", c.id)
        .single()
      if (!error && data) row = data as CompanyWithCert
    } catch {
      // fallback para o item da lista
    }

    setEditingCompany(row as unknown as Company)
    setEditName(row.name)
    setEditDocument(row.document ?? "")
    setEditStateRegistration(row.state_registration ?? "")
    setEditStateCode(row.state_code ?? "")
    setEditCityName(row.city_name ?? "")
    setEditCae((row as CompanyWithCert).cae ?? "")
    setEditActive(row.active !== false)
    const withCert = row
    const shouldFocusCertificate = options?.focusCertificate ?? false
    const hasCertificate = !!(withCert.cert_blob_b64 || withCert.auth_mode === "certificate")
    setEditUseCertificate(shouldFocusCertificate ? true : hasCertificate)
    setEditCertReplacing(Boolean(options?.renewMode && hasCertificate))
    setEditCertFile(null)
    setEditCertPassword("")
    setEditContadorCpf(withCert.contador_cpf ?? "")
    setEditError("")
    setSelectedRobotTechnicalId((current) => current || robots[0]?.technical_id || "")
    try {
      const configs = await getCompanyRobotConfigs(row.id)
      const configsByRobot = Object.fromEntries(
        configs.map((config) => [
          config.robot_technical_id,
          {
            enabled: config.enabled,
            auth_mode: config.auth_mode ?? "password",
            nfs_password: config.nfs_password ?? null,
            selected_login_cpf: config.selected_login_cpf ?? null,
          } satisfies RobotCompanyConfigInput,
        ])
      )
      setEditRobotConfigs(configsByRobot)
    } catch {
      setEditRobotConfigs({})
    }
  }

  useEffect(() => {
    const targetCompanyId = searchParams.get("editCompany")
    if (!targetCompanyId || isLoading || companies.length === 0 || editingCompany) return

    const company = companies.find((item) => item.id === targetCompanyId)
    if (!company) return

    const focusCertificate = searchParams.get("focus") === "certificate"
    const renewMode = searchParams.get("mode") === "renew"
    void openEdit(company, { focusCertificate, renewMode })

    const nextParams = new URLSearchParams(searchParams)
    nextParams.delete("editCompany")
    nextParams.delete("focus")
    nextParams.delete("mode")
    setSearchParams(nextParams, { replace: true })
  }, [searchParams, setSearchParams, companies, isLoading, editingCompany])

  const handleSave = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!editingCompany) return
    setEditError("")
    if (editUseCertificate && (editCertFile || editCertPassword) && (!editCertFile || !editCertPassword.trim())) {
      setEditError("Selecione o arquivo .pfx e informe a senha do certificado.")
      return
    }
    setEditSaving(true)
    try {
      const updates: Parameters<typeof updateCompany>[1] = {
        name: editName.trim(),
        document: editDocument.trim() || null,
        state_registration: editStateRegistration.trim() || null,
        state_code: editStateCode || null,
        city_name: editCityName || null,
        cae: editCae.trim() || null,
        active: editActive,
      }
      if (!editUseCertificate) {
        updates.auth_mode = null
        updates.cert_blob_b64 = null
        updates.cert_password = null
        updates.cert_valid_until = null
      } else if (editCertFile && editCertPassword.trim()) {
        updates.auth_mode = "certificate"
        const b64 = await fileToBase64(editCertFile)
        const pwd = editCertPassword.trim()
        const info = getPfxInfo(b64, pwd)
        if (!info.valid) {
          setEditError("Senha do certificado incorreta. Não foi possível salvar.")
          toast.error("Senha do certificado incorreta.")
          return
        }
        const docDigits = onlyDigits(editDocument)
        if (docDigits.length !== 14) {
          setEditError("Para vincular o certificado corretamente, informe um CNPJ válido (14 dígitos) antes de enviar o .pfx.")
          toast.error("Informe um CNPJ válido antes de enviar o certificado.")
          return
        }
        if (info.cnpj && info.cnpj !== docDigits) {
          setEditError(`CNPJ do certificado (${formatCnpjDigits(info.cnpj)}) não corresponde ao CNPJ da empresa (${formatCnpjDigits(docDigits)}). Não foi possível salvar.`)
          toast.error("CNPJ do certificado não corresponde ao da empresa.")
          return
        }
        updates.cert_blob_b64 = b64
        updates.cert_password = pwd
        updates.cert_valid_until = info.validUntil ?? null
      } else if (editUseCertificate && (editingCompany as CompanyWithCert).cert_blob_b64) {
        updates.auth_mode = "certificate"
        // mantém certificado existente (não envia cert_blob_b64/cert_password)
      }
      const contador = findAccountantByCpf(accountants, editContadorCpf)
      updates.contador_nome = contador ? contador.name : null
      updates.contador_cpf = editContadorCpf || null
      await updateCompany(editingCompany.id, updates)
      await Promise.all(
        robots.map((robot) => {
          const rawConfig = editRobotConfigs[robot.technical_id] ?? {
            enabled: false,
            auth_mode: "password" as const,
            nfs_password: null,
            selected_login_cpf: null,
          }
          const config = sanitizeRobotConfigForCompany(robot.technical_id, rawConfig, editStateRegistration)
          return upsertCompanyRobotConfig(editingCompany.id, robot.technical_id, {
            enabled: config.enabled,
            auth_mode: config.auth_mode,
            nfs_password: config.auth_mode === "password" ? config.nfs_password ?? null : null,
            selected_login_cpf: config.selected_login_cpf ?? null,
          })
        })
      )
      queryClient.invalidateQueries({ queryKey: ["companies-list"] })
      queryClient.invalidateQueries({ queryKey: ["admin-companies"] })
      queryClient.invalidateQueries({ queryKey: ["admin-robots"] })
      setEditingCompany(null)
      toast.success("Empresa salva com sucesso. Certificado enviado ao Supabase.")
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : "Erro ao salvar"
      setEditError(msg)
      toast.error(msg)
    } finally {
      setEditSaving(false)
    }
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h1 className="text-2xl font-bold font-display tracking-tight">Empresas</h1>
          <p className="text-sm text-muted-foreground mt-1">Lista de empresas que você gerencia</p>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button type="button" variant="outline" className="gap-2" onClick={() => { setBulkOpen(true); setBulkFile(null); setBulkRows([]); setBulkError(""); setBulkResult(null); }}>
            <Upload className="h-4 w-4" />
            Importar em lote
          </Button>
          <Button type="button" variant="outline" className="gap-2" onClick={() => { setContadoresOpen(true); setContadorError(""); setContadorNewName(""); setContadorNewCpf(""); setContadorEditingId(null); }}>
            <Users className="h-4 w-4" />
            Gerenciar contadores
          </Button>
          <Link to="/empresas/nova">
            <Button className="gap-2">
              <Plus className="h-4 w-4" />
              Nova empresa
            </Button>
          </Link>
        </div>
      </div>

      <GlassCard className="overflow-hidden">
        <div className="p-4 border-b border-border space-y-4">
          <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
            <div className="relative flex-1">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
              <Input
                placeholder="Buscar por nome, CNPJ, IE ou contador..."
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                className="pl-9"
              />
            </div>
            <div className="flex rounded-lg border border-input bg-muted/30 p-0.5">
              {[
                { value: "active" as const, label: "Ativas" },
                { value: "all" as const, label: "Todas" },
                { value: "inactive" as const, label: "Inativas" },
              ].map(({ value, label }) => (
                <button
                  key={value}
                  type="button"
                  onClick={() => setFilter(value)}
                  className={cn(
                    "px-3 py-1.5 text-xs font-medium rounded-md transition-colors",
                    filter === value
                      ? "bg-background text-foreground shadow-sm"
                      : "text-muted-foreground hover:text-foreground"
                  )}
                >
                  {label}
                </button>
              ))}
            </div>
          </div>
        </div>
        <div className="divide-y divide-border">
          {isLoading ? (
            <div className="px-4 py-8 text-center text-muted-foreground text-sm">Carregando...</div>
          ) : filtered.length === 0 ? (
            <div className="px-4 py-8 text-center text-muted-foreground text-sm">
              {companies.length === 0
                ? "Nenhuma empresa encontrada para este filtro."
                : "Nenhum resultado para a busca."}
            </div>
          ) : (
            tablePagination.list.map((emp) => (
              <div
                key={emp.id}
                className="px-4 py-3 flex items-center justify-between gap-4 hover:bg-muted/30 transition-colors"
              >
                <div className="flex items-center gap-3 min-w-0">
                  <div className="rounded-lg bg-primary/10 p-2 flex-shrink-0">
                    <Building2 className="h-4 w-4 text-primary-icon" />
                  </div>
                  <div className="min-w-0">
                    <p className="text-sm font-medium truncate">{emp.name}</p>
                    <p className="text-xs text-muted-foreground">{emp.document ?? "—"}</p>
                    {(emp as CompanyWithCert).state_registration && (
                      <p className="text-xs text-muted-foreground">IE: {(emp as CompanyWithCert).state_registration}</p>
                    )}
                    {((emp as CompanyWithCert).state_code || (emp as CompanyWithCert).city_name) && (
                      <p className="text-xs text-muted-foreground">
                        Localidade: {[(emp as CompanyWithCert).city_name, (emp as CompanyWithCert).state_code].filter(Boolean).join(" - ")}
                      </p>
                    )}
                    {(emp as CompanyWithCert).contador_nome && (
                      <p className="text-xs text-muted-foreground mt-0.5">
                        Contador: {(emp as CompanyWithCert).contador_nome}
                      </p>
                    )}
                  </div>
                </div>
                <div className="flex items-center gap-2 flex-shrink-0">
                  <span
                    className={cn(
                      "text-xs font-medium px-2 py-0.5 rounded-full",
                      (emp as { active?: boolean }).active !== false
                        ? "bg-success/15 text-success"
                        : "bg-muted text-muted-foreground"
                    )}
                  >
                    {(emp as { active?: boolean }).active !== false ? "Ativa" : "Inativa"}
                  </span>
                  <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    className="gap-1.5"
                    onClick={() => openEdit(emp)}
                  >
                    <Pencil className="h-3.5 w-3.5" />
                    Editar
                  </Button>
                </div>
              </div>
            ))
          )}
        </div>
        {!isLoading && filtered.length > 0 && (
          <DataPagination
            currentPage={tablePagination.currentPage}
            totalPages={tablePagination.totalPages}
            totalItems={tablePagination.total}
            from={tablePagination.from}
            to={tablePagination.to}
            pageSize={tablePageSize}
            onPageChange={setTablePage}
            onPageSizeChange={(next) => { setTablePageSize(next); setTablePage(1); }}
          />
        )}
      </GlassCard>

      <Dialog open={!!editingCompany} onOpenChange={(open) => !open && !editSaving && setEditingCompany(null)}>
        <DialogContent aria-describedby={undefined} className="max-h-[90vh] overflow-y-auto">
          {editSaving && (
            <div className="absolute inset-0 z-50 flex items-center justify-center rounded-lg bg-background/80 backdrop-blur-sm">
              <div className="flex flex-col items-center gap-3">
                <Loader2 className="h-10 w-10 animate-spin text-primary-icon" />
                <p className="text-sm font-medium">Salvando empresa e enviando certificado ao Supabase...</p>
              </div>
            </div>
          )}
          <DialogHeader>
            <DialogTitle>Editar empresa</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleSave} className="space-y-4 min-w-0 overflow-hidden">
            <Tabs defaultValue="geral" className="space-y-4">
              <TabsList className="grid w-full grid-cols-3">
                <TabsTrigger value="geral">Geral</TabsTrigger>
                <TabsTrigger value="certificado">Certificado digital</TabsTrigger>
                <TabsTrigger value="robos">Robôs</TabsTrigger>
              </TabsList>
              <TabsContent value="geral" className="space-y-4">
            <div className="space-y-2">
              <Label>Nome</Label>
              <Input
                value={editName}
                onChange={(e) => setEditName(e.target.value)}
                required
                disabled={editSaving}
              />
            </div>
            <div className="space-y-2">
              <Label>Documento (CNPJ)</Label>
              <Input
                value={editDocument}
                onChange={(e) => setEditDocument(e.target.value)}
                disabled={editSaving}
                placeholder="00.000.000/0001-00"
              />
            </div>
            <div className="space-y-2">
              <Label>IE</Label>
              <Input
                value={editStateRegistration}
                onChange={(e) => setEditStateRegistration(e.target.value)}
                disabled={editSaving}
                placeholder="Inscrição estadual"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="edit-cae">CAE (Inscrição Municipal)</Label>
              <Input
                id="edit-cae"
                value={editCae}
                onChange={(e) => setEditCae(e.target.value)}
                disabled={editSaving}
                placeholder="Ex.: 2163519"
              />
              <p className="text-xs text-muted-foreground">Inscrição municipal (ex.: Prefeitura de Goiânia). Usado pelo robô de taxas para localizar a empresa no portal.</p>
            </div>
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="space-y-2">
                <Label>Estado</Label>
                <Select
                  value={editStateCode || "none"}
                  onValueChange={(value) => {
                    const nextState = value === "none" ? "" : value
                    setEditStateCode(nextState)
                    setEditCityName("")
                  }}
                  disabled={editSaving}
                >
                  <SelectTrigger>
                    <SelectValue placeholder="Selecione o estado" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="none">Não informado</SelectItem>
                    {states.map((state) => (
                      <SelectItem key={state.code} value={state.code}>
                        {state.code} - {state.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
              <div className="space-y-2">
                <Label>Município</Label>
                <Select
                  value={editCityName || "none"}
                  onValueChange={(value) => setEditCityName(value === "none" ? "" : value)}
                  disabled={editSaving || !editStateCode}
                >
                  <SelectTrigger>
                    <SelectValue placeholder={editStateCode ? "Selecione o município" : "Selecione o estado primeiro"} />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="none">Não informado</SelectItem>
                    {cities.map((city) => (
                      <SelectItem key={city.name} value={city.name}>
                        {city.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <input
                type="checkbox"
                id="emp-edit-active"
                checked={editActive}
                onChange={(e) => setEditActive(e.target.checked)}
                disabled={editSaving}
                className="rounded border-input"
              />
              <Label htmlFor="emp-edit-active">Ativa</Label>
            </div>
            <div className="space-y-2">
              <Label>Contador responsável</Label>
              <Select
                value={editContadorCpf || "none"}
                onValueChange={(v) => setEditContadorCpf(v === "none" ? "" : v)}
                disabled={editSaving}
              >
                <SelectTrigger className="min-h-10 [&>span]:line-clamp-none [&>span]:whitespace-normal [&>span]:text-left py-2">
                  <SelectValue placeholder="Selecione o contador" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="none">Nenhum</SelectItem>
                  {accountants.map((c) => (
                    <SelectItem key={c.cpf} value={c.cpf}>
                      {c.name} — CPF {formatCpf(c.cpf)}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
              </TabsContent>
              <TabsContent value="certificado" className="space-y-3">
            <div className="space-y-3">
              <div className="flex items-center gap-2">
                <input
                  type="checkbox"
                  id="emp-edit-cert"
                  checked={editUseCertificate}
                  onChange={(e) => {
                    setEditUseCertificate(e.target.checked)
                    if (!e.target.checked) setEditCertReplacing(false)
                  }}
                  disabled={editSaving}
                  className="rounded border-input"
                />
                <Label htmlFor="emp-edit-cert" className="font-normal cursor-pointer">Certificado digital (uso geral)</Label>
              </div>
              {editUseCertificate && (
                <div className="pl-4 border-l-2 border-border space-y-2">
                  {(!!(editingCompany as CompanyWithCert)?.cert_blob_b64 || (editingCompany as CompanyWithCert)?.auth_mode === "certificate") && !editCertReplacing ? (
                    <div className="space-y-1">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="text-sm text-muted-foreground">Certificado cadastrado</span>
                        <Button type="button" variant="outline" size="sm" onClick={() => { setEditCertReplacing(true); setEditCertFile(null); setEditCertPassword(""); editCertInputRef.current?.click(); }} disabled={editSaving}>
                          Substituir
                        </Button>
                        <Button type="button" variant="ghost" size="sm" onClick={() => setEditUseCertificate(false)} disabled={editSaving}>
                          Remover
                        </Button>
                      </div>
                      {(editingCompany as CompanyWithCert)?.cert_valid_until && (
                        <p className="text-sm text-green-600 dark:text-green-400 font-medium">
                          Certificado ativo — Válido até {(() => {
                            const [y, m, d] = (editingCompany as CompanyWithCert).cert_valid_until!.split("-")
                            return `${d}/${m}/${y}`
                          })()}
                        </p>
                      )}
                    </div>
                  ) : (
                    <>
                      <input
                        ref={editCertInputRef}
                        type="file"
                        accept=".pfx"
                        onChange={(e) => setEditCertFile(e.target.files?.[0] ?? null)}
                        className="hidden"
                      />
                      <div className="space-y-1">
                        <Label>Arquivo .pfx</Label>
                        <div className="flex gap-2 items-center min-w-0 overflow-hidden">
                          <Button type="button" variant="outline" size="sm" onClick={() => editCertInputRef.current?.click()} disabled={editSaving} className="w-full min-w-0 overflow-hidden justify-start text-left">
                            <span className="truncate block w-full" title={editCertFile?.name ?? undefined}>
                              {editCertFile ? editCertFile.name : "Selecionar"}
                            </span>
                          </Button>
                        </div>
                      </div>
                      <div className="space-y-1">
                        <Label>Senha do certificado</Label>
                        <Input
                          type="password"
                          value={editCertPassword}
                          onChange={(e) => setEditCertPassword(e.target.value)}
                          placeholder="Senha do .pfx"
                          disabled={editSaving}
                          autoComplete="off"
                        />
                      </div>
                      {(!!(editingCompany as CompanyWithCert)?.cert_blob_b64 || (editingCompany as CompanyWithCert)?.auth_mode === "certificate") && (
                        <Button type="button" variant="ghost" size="sm" onClick={() => { setEditCertReplacing(false); setEditCertFile(null); setEditCertPassword(""); }} disabled={editSaving}>
                          Manter certificado atual
                        </Button>
                      )}
                    </>
                  )}
                </div>
              )}
            </div>
              </TabsContent>
              <TabsContent value="robos" className="space-y-4">
                <div className="space-y-1">
                  <p className="text-sm font-medium">Robôs vinculados</p>
                  <p className="text-xs text-muted-foreground">
                    Escolha um robô para configurar esta empresa. Os logins globais do Sefaz Xml ficam no editar robô; aqui você só vincula o login correto à empresa.
                  </p>
                </div>
                <CompanyRobotsEditor
                  robots={robots}
                  accountants={accountants}
                  selectedRobotTechnicalId={selectedRobotTechnicalId}
                  onSelectedRobotTechnicalIdChange={setSelectedRobotTechnicalId}
                  configsByRobot={editRobotConfigs}
                  onConfigChange={(robotTechnicalId, next) =>
                    setEditRobotConfigs((current) => ({
                      ...current,
                      [robotTechnicalId]: next,
                    }))
                  }
                  contadorCpf={editContadorCpf}
                  stateRegistration={editStateRegistration}
                  disabled={editSaving}
                />
              </TabsContent>
            </Tabs>
            {editError && <p className="text-sm text-destructive">{editError}</p>}
            <DialogFooter>
              <Button
                type="button"
                variant="outline"
                onClick={() => setEditingCompany(null)}
                disabled={editSaving}
              >
                Cancelar
              </Button>
              <Button type="submit" disabled={editSaving}>
                {editSaving ? "Salvando..." : "Salvar"}
              </Button>
            </DialogFooter>
          </form>
        </DialogContent>
      </Dialog>

      <Dialog open={bulkOpen} onOpenChange={(open) => { if (!bulkLoading) setBulkOpen(open); }}>
        <DialogContent aria-describedby={undefined} className="max-w-2xl max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Importar empresas em lote</DialogTitle>
          </DialogHeader>
          <p className="text-sm text-muted-foreground">
            Envie um arquivo <strong>CSV</strong> ou <strong>Excel</strong> (.xlsx, .xls) com as colunas: <strong>nome</strong> (ou deixe vazio e informe CNPJ para buscar na Receita), cnpj, ie, cae, estado, municipio, ativo (S/N), contador_cpf.
            A importação <strong>adiciona</strong> empresas à sua lista (não substitui). CPF e CNPJ podem ser só dígitos. Estado e município vazios são preenchidos pela consulta ao CNPJ e gravados no formato IBGE. Remova a linha de exemplo antes de importar.
          </p>
          <div className="flex flex-wrap gap-2 items-center">
            <input
              ref={bulkInputRef}
              type="file"
              accept=".csv,.xlsx,.xls"
              className="hidden"
              onChange={async (e) => {
                const file = e.target.files?.[0]
                if (!file) return
                setBulkFile(file)
                setBulkError("")
                setBulkResult(null)
                const ext = file.name.toLowerCase().split(".").pop()
                try {
                  if (ext === "csv") {
                    const text = await new Promise<string>((res, rej) => {
                      const reader = new FileReader()
                      reader.onload = () => res(String(reader.result ?? ""))
                      reader.onerror = () => rej(reader.error)
                      reader.readAsText(file, "UTF-8")
                    })
                    setBulkRows(parseCsvToRows(text))
                  } else {
                    const rows = await parseExcelToRows(file)
                    setBulkRows(rows)
                  }
                } catch (err) {
                  setBulkError(err instanceof Error ? err.message : "Erro ao ler o arquivo.")
                }
                e.target.value = ""
              }}
            />
            <Button type="button" variant="outline" size="sm" onClick={() => bulkInputRef.current?.click()} disabled={bulkLoading}>
              {bulkFile ? bulkFile.name : "Selecionar planilha (CSV ou Excel)"}
            </Button>
            <a
              href={`data:text/csv;charset=utf-8,${encodeURIComponent(BULK_CSV_TEMPLATE)}`}
              download="modelo_empresas.csv"
              className="text-sm text-primary hover:underline"
            >
              Baixar modelo CSV
            </a>
            <Button type="button" variant="link" className="text-sm h-auto p-0" onClick={downloadBulkExcelTemplate}>
              Baixar modelo Excel
            </Button>
          </div>
          {bulkError && <p className="text-sm text-destructive">{bulkError}</p>}
          {bulkProgress && (
            <>
              <div className="space-y-3 py-2">
                <p className="text-sm font-medium">
                  {bulkProgress.phase === "fetching"
                    ? `Buscando dados na Receita (${bulkProgress.current} de ${bulkProgress.total})`
                    : `Inserindo empresas (${bulkProgress.current} de ${bulkProgress.total})`}
                </p>
                <Progress value={bulkProgress.total ? (100 * bulkProgress.current) / bulkProgress.total : 0} className="h-2" />
                <p className="text-xs text-muted-foreground">
                  {bulkProgress.phase === "fetching"
                    ? "Aguarde: intervalo entre consultas para não sobrecarregar a API."
                    : "Cadastrando empresas no sistema."}
                </p>
                <div className="border rounded-lg bg-muted/20 max-h-32 overflow-y-auto p-2 text-xs font-mono space-y-0.5">
                  {bulkProgress.log.slice(-20).map((entry, i) => (
                    <div key={i} className={cn(entry.type === "error" && "text-destructive")}>
                      {entry.type === "ok" ? "✓" : "✗"} {entry.msg}
                    </div>
                  ))}
                </div>
              </div>
            </>
          )}
          {bulkResult && !bulkProgress && (
            <>
              <div className="rounded-lg border bg-muted/30 p-3 text-sm space-y-3">
                <p className="font-medium">{bulkResult.created} empresa(s) criada(s) — adicionadas à sua lista (a importação não substitui o que já existe).</p>
                {bulkResult.errors > 0 && (
                  <>
                    <p className="text-destructive">{bulkResult.errors} erro(s) ao cadastrar:</p>
                    <ul className="list-disc list-inside text-muted-foreground">
                      {bulkResult.messages.slice(0, 10).map((m, i) => (
                        <li key={i}>{m}</li>
                      ))}
                      {bulkResult.messages.length > 10 && <li>… e mais {bulkResult.messages.length - 10}.</li>}
                    </ul>
                  </>
                )}
                {bulkResult.duplicates.length > 0 && (
                  <>
                    <p className="text-amber-600 dark:text-amber-500 font-medium">Duplicadas (não importadas — CNPJ já cadastrado):</p>
                    <ul className="list-disc list-inside text-muted-foreground">
                      {bulkResult.duplicates.slice(0, 15).map((d, i) => (
                        <li key={i}>{d.name ? `${d.name} — ` : ""}CNPJ {d.cnpj}</li>
                      ))}
                      {bulkResult.duplicates.length > 15 && <li>… e mais {bulkResult.duplicates.length - 15}.</li>}
                    </ul>
                  </>
                )}
                {bulkResult.cnpjNotFound.length > 0 && (
                  <>
                    <p className="text-amber-600 dark:text-amber-500 font-medium">CNPJ não encontrado na Receita/APIs (empresa criada com nome genérico):</p>
                    <ul className="list-disc list-inside text-muted-foreground">
                      {bulkResult.cnpjNotFound.slice(0, 15).map((cnpj, i) => (
                        <li key={i}>{cnpj}</li>
                      ))}
                      {bulkResult.cnpjNotFound.length > 15 && <li>… e mais {bulkResult.cnpjNotFound.length - 15}.</li>}
                    </ul>
                  </>
                )}
                {bulkResult.cpfContadorNotCadastrado.length > 0 && (
                  <>
                    <p className="text-amber-600 dark:text-amber-500 font-medium">CPF de contador não cadastrado (empresa importada; vincule o contador depois em Editar):</p>
                    <ul className="list-disc list-inside text-muted-foreground">
                      {bulkResult.cpfContadorNotCadastrado.slice(0, 10).map((x, i) => (
                        <li key={i}>CPF {x.cpf} — {x.empresa || "—"}</li>
                      ))}
                      {bulkResult.cpfContadorNotCadastrado.length > 10 && <li>… e mais {bulkResult.cpfContadorNotCadastrado.length - 10}.</li>}
                    </ul>
                  </>
                )}
              </div>
              <DialogFooter>
                <Button type="button" onClick={() => setBulkOpen(false)}>Fechar</Button>
              </DialogFooter>
            </>
          )}
          {bulkRows.length > 0 && !bulkResult && !bulkProgress && (
            <>
              <p className="text-sm text-muted-foreground">
                Preview ({bulkRows.length} linha(s)). A importação adiciona à lista (não substitui). Empresas com CNPJ já cadastrado serão ignoradas. CPF e CNPJ podem ser só dígitos; município aceita com ou sem acento.
              </p>
              <div className="border rounded-lg overflow-auto max-h-48">
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b bg-muted/50">
                      {BULK_CSV_HEADERS.map((h) => (
                        <th key={h} className="px-2 py-1.5 text-left font-medium">{h}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {bulkRows.slice(0, 10).map((row, i) => (
                      <tr key={i} className="border-b last:border-0">
                        {BULK_CSV_HEADERS.map((h) => (
                          <td key={h} className="px-2 py-1 truncate max-w-[120px]" title={row[h] ?? ""}>
                            {(row[h] ?? "").slice(0, 20)}{(row[h] ?? "").length > 20 ? "…" : ""}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <DialogFooter>
                <Button type="button" variant="outline" onClick={() => setBulkOpen(false)} disabled={bulkLoading}>
                  Fechar
                </Button>
                <Button
                  disabled={bulkLoading}
                  onClick={async () => {
                    const payloads = bulkRows
                      .map((r) => bulkRowToCompanyPayload(r))
                      .filter(Boolean) as BulkCompanyPayload[]
                    if (payloads.length === 0) {
                      setBulkError("Nenhuma linha válida: informe nome ou CNPJ com 14 dígitos.")
                      return
                    }
                    setBulkLoading(true)
                    setBulkError("")
                    setBulkProgress({ current: 0, total: payloads.length, phase: "fetching", log: [] })

                    const [existingCompanies, accountantsList] = await Promise.all([
                      getCompaniesForUser("all"),
                      getAccountants(false),
                    ])
                    const existingCnpjSet = new Set(existingCompanies.map((c) => onlyDigits(c.document ?? "")).filter(Boolean))
                    const accountantCpfs = new Set(accountantsList.map((a) => onlyDigits(a.cpf)))

                    let created = 0
                    const messages: string[] = []
                    const duplicates: Array<{ cnpj: string; name?: string }> = []
                    const cnpjNotFound: string[] = []
                    const cpfContadorNotCadastrado: Array<{ cpf: string; empresa: string }> = []

                    const delay = () => new Promise((r) => setTimeout(r, BULK_API_DELAY_MS))
                    const ibgeCitiesByState = new Map<string, IbgeCity[]>()

                    for (let i = 0; i < payloads.length; i++) {
                      const p = payloads[i]
                      const docDigits = p.document ? onlyDigits(p.document) : ""

                      if (docDigits && existingCnpjSet.has(docDigits)) {
                        duplicates.push({ cnpj: p.document!, name: p.name || undefined })
                        setBulkProgress((prev) =>
                          prev ? { ...prev, current: i + 1, phase: "inserting", log: [...prev.log, { type: "error" as const, msg: `Duplicado (já cadastrado): CNPJ ${p.document}` }] } : prev
                        )
                        continue
                      }

                      const needsFetch =
                        p.document &&
                        String(p.document).length === 14 &&
                        (!p.name?.trim() || !p.state_code?.trim() || !p.city_name?.trim())

                      if (needsFetch) {
                        setBulkProgress((prev) =>
                          prev
                            ? {
                                ...prev,
                                current: i + 1,
                                phase: "fetching",
                                log: [...prev.log, { type: "ok" as const, msg: `Buscando CNPJ ${p.document}...` }],
                              }
                            : prev
                        )
                        await delay()
                        try {
                          const data = await fetchCnpjPublica(p.document!)
                          if (data) {
                            if (!p.name?.trim()) p.name = data.razao_social?.trim() || data.nome_fantasia?.trim() || ""
                            if (!p.state_registration?.trim() && data.inscricao_estadual) p.state_registration = data.inscricao_estadual.trim()
                            if (!p.state_code?.trim() && data.state_code) p.state_code = data.state_code.trim().toUpperCase().slice(0, 2)
                            if (!p.city_name?.trim() && data.city_name) p.city_name = data.city_name.trim()
                            setBulkProgress((prev) =>
                              prev ? { ...prev, log: [...prev.log, { type: "ok" as const, msg: `${p.document}: ${p.name || "(sem nome)"}` }] } : prev
                            )
                          }
                        } catch (e) {
                          const msg = e instanceof Error ? e.message : String(e)
                          cnpjNotFound.push(p.document!)
                          setBulkProgress((prev) =>
                            prev ? { ...prev, log: [...prev.log, { type: "error" as const, msg: `CNPJ ${p.document}: ${msg}` }] } : prev
                          )
                          if (!p.name?.trim()) p.name = `Empresa CNPJ ${p.document}`
                        }
                      }

                      if (p.state_code?.trim() && p.city_name?.trim()) {
                        let cities = ibgeCitiesByState.get(p.state_code)
                        if (!cities) {
                          try {
                            cities = await getCitiesByState(p.state_code)
                            ibgeCitiesByState.set(p.state_code, cities)
                          } catch {
                            cities = []
                          }
                        }
                        const ibgeName = matchCityIbge(p.city_name, cities)
                        if (ibgeName) p.city_name = ibgeName
                      }

                      if (p.contador_cpf && !accountantCpfs.has(onlyDigits(p.contador_cpf))) {
                        cpfContadorNotCadastrado.push({ cpf: p.contador_cpf, empresa: p.name?.trim() || p.document || "" })
                      }

                      setBulkProgress((prev) =>
                        prev ? { ...prev, current: i + 1, phase: "inserting" } : prev
                      )
                      const nameToUse = p.name?.trim() || `Empresa CNPJ ${p.document || i + 1}`
                      try {
                        await createCompany({
                          name: nameToUse,
                          document: p.document ?? null,
                          state_registration: p.state_registration ?? null,
                          state_code: p.state_code ?? null,
                          city_name: p.city_name ?? null,
                          cae: p.cae ?? null,
                          contador_cpf: p.contador_cpf ?? null,
                          active: p.active ?? true,
                        })
                        created++
                        existingCnpjSet.add(docDigits)
                        setBulkProgress((prev) =>
                          prev ? { ...prev, log: [...prev.log, { type: "ok" as const, msg: `Criada: ${nameToUse}` }] } : prev
                        )
                      } catch (e) {
                        const msg = e instanceof Error ? e.message : String(e)
                        messages.push(`${nameToUse}: ${msg}`)
                        setBulkProgress((prev) =>
                          prev ? { ...prev, log: [...prev.log, { type: "error" as const, msg: `${nameToUse}: ${msg}` }] } : prev
                        )
                      }
                    }

                    setBulkResult({
                      created,
                      errors: messages.length,
                      messages,
                      duplicates,
                      cnpjNotFound,
                      cpfContadorNotCadastrado,
                    })
                    setBulkLoading(false)
                    setBulkProgress(null)
                    queryClient.invalidateQueries({ queryKey: ["companies-list"] })
                  }}
                >
                  {bulkLoading ? "Importando…" : `Confirmar importação (${bulkRows.map((r) => bulkRowToCompanyPayload(r)).filter(Boolean).length} empresa(s))`}
                </Button>
              </DialogFooter>
            </>
          )}
          {bulkRows.length === 0 && !bulkResult && (
            <DialogFooter>
              <Button type="button" variant="outline" onClick={() => setBulkOpen(false)}>
                Fechar
              </Button>
            </DialogFooter>
          )}
        </DialogContent>
      </Dialog>

      <Dialog open={contadoresOpen} onOpenChange={(open) => {
        if (!contadorSaving && !moveSaving) {
          setContadoresOpen(open)
          if (!open) { setMoveStep(null); setMoveSourceCpf(null); setMoveSelectedIds(new Set()); setMoveDestCpf(null); }
        }
      }}>
        <DialogContent aria-describedby={undefined} className="max-w-lg max-h-[90vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle>Contadores responsáveis</DialogTitle>
          </DialogHeader>
          <p className="text-sm text-muted-foreground">
            Contadores cadastrados aparecem ao escolher o responsável por uma empresa. Você pode editar, inativar e excluir qualquer um.
          </p>
          {moveStep !== null ? (
            <>
              {moveStep === "source" && (
                <>
                  <p className="text-sm font-medium">Selecione o contador de origem</p>
                  <Select value={moveSourceCpf ?? ""} onValueChange={setMoveSourceCpf}>
                    <SelectTrigger className="w-full"><SelectValue placeholder="Contador de origem" /></SelectTrigger>
                    <SelectContent>
                      {accountantsAll.map((a) => (
                        <SelectItem key={a.id} value={a.cpf}>{a.name} — {formatCpf(a.cpf)}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <div className="flex gap-2">
                    <Button variant="outline" size="sm" onClick={() => setMoveStep(null)}>Voltar</Button>
                    <Button size="sm" disabled={!moveSourceCpf} onClick={() => { setMoveStep("companies"); setMoveSelectedIds(new Set()); setMoveCompaniesSearch(""); setMoveLastClickedIndex(null); }}>Avançar</Button>
                  </div>
                </>
              )}
              {moveStep === "companies" && moveSourceCpf && (() => {
                const companiesFromSource = companiesForMove.filter((c) => onlyDigits((c as CompanyWithCert).contador_cpf ?? "") === onlyDigits(moveSourceCpf))
                const searchTerm = moveCompaniesSearch.trim()
                const searchLower = searchTerm.toLowerCase()
                const searchDigits = searchTerm.replace(/\D/g, "")
                const filteredCompanies = searchTerm
                  ? companiesFromSource.filter((c) => {
                      const nameMatch = searchLower ? c.name.toLowerCase().includes(searchLower) : false
                      const docMatch = searchDigits.length > 0 && (c.document ?? "").replace(/\D/g, "").includes(searchDigits)
                      return nameMatch || docMatch
                    })
                  : companiesFromSource
                const sourceAcc = accountantsAll.find((a) => onlyDigits(a.cpf) === onlyDigits(moveSourceCpf))
                const allFilteredSelected = filteredCompanies.length > 0 && filteredCompanies.every((c) => moveSelectedIds.has(c.id))
                const visibleIds = new Set(filteredCompanies.map((c) => c.id))
                return (
                  <>
                    <p className="text-sm font-medium">Empresas sob responsabilidade de {sourceAcc?.name ?? "—"}</p>
                    <p className="text-xs text-muted-foreground">Selecione as empresas que deseja mover. Use Ctrl+A para selecionar todas (visíveis) e Shift+clique para intervalo.</p>
                    <div className="relative">
                      <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground pointer-events-none" />
                      <Input
                        placeholder="Buscar por nome ou CNPJ..."
                        value={moveCompaniesSearch}
                        onChange={(e) => setMoveCompaniesSearch(e.target.value)}
                        className="pl-8 h-9 mb-2"
                        autoComplete="off"
                        aria-label="Buscar empresas por nome ou CNPJ"
                      />
                    </div>
                    {searchTerm ? (
                      <p className="text-xs text-muted-foreground mb-1">
                        {filteredCompanies.length} de {companiesFromSource.length} empresa(s)
                      </p>
                    ) : null}
                    <div
                      ref={moveCompaniesListRef}
                      className="border rounded-lg divide-y divide-border max-h-48 overflow-y-auto focus-within:ring-2 focus-within:ring-ring focus-within:ring-offset-2 outline-none"
                      tabIndex={0}
                      onKeyDown={(e) => {
                        const target = e.target as HTMLElement
                        if (target.closest("input") || target.closest("textarea")) return
                        if (e.ctrlKey && e.key.toLowerCase() === "a") {
                          e.preventDefault()
                          setMoveSelectedIds((prev) => {
                            const next = new Set(prev)
                            if (allFilteredSelected) {
                              visibleIds.forEach((id) => next.delete(id))
                            } else {
                              visibleIds.forEach((id) => next.add(id))
                            }
                            return next
                          })
                        }
                      }}
                    >
                      {filteredCompanies.length === 0 ? (
                        <div className="px-4 py-3 text-sm text-muted-foreground">
                          {companiesFromSource.length === 0 ? "Nenhuma empresa vinculada a este contador." : "Nenhuma empresa encontrada na busca."}
                        </div>
                      ) : (
                        filteredCompanies.map((emp, index) => (
                          <label
                            key={emp.id}
                            className="flex items-center gap-2 px-4 py-2 hover:bg-muted/30 cursor-pointer select-none"
                            onClick={(e) => {
                              e.preventDefault()
                              const target = e.target as HTMLElement
                              if (target.closest('input[type="checkbox"]')) return
                              const checkbox = (e.currentTarget as HTMLElement).querySelector<HTMLInputElement>('input[type="checkbox"]')
                              if (!checkbox) return
                              const shift = (e as React.MouseEvent).shiftKey
                              if (shift && moveLastClickedIndex !== null) {
                                const from = Math.min(moveLastClickedIndex, index)
                                const to = Math.max(moveLastClickedIndex, index)
                                setMoveSelectedIds((prev) => {
                                  const next = new Set(prev)
                                  const toSelect = !moveSelectedIds.has(emp.id)
                                  for (let i = from; i <= to; i++) {
                                    const id = filteredCompanies[i].id
                                    if (toSelect) next.add(id); else next.delete(id)
                                  }
                                  return next
                                })
                              } else {
                                setMoveLastClickedIndex(index)
                                setMoveSelectedIds((prev) => {
                                  const next = new Set(prev)
                                  if (next.has(emp.id)) next.delete(emp.id); else next.add(emp.id)
                                  return next
                                })
                              }
                            }}
                          >
                            <input
                              type="checkbox"
                              checked={moveSelectedIds.has(emp.id)}
                              onChange={(e) => {
                                e.stopPropagation()
                                setMoveLastClickedIndex(index)
                                setMoveSelectedIds((prev) => {
                                  const next = new Set(prev)
                                  if (e.target.checked) next.add(emp.id); else next.delete(emp.id)
                                  return next
                                })
                              }}
                              className="rounded border-input"
                            />
                            <span className="text-sm truncate">{emp.name}</span>
                            {emp.document && <span className="text-xs text-muted-foreground truncate ml-1">({emp.document.replace(/\D/g, "").slice(0, 8)}…)</span>}
                          </label>
                        ))
                      )}
                    </div>
                    <div className="flex gap-2 flex-wrap mt-2">
                      <Button variant="outline" size="sm" onClick={() => setMoveStep("source")}>Voltar</Button>
                      <Button size="sm" disabled={moveSelectedIds.size === 0} onClick={() => { setMoveStep("dest"); setMoveDestCpf(null); }}>
                        Mover {moveSelectedIds.size} selecionada(s)
                      </Button>
                    </div>
                  </>
                )
              })()}
              {moveStep === "dest" && moveSourceCpf && (
                <>
                  <p className="text-sm font-medium">Mover para qual contador?</p>
                  <Select value={moveDestCpf ?? ""} onValueChange={setMoveDestCpf}>
                    <SelectTrigger className="w-full"><SelectValue placeholder="Selecione o contador de destino" /></SelectTrigger>
                    <SelectContent>
                      {accountantsAll.filter((a) => onlyDigits(a.cpf) !== onlyDigits(moveSourceCpf)).map((a) => (
                        <SelectItem key={a.id} value={a.cpf}>{a.name} — {formatCpf(a.cpf)}</SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <DialogFooter>
                    <Button variant="outline" size="sm" onClick={() => setMoveStep("companies")}>Voltar</Button>
                    <Button
                      size="sm"
                      disabled={!moveDestCpf || moveSaving}
                      onClick={async () => {
                        const destAcc = accountantsAll.find((a) => onlyDigits(a.cpf) === onlyDigits(moveDestCpf!))
                        if (!destAcc || moveSelectedIds.size === 0) return
                        setMoveSaving(true)
                        try {
                          for (const companyId of moveSelectedIds) {
                            await updateCompany(companyId, { contador_cpf: destAcc.cpf, contador_nome: destAcc.name })
                          }
                          queryClient.invalidateQueries({ queryKey: ["companies-list"] })
                          setMoveStep(null)
                          setMoveSourceCpf(null)
                          setMoveSelectedIds(new Set())
                          setMoveDestCpf(null)
                          toast.success(`${moveSelectedIds.size} empresa(s) movida(s) para ${destAcc.name}.`)
                        } catch (e) {
                          toast.error(e instanceof Error ? e.message : "Erro ao mover")
                        } finally {
                          setMoveSaving(false)
                        }
                      }}
                    >
                      {moveSaving ? "Movendo…" : "Confirmar"}
                    </Button>
                  </DialogFooter>
                </>
              )}
            </>
          ) : accountantsAllLoading ? (
            <div className="flex items-center justify-center py-8">
              <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
          ) : (
            <>
              <ul className="border rounded-lg divide-y divide-border max-h-60 overflow-y-auto">
                {accountantsAll.length === 0 ? (
                  <li className="px-4 py-3 text-sm text-muted-foreground">Nenhum contador cadastrado.</li>
                ) : (
                  accountantsAll.map((acc) => (
                    <li key={acc.id} className="px-4 py-2.5 flex items-center justify-between gap-2">
                      {contadorEditingId === acc.id ? (
                        <div className="flex flex-col gap-2 flex-1 min-w-0">
                          <Input
                            value={contadorEditName}
                            onChange={(e) => setContadorEditName(e.target.value)}
                            placeholder="Nome"
                            className="h-8 text-sm"
                            disabled={contadorSaving}
                          />
                          <div className="flex items-center gap-2">
                            <label className="flex items-center gap-1.5 text-xs cursor-pointer">
                              <input
                                type="checkbox"
                                checked={contadorEditActive}
                                onChange={(e) => setContadorEditActive(e.target.checked)}
                                disabled={contadorSaving}
                                className="rounded border-input"
                              />
                              Ativo
                            </label>
                            <Button
                              size="sm"
                              className="h-7"
                              disabled={contadorSaving}
                              onClick={async () => {
                                setContadorError("")
                                setContadorSaving(true)
                                try {
                                  await updateAccountant(acc.id, { name: contadorEditName, active: contadorEditActive })
                                  queryClient.invalidateQueries({ queryKey: ["accountants"] })
                                  queryClient.invalidateQueries({ queryKey: ["accountants", "all"] })
                                  setContadorEditingId(null)
                                  toast.success("Contador atualizado.")
                                } catch (e) {
                                  setContadorError(e instanceof Error ? e.message : "Erro ao salvar")
                                } finally {
                                  setContadorSaving(false)
                                }
                              }}
                            >
                              Salvar
                            </Button>
                            <Button size="sm" variant="ghost" className="h-7" disabled={contadorSaving} onClick={() => setContadorEditingId(null)}>
                              Cancelar
                            </Button>
                          </div>
                        </div>
                      ) : (
                        <>
                          <div className="min-w-0">
                            <p className="text-sm font-medium truncate">{acc.name}</p>
                            <p className="text-xs text-muted-foreground">{formatCpf(acc.cpf)}</p>
                          </div>
                          <div className="flex items-center gap-2 flex-shrink-0 flex-wrap">
                            <span className={cn("text-xs font-medium px-2 py-0.5 rounded-full", acc.active ? "bg-success/15 text-success" : "bg-muted text-muted-foreground")}>
                              {acc.active ? "Ativo" : "Inativo"}
                            </span>
                            <Button size="sm" variant="outline" className="h-7 gap-1" onClick={() => { setContadorEditingId(acc.id); setContadorEditName(acc.name); setContadorEditActive(acc.active); setContadorError(""); }}>
                              <Pencil className="h-3 w-3" />
                              Editar
                            </Button>
                            {acc.active && (
                              <Button
                                size="sm"
                                variant="ghost"
                                className="h-7 text-destructive hover:text-destructive"
                                disabled={contadorSaving}
                                onClick={async () => {
                                  setContadorError("")
                                  setContadorSaving(true)
                                  try {
                                    await updateAccountant(acc.id, { active: false })
                                    queryClient.invalidateQueries({ queryKey: ["accountants"] })
                                    queryClient.invalidateQueries({ queryKey: ["accountants", "all"] })
                                    toast.success("Contador inativado.")
                                  } catch (e) {
                                    setContadorError(e instanceof Error ? e.message : "Erro ao inativar")
                                  } finally {
                                    setContadorSaving(false)
                                  }
                                }}
                              >
                                Inativar
                              </Button>
                            )}
                            <Button
                              size="sm"
                              variant="ghost"
                              className="h-7 text-destructive hover:text-destructive"
                              disabled={contadorSaving}
                              onClick={() => setContadorExcludeId(acc.id)}
                              title="Excluir contador"
                            >
                              <Trash2 className="h-3 w-3" />
                            </Button>
                          </div>
                        </>
                      )}
                    </li>
                  ))
                )}
              </ul>
              {contadorError && <p className="text-sm text-destructive">{contadorError}</p>}
              <div className="border-t pt-4">
                <Button type="button" variant="outline" size="sm" className="gap-2 w-full sm:w-auto mb-4" onClick={() => setMoveStep("source")}>
                  <ArrowRightLeft className="h-4 w-4" />
                  Mover responsabilidade de empresas
                </Button>
              </div>
              <div className="border-t pt-4 space-y-2">
                <p className="text-sm font-medium">Adicionar contador</p>
                <div className="flex flex-wrap gap-2 items-end">
                  <div className="space-y-1">
                    <Label className="text-xs">Nome</Label>
                    <Input
                      value={contadorNewName}
                      onChange={(e) => setContadorNewName(e.target.value)}
                      placeholder="Nome do contador"
                      className="h-8"
                      disabled={contadorSaving}
                    />
                  </div>
                  <div className="space-y-1">
                    <Label className="text-xs">CPF</Label>
                    <Input
                      value={contadorNewCpf}
                      onChange={(e) => setContadorNewCpf(e.target.value)}
                      placeholder="000.000.000-00"
                      className="h-8 w-32"
                      disabled={contadorSaving}
                    />
                  </div>
                  <Button
                    size="sm"
                    disabled={contadorSaving || !contadorNewName.trim() || onlyDigits(contadorNewCpf).length !== 11}
                    onClick={async () => {
                      setContadorError("")
                      setContadorSaving(true)
                      try {
                        await createAccountant({ name: contadorNewName.trim(), cpf: contadorNewCpf })
                        queryClient.invalidateQueries({ queryKey: ["accountants"] })
                        queryClient.invalidateQueries({ queryKey: ["accountants", "all"] })
                        setContadorNewName("")
                        setContadorNewCpf("")
                        toast.success("Contador adicionado.")
                      } catch (e) {
                        setContadorError(e instanceof Error ? e.message : "Erro ao adicionar")
                      } finally {
                        setContadorSaving(false)
                      }
                    }}
                  >
                    Adicionar
                  </Button>
                </div>
              </div>
            </>
          )}
          <DialogFooter>
            <Button type="button" onClick={() => setContadoresOpen(false)}>Fechar</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <AlertDialog open={!!contadorExcludeId} onOpenChange={(open) => !open && setContadorExcludeId(null)}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Excluir contador?</AlertDialogTitle>
            <AlertDialogDescription>
              {contadorExcludeId && accountantsAll.find((a) => a.id === contadorExcludeId)
                ? `O contador "${accountantsAll.find((a) => a.id === contadorExcludeId)!.name}" será excluído. As empresas vinculadas a ele não serão removidas, apenas ficarão sem contador responsável.`
                : "Este contador será excluído."}
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel disabled={contadorSaving}>Cancelar</AlertDialogCancel>
            <AlertDialogAction
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
              disabled={contadorSaving}
              onClick={async () => {
                if (!contadorExcludeId) return
                setContadorSaving(true)
                try {
                  await deleteAccountant(contadorExcludeId)
                  queryClient.invalidateQueries({ queryKey: ["accountants"] })
                  queryClient.invalidateQueries({ queryKey: ["accountants", "all"] })
                  setContadorExcludeId(null)
                  toast.success("Contador excluído.")
                } catch (e) {
                  toast.error(e instanceof Error ? e.message : "Erro ao excluir")
                } finally {
                  setContadorSaving(false)
                }
              }}
            >
              {contadorSaving ? "Excluindo…" : "Excluir"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
