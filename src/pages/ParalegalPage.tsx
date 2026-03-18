import { useMemo, useState, type Dispatch, type SetStateAction } from "react"
import { useQuery } from "@tanstack/react-query"
import { Link, useLocation, useNavigate } from "react-router-dom"
import {
  ArrowDownUp,
  AlertTriangle,
  Building2,
  Calendar,
  Clock3,
  Crown,
  Download,
  FileArchive,
  FileBadge2,
  FolderClock,
  Landmark,
  Search,
  ShieldAlert,
  ShieldCheck,
  Users,
} from "lucide-react"
import { Bar, BarChart, CartesianGrid, Cell, Pie, PieChart, XAxis, YAxis } from "recharts"
import { GlassCard } from "@/components/dashboard/GlassCard"
import { StatsCard } from "@/components/dashboard/StatsCard"
import { Button } from "@/components/ui/button"
import { ChartContainer, ChartTooltip, ChartTooltipContent } from "@/components/ui/chart"
import { DataPagination } from "@/components/common/DataPagination"
import { Input } from "@/components/ui/input"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { useCompanies } from "@/hooks/useCompanies"
import { useBrandingOptional } from "@/contexts/BrandingContext"
import { useSelectedCompanyIds } from "@/hooks/useSelectedCompanies"
import {
  QUALIFICACAO_DISPLAY,
  fetchSalarioMinimoBCB,
  qualificacaoFromHonorario,
  type QualificacaoPlano,
} from "@/services/bcbSalarioMinimoService"
import {
  CERTIFICATE_EXPIRY_WARNING_DAYS,
  getParalegalCertificates,
  getParalegalCertificateOverview,
  type CertificateStatus,
} from "@/services/paralegalService"
import {
  getMunicipalTaxOverview,
  getMunicipalTaxDebtsPage,
  type MunicipalTaxDebtView,
  type MunicipalTaxOverview,
  type MunicipalTaxStatusClass,
} from "@/services/municipalTaxesService"
import { downloadServerFileByPath, downloadServerFilesZip, hasServerApi } from "@/services/serverFileService"
import { cn } from "@/utils"
import { toast } from "sonner"
import { getVisibilityAwareRefetchInterval } from "@/lib/queryPolling"

type Topic = "overview" | "certificados" | "tarefas" | "clientes" | "taxas-impostos"
type CertificateFilter = "todos" | CertificateStatus
type ClientTier = QualificacaoPlano

type MockTask = {
  id: string
  titulo: string
  empresa: string
  prioridade: "alta" | "media" | "baixa"
  prazo: string
  responsavel: string
  status: "em_dia" | "vence_hoje" | "atrasada"
}

/** Filtros apenas da tabela completa de débitos (empresa vem do painel lateral). */
type MunicipalTaxTableFiltersState = {
  search: string
  year: string
  status: "todos" | MunicipalTaxStatusClass
  periodFrom: string
  periodTo: string
}

const CLIENT_TIER_ORDER: ClientTier[] = ["DIAMANTE", "OURO", "PRATA", "BRONZE"]

const TOPIC_LINKS: Array<{ label: string; path: string; topic: Topic }> = [
  { label: "Visao Geral", path: "/paralegal", topic: "overview" },
  { label: "Certificados", path: "/paralegal/certificados", topic: "certificados" },
  { label: "Tarefas", path: "/paralegal/tarefas", topic: "tarefas" },
  { label: "Taxas e Impostos", path: "/paralegal/taxas-impostos", topic: "taxas-impostos" },
]

const CERTIFICATE_STATUS_META: Record<CertificateStatus, { label: string; tone: string; chartColor: string }> = {
  ativo: { label: "Ativo", tone: "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300", chartColor: "#10B981" },
  vence_em_breve: { label: "Perto de vencer", tone: "bg-amber-500/15 text-amber-700 dark:text-amber-300", chartColor: "#F59E0B" },
  vencido: { label: "Vencido", tone: "bg-rose-500/15 text-rose-700 dark:text-rose-300", chartColor: "#F43F5E" },
  sem_certificado: { label: "Sem certificado", tone: "bg-slate-500/15 text-slate-700 dark:text-slate-300", chartColor: "#64748B" },
}

const MUNICIPAL_TAX_META: Record<MunicipalTaxStatusClass, { label: string; tone: string; color: string }> = {
  vencido: { label: "Vencido", tone: "bg-rose-500/15 text-rose-700 dark:text-rose-300", color: "#F43F5E" },
  a_vencer: { label: "A vencer (proximos 30 dias)", tone: "bg-amber-500/15 text-amber-700 dark:text-amber-300", color: "#F59E0B" },
  regular: { label: "Regular", tone: "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300", color: "#10B981" },
}

/** Retorna o texto da classificacao para exibicao. Para "a_vencer", usa os dias reais: "Vence hoje", "A vencer daqui 1 dia" ou "A vencer daqui N dias". */
function getMunicipalTaxClassificationLabel(item: MunicipalTaxDebtView): string {
  if (item.status_class === "a_vencer" && item.days_until_due != null) {
    const n = item.days_until_due
    if (n === 0) return "Vence hoje"
    if (n === 1) return "A vencer daqui 1 dia"
    return `A vencer daqui ${n} dias`
  }
  return MUNICIPAL_TAX_META[item.status_class].label
}

const MOCK_TASKS: MockTask[] = [
  { id: "1", titulo: "Alteracao contratual", empresa: "Grupo Fleury", prioridade: "alta", prazo: "2026-03-12", responsavel: "Julia", status: "vence_hoje" },
  { id: "2", titulo: "Baixa estadual", empresa: "Tech Solutions Ltda", prioridade: "alta", prazo: "2026-03-10", responsavel: "Victor", status: "atrasada" },
  { id: "3", titulo: "Atualizacao cadastral na junta", empresa: "Comercio ABC", prioridade: "media", prazo: "2026-03-15", responsavel: "Carla", status: "em_dia" },
  { id: "4", titulo: "Renovacao de alvara", empresa: "Industria XYZ", prioridade: "baixa", prazo: "2026-03-21", responsavel: "Leandro", status: "em_dia" },
  { id: "5", titulo: "Assinatura de procuracao", empresa: "Nova Era Servicos", prioridade: "media", prazo: "2026-03-13", responsavel: "Julia", status: "vence_hoje" },
]

const MOCK_CLIENT_TIERS: Array<{ empresa: string; honorario: number; carteira: string }> = [
  { empresa: "Grupo Fleury", honorario: 3600, carteira: "Holding e societario" },
  { empresa: "Industria XYZ", honorario: 1800, carteira: "Societario e licencas" },
  { empresa: "Tech Solutions Ltda", honorario: 920, carteira: "Legalizacao" },
  { empresa: "Comercio ABC", honorario: 420, carteira: "Rotina basica" },
  { empresa: "Nova Era Servicos", honorario: 760, carteira: "Procuracoes e certidoes" },
]

const MOCK_SALARIO_MINIMO = 1518

function getTopicFromPath(pathname: string): Topic {
  if (pathname === "/paralegal/certificados") return "certificados"
  if (pathname === "/paralegal/tarefas") return "tarefas"
  if (pathname === "/paralegal/clientes") return "overview"
  if (pathname === "/paralegal/taxas-impostos") return "taxas-impostos"
  return "overview"
}

function statusAccentColor(status: CertificateStatus) {
  if (status === "ativo") return "bg-emerald-500"
  if (status === "vence_em_breve") return "bg-amber-500"
  if (status === "vencido") return "bg-rose-500"
  return "bg-slate-400"
}

function formatDate(value: string | null) {
  if (!value) return "-"
  const [year, month, day] = value.split("-")
  return `${day}/${month}/${year}`
}

function formatDaysToExpiry(days: number | null) {
  if (days == null) return "Sem validade cadastrada"
  if (days < 0) return `Venceu ha ${Math.abs(days)} dia(s)`
  if (days === 0) return "Vence hoje"
  return `${days} dia(s) restantes`
}

function formatCnpj(value: string | null) {
  if (!value) return "-"
  const digits = value.replace(/\D/g, "")
  if (digits.length !== 14) return value
  return digits.replace(/^(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})$/, "$1.$2.$3/$4-$5")
}

function formatCurrencyBRL(value: number) {
  return new Intl.NumberFormat("pt-BR", { style: "currency", currency: "BRL" }).format(value)
}

type MunicipalTaxSortKey =
  | "company_name"
  | "tributo"
  | "ano"
  | "numero_documento"
  | "data_vencimento"
  | "valor"
  | "situacao"
  | "status_class"
  | null
type MunicipalTaxSortDirection = "asc" | "desc" | null
type MunicipalTaxSortState = { key: MunicipalTaxSortKey; direction: MunicipalTaxSortDirection }

function cycleMunicipalTaxSort(current: MunicipalTaxSortState, key: Exclude<MunicipalTaxSortKey, null>): MunicipalTaxSortState {
  if (current.key !== key) return { key, direction: "desc" }
  if (current.direction === "desc") return { key, direction: "asc" }
  return { key: null, direction: null }
}

function MunicipalTaxSortHeader({
  label,
  column,
  sort,
  onToggle,
}: {
  label: string
  column: Exclude<MunicipalTaxSortKey, null>
  sort: MunicipalTaxSortState
  onToggle: (key: Exclude<MunicipalTaxSortKey, null>) => void
}) {
  const active = sort.key === column ? (sort.direction === "desc" ? " ↓" : sort.direction === "asc" ? " ↑" : "") : ""
  return (
    <button
      type="button"
      onClick={() => onToggle(column)}
      className="inline-flex items-center gap-1 text-left font-medium text-muted-foreground hover:text-foreground"
    >
      <span>{label}{active}</span>
      <ArrowDownUp className={cn("h-3.5 w-3.5", sort.key === column ? "text-foreground" : "opacity-50")} />
    </button>
  )
}

function TasksPanel() {
  const taskSummary = MOCK_TASKS.reduce(
    (acc, task) => {
      acc.total += 1
      if (task.prioridade === "alta") acc.alta += 1
      if (task.status === "atrasada") acc.atrasadas += 1
      if (task.status === "vence_hoje") acc.venceHoje += 1
      return acc
    },
    { total: 0, alta: 0, atrasadas: 0, venceHoje: 0 }
  )

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatsCard title="Tarefas no front" value={taskSummary.total.toString()} icon={FolderClock} />
        <StatsCard title="Prioridade alta" value={taskSummary.alta.toString()} icon={AlertTriangle} />
        <StatsCard title="Vencem hoje" value={taskSummary.venceHoje.toString()} icon={Clock3} />
        <StatsCard title="Atrasadas" value={taskSummary.atrasadas.toString()} icon={ShieldAlert} />
      </div>
    </div>
  )
}

function ClientsPanel({ salarioMinimo, salarioMinimoLoading }: { salarioMinimo: number; salarioMinimoLoading: boolean }) {
  const clientsWithTier = MOCK_CLIENT_TIERS.map((item) => {
    const tier = qualificacaoFromHonorario(item.honorario, salarioMinimo)
    return { ...item, tier, percentualSalarioMinimo: (item.honorario / salarioMinimo) * 100 }
  })

  const tierCounts = clientsWithTier.reduce<Record<ClientTier, number>>(
    (acc, item) => {
      acc[item.tier] += 1
      return acc
    },
    { BRONZE: 0, PRATA: 0, OURO: 0, DIAMANTE: 0 }
  )

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatsCard title={QUALIFICACAO_DISPLAY.DIAMANTE.label} value={tierCounts.DIAMANTE.toString()} icon={Crown} />
        <StatsCard title={QUALIFICACAO_DISPLAY.OURO.label} value={tierCounts.OURO.toString()} icon={Crown} />
        <StatsCard title={QUALIFICACAO_DISPLAY.PRATA.label} value={tierCounts.PRATA.toString()} icon={Users} />
        <StatsCard title={QUALIFICACAO_DISPLAY.BRONZE.label} value={tierCounts.BRONZE.toString()} icon={Users} />
      </div>
      <GlassCard className="p-6">
        <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
          <div>
            <h3 className="text-sm font-semibold font-display">Qualificacao por honorario</h3>
            <p className="mt-1 text-xs text-muted-foreground">Front apenas, usando a mesma logica do formulario baseada no salario minimo.</p>
          </div>
          <div className="rounded-xl border border-border bg-background/70 px-4 py-3 text-xs">
            <p className="text-muted-foreground">Salario minimo de referencia</p>
            <p className="mt-1 text-sm font-semibold">{salarioMinimoLoading ? "Consultando..." : formatCurrencyBRL(salarioMinimo)}</p>
          </div>
        </div>
      </GlassCard>
    </div>
  )
}

function MunicipalTaxesPanel({
  filters,
  setFilters,
  companyIdsFilter,
  municipalOverview,
  municipalOverviewLoading,
  chartPrimaryColor = "#2563EB",
}: {
  filters: MunicipalTaxTableFiltersState
  setFilters: Dispatch<SetStateAction<MunicipalTaxTableFiltersState>>
  companyIdsFilter: string[] | null
  municipalOverview: MunicipalTaxOverview | undefined
  municipalOverviewLoading: boolean
  chartPrimaryColor?: string
}) {
  const summary = municipalOverview?.cards ?? {
    totalDebitos: 0,
    totalVencido: 0,
    totalAVencer: 0,
    quantidadeDebitos: 0,
    empresasComVencidos: 0,
    empresasProximasVencimento: 0,
    totalValor: 0,
  }
  const [downloadingZip, setDownloadingZip] = useState(false)
  const [tablePage, setTablePage] = useState(1)
  const [tablePageSize, setTablePageSize] = useState(10)

  const [tableSort, setTableSort] = useState<MunicipalTaxSortState>({ key: null, direction: null })
  const companyIdsKey = companyIdsFilter?.length ? companyIdsFilter.join(",") : "all"

  const yearOptions = useMemo(() => (municipalOverview?.years ?? []).slice().sort((a, b) => b - a), [municipalOverview])

  const {
    data: debtsPage,
    isLoading: debtsPageLoading,
  } = useQuery({
    queryKey: [
      "paralegal-municipal-debts-page",
      companyIdsKey,
      filters.search,
      filters.year,
      filters.status,
      filters.periodFrom,
      filters.periodTo,
      tablePage,
      tablePageSize,
      tableSort.key,
      tableSort.direction,
    ],
    queryFn: () =>
      getMunicipalTaxDebtsPage({
        companyIds: companyIdsFilter,
        year: filters.year,
        status: filters.status,
        dateFrom: filters.periodFrom || undefined,
        dateTo: filters.periodTo || undefined,
        search: filters.search,
        sortKey: tableSort.key,
        sortDirection: tableSort.direction,
        page: tablePage,
        pageSize: tablePageSize,
      }),
  })

  const pageItems = debtsPage?.items ?? []
  const totalFiltered = debtsPage?.total ?? 0
  const totalPages = Math.max(1, Math.ceil(totalFiltered / tablePageSize))
  const from = (tablePage - 1) * tablePageSize
  const to = Math.min(from + tablePageSize, totalFiltered)
  const listedGuidePaths = pageItems.map((item) => String(item.guia_pdf_path || "").trim()).filter(Boolean)

  const statusChartData = useMemo(() => {
    if (municipalOverview?.byStatus?.length) {
      return municipalOverview.byStatus.map((entry) => ({
        key: entry.key,
        name: entry.name,
        total: entry.total,
        fill: MUNICIPAL_TAX_META[entry.key].color,
      }))
    }

    return (["vencido", "a_vencer", "regular"] as MunicipalTaxStatusClass[]).map((status) => ({
      key: status,
      name: MUNICIPAL_TAX_META[status].label,
      total: 0,
      fill: MUNICIPAL_TAX_META[status].color,
    }))
  }, [municipalOverview])

  /** Os 30 primeiros documentos com data de vencimento de hoje para frente (hoje ou futuro). */
  const documentsByDueDate = municipalOverview?.dueSoon ?? []

  const companyChartData = municipalOverview?.byCompany ?? []
  const yearChartData = municipalOverview?.byYear ?? []

  return (
    <div className="space-y-4">
      <GlassCard className="p-4">
        <div className="flex flex-wrap items-center gap-3">
          <div className="flex items-center gap-2 text-muted-foreground">
            <Calendar className="h-4 w-4 shrink-0" />
            <span className="text-sm font-medium">Período</span>
          </div>
          <label className="flex items-center gap-2">
            <span className="text-xs text-muted-foreground">De</span>
            <Input
              type="date"
              value={filters.periodFrom}
              onChange={(e) => {
                setFilters((c) => ({ ...c, periodFrom: e.target.value }))
                setTablePage(1)
              }}
              className="h-8 w-[10rem]"
            />
          </label>
          <label className="flex items-center gap-2">
            <span className="text-xs text-muted-foreground">Até</span>
            <Input
              type="date"
              value={filters.periodTo}
              onChange={(e) => {
                setFilters((c) => ({ ...c, periodTo: e.target.value }))
                setTablePage(1)
              }}
              className="h-8 w-[10rem]"
            />
          </label>
        </div>
        <p className="text-xs text-muted-foreground mt-2">Todos os dados da página (estatísticas e tabela) seguem este período.</p>
      </GlassCard>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatsCard title="Total de debitos" value={formatCurrencyBRL(summary.totalValor)} icon={Landmark} />
        <StatsCard title="Total vencido" value={formatCurrencyBRL(summary.totalVencido)} icon={ShieldAlert} />
        <StatsCard title="Total a vencer" value={formatCurrencyBRL(summary.totalAVencer)} icon={Clock3} />
        <StatsCard title="Quantidade de debitos" value={summary.quantidadeDebitos.toString()} icon={FileBadge2} />
      </div>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[1.1fr_0.9fr]">
        <GlassCard className="p-6">
          <div className="mb-4">
            <h3 className="text-sm font-semibold font-display">Classificacao automatica</h3>
            <p className="mt-1 text-xs text-muted-foreground">Percentual de debitos vencidos, a vencer e regulares.</p>
          </div>
          <ChartContainer className="h-[280px] w-full" config={{ total: { label: "Debitos", color: chartPrimaryColor } }}>
            <PieChart>
              <Pie data={statusChartData} dataKey="total" nameKey="name" innerRadius={64} outerRadius={92} paddingAngle={3}>
                {statusChartData.map((entry) => <Cell key={entry.key} fill={entry.fill} />)}
              </Pie>
              <ChartTooltip content={<ChartTooltipContent />} />
            </PieChart>
          </ChartContainer>
          <div className="grid grid-cols-1 gap-2 md:grid-cols-3">
            {statusChartData.map((entry) => (
              <div key={entry.key} className="rounded-xl border border-border px-3 py-2 text-xs">
                <div className="flex items-center gap-2">
                  <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: entry.fill }} />
                  <span className="text-muted-foreground">{entry.name}</span>
                </div>
                <p className="mt-2 text-sm font-semibold">{entry.total}</p>
              </div>
            ))}
          </div>
        </GlassCard>

        <GlassCard className="p-6">
          <div className="mb-4">
            <h3 className="text-sm font-semibold font-display">Documentos próximos de vencer</h3>
            <p className="mt-1 text-xs text-muted-foreground">Os 30 primeiros com vencimento de hoje para frente (do mais próximo).</p>
          </div>
          <div className="space-y-2 max-h-[320px] overflow-y-auto -webkit-overflow-scrolling-touch">
            {municipalOverviewLoading ? (
              <p className="text-xs text-muted-foreground">Carregando...</p>
            ) : documentsByDueDate.length === 0 ? (
              <p className="text-xs text-muted-foreground">Nenhum débito com vencimento de hoje em diante nos filtros atuais.</p>
            ) : (
              documentsByDueDate.map((item) => (
                <div key={item.id} className="rounded-xl border border-border bg-background/60 px-3 py-2.5">
                  <div className="flex items-start justify-between gap-2">
                    <div className="min-w-0 flex-1">
                      <p className="truncate text-sm font-medium">{item.company_name}</p>
                      <p className="mt-0.5 truncate text-xs text-muted-foreground" title={item.tributo ?? undefined}>{item.tributo || "-"}</p>
                    </div>
                    <span className={cn("shrink-0 rounded-full px-2 py-0.5 text-[11px] font-medium", MUNICIPAL_TAX_META[item.status_class].tone)}>
                      {getMunicipalTaxClassificationLabel(item)}
                    </span>
                  </div>
                  <div className="mt-1.5 flex items-center gap-2 text-xs text-muted-foreground">
                    <span>Venc.: {formatDate(item.data_vencimento)}</span>
                    <span className="tabular-nums">{formatCurrencyBRL(Number(item.valor ?? 0))}</span>
                  </div>
                  {hasServerApi() && (
                    <div className="mt-2.5 flex justify-end">
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        className="h-7 gap-1 text-xs"
                        disabled={!item.guia_pdf_path}
                        onClick={() =>
                          item.guia_pdf_path &&
                          downloadServerFileByPath(
                            item.guia_pdf_path,
                            item.guia_pdf_path.split(/[\\/]/).pop() || undefined,
                          )
                        }
                      >
                        <Download className="h-3.5 w-3.5" /> PDF
                      </Button>
                    </div>
                  )}
                </div>
              ))
            )}
          </div>
        </GlassCard>
      </div>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
        <GlassCard className="p-6">
          <div className="mb-4">
            <h3 className="text-sm font-semibold font-display">Distribuicao por empresa</h3>
            <p className="mt-1 text-xs text-muted-foreground">Top empresas com maior valor em debitos municipais.</p>
          </div>
          <ChartContainer className="h-[280px] w-full" config={{ total: { label: "Valor", color: chartPrimaryColor } }}>
            <BarChart data={companyChartData} layout="vertical" margin={{ left: 24, right: 16, top: 8, bottom: 8 }}>
              <CartesianGrid horizontal={false} />
              <XAxis type="number" tickFormatter={(value) => formatCurrencyBRL(Number(value))} />
              <YAxis type="category" dataKey="name" width={140} tickLine={false} axisLine={false} />
              <ChartTooltip content={<ChartTooltipContent formatter={(value) => formatCurrencyBRL(Number(value))} />} />
              <Bar dataKey="total" fill={chartPrimaryColor} radius={[0, 10, 10, 0]} />
            </BarChart>
          </ChartContainer>
        </GlassCard>

        <GlassCard className="p-6">
          <div className="mb-4">
            <h3 className="text-sm font-semibold font-display">Distribuicao por ano</h3>
            <p className="mt-1 text-xs text-muted-foreground">Soma de valores agrupada pelo ano do debito.</p>
          </div>
          <ChartContainer className="h-[280px] w-full" config={{ total: { label: "Valor", color: "#0F766E" } }}>
            <BarChart data={yearChartData} margin={{ left: 60, right: 16, top: 8, bottom: 8 }}>
              <CartesianGrid vertical={false} />
              <XAxis dataKey="name" tickLine={false} axisLine={false} />
              <YAxis width={58} tickFormatter={(value) => formatCurrencyBRL(Number(value))} />
              <ChartTooltip content={<ChartTooltipContent formatter={(value) => formatCurrencyBRL(Number(value))} />} />
              <Bar dataKey="total" fill="#0F766E" radius={[10, 10, 0, 0]} />
            </BarChart>
          </ChartContainer>
        </GlassCard>
      </div>

      <GlassCard className="overflow-hidden">
        <div className="border-b border-border p-4">
          <h3 className="text-sm font-semibold font-display">Tabela completa de debitos</h3>
          <p className="mt-1 text-xs text-muted-foreground">Consulta consolidada de taxas e impostos municipais da Prefeitura de Goiania. Empresas conforme seleção do painel lateral.</p>
          <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-[minmax(0,1.35fr)_minmax(9rem,10rem)_minmax(9rem,10rem)_auto]">
            <div className="relative">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <Input value={filters.search} onChange={(e) => { setFilters((c) => ({ ...c, search: e.target.value })); setTablePage(1); }} placeholder="Pesquisar por empresa ou CNPJ" className="pl-9" />
            </div>
            <Select value={filters.year} onValueChange={(v) => { setFilters((c) => ({ ...c, year: v })); setTablePage(1); }}>
              <SelectTrigger><SelectValue placeholder="Ano" /></SelectTrigger>
              <SelectContent>
                <SelectItem value="todos">Todos os anos</SelectItem>
                {yearOptions.map((year) => <SelectItem key={year} value={String(year)}>{year}</SelectItem>)}
              </SelectContent>
            </Select>
            <Select value={filters.status} onValueChange={(v) => { setFilters((c) => ({ ...c, status: v as MunicipalTaxTableFiltersState["status"] })); setTablePage(1); }}>
              <SelectTrigger><SelectValue placeholder="Status" /></SelectTrigger>
              <SelectContent>
                <SelectItem value="todos">Todos</SelectItem>
                <SelectItem value="vencido">Vencido</SelectItem>
                <SelectItem value="a_vencer">A vencer</SelectItem>
                <SelectItem value="regular">Regular</SelectItem>
              </SelectContent>
            </Select>
            {hasServerApi() && (
              <Button
                type="button"
                className="gap-2 xl:justify-self-end"
                disabled={downloadingZip || listedGuidePaths.length === 0}
                onClick={async () => {
                  if (listedGuidePaths.length === 0) {
                    toast.error("Nenhum débito com guia disponível na lista.")
                    return
                  }
                  setDownloadingZip(true)
                  try {
                    await downloadServerFilesZip(listedGuidePaths, "guias-taxas-impostos")
                    toast.success(`Download iniciado: ${listedGuidePaths.length} guia(s) (página atual).`)
                  } catch (e) {
                    toast.error(e instanceof Error ? e.message : "Erro ao baixar ZIP.")
                  } finally {
                    setDownloadingZip(false)
                  }
                }}
              >
                <FileArchive className="h-4 w-4" />
                {downloadingZip ? "Gerando ZIP..." : "Baixar ZIP dos documentos listados"}
              </Button>
            )}
          </div>
        </div>
        <div className="overflow-x-auto -webkit-overflow-scrolling-touch rounded-b-lg border-x border-b border-border">
          {debtsPageLoading ? (
            <div className="px-4 py-10 text-center text-sm text-muted-foreground">Carregando debitos municipais...</div>
          ) : pageItems.length === 0 ? (
            <div className="px-4 py-10 text-center text-sm text-muted-foreground">Nenhum debito encontrado para os filtros informados.</div>
          ) : (
            <table className="min-w-[1200px] w-full table-fixed text-[11px]">
              <thead className="bg-muted/50 text-left text-[11px] uppercase tracking-wide text-muted-foreground">
                <tr className="border-b border-border">
                  <th className="w-[160px] min-w-[160px] px-3 py-3 pr-6"><MunicipalTaxSortHeader label="Empresa" column="company_name" sort={tableSort} onToggle={(k) => { setTableSort((s) => cycleMunicipalTaxSort(s, k)); setTablePage(1); }} /></th>
                  <th className="w-[300px] min-w-[300px] px-3 py-3 pr-6"><MunicipalTaxSortHeader label="Tributo" column="tributo" sort={tableSort} onToggle={(k) => { setTableSort((s) => cycleMunicipalTaxSort(s, k)); setTablePage(1); }} /></th>
                  <th className="w-[52px] px-3 py-3"><MunicipalTaxSortHeader label="Ano" column="ano" sort={tableSort} onToggle={(k) => { setTableSort((s) => cycleMunicipalTaxSort(s, k)); setTablePage(1); }} /></th>
                  <th className="w-[90px] px-3 py-3"><MunicipalTaxSortHeader label="Documento" column="numero_documento" sort={tableSort} onToggle={(k) => { setTableSort((s) => cycleMunicipalTaxSort(s, k)); setTablePage(1); }} /></th>
                  <th className="w-[92px] px-3 py-3"><MunicipalTaxSortHeader label="Vencimento" column="data_vencimento" sort={tableSort} onToggle={(k) => { setTableSort((s) => cycleMunicipalTaxSort(s, k)); setTablePage(1); }} /></th>
                  <th className="w-[92px] px-3 py-3"><MunicipalTaxSortHeader label="Valor" column="valor" sort={tableSort} onToggle={(k) => { setTableSort((s) => cycleMunicipalTaxSort(s, k)); setTablePage(1); }} /></th>
                  <th className="w-[90px] px-3 py-3"><MunicipalTaxSortHeader label="Situacao" column="situacao" sort={tableSort} onToggle={(k) => { setTableSort((s) => cycleMunicipalTaxSort(s, k)); setTablePage(1); }} /></th>
                  <th className="w-[120px] px-3 py-3 whitespace-nowrap"><MunicipalTaxSortHeader label="Classificacao" column="status_class" sort={tableSort} onToggle={(k) => { setTableSort((s) => cycleMunicipalTaxSort(s, k)); setTablePage(1); }} /></th>
                  {hasServerApi() && <th className="w-[80px] px-3 py-3 text-left">Guia PDF</th>}
                </tr>
              </thead>
              <tbody>
                {pageItems.map((item) => (
                  <tr key={item.id} className="border-b border-border/70 hover:bg-muted/30 transition-colors">
                    <td className="w-[160px] min-w-[160px] px-3 py-3 pr-6 align-top min-w-0">
                      <div className="font-medium truncate" title={item.company_name ?? undefined}>{item.company_name}</div>
                      <div className="text-[11px] text-muted-foreground truncate mt-0.5" title={item.company_document ?? undefined}>{formatCnpj(item.company_document)}</div>
                    </td>
                    <td className="w-[300px] min-w-[300px] px-3 py-3 pr-6 align-top min-w-0">
                      <div className="break-words leading-snug">{item.tributo || "-"}</div>
                    </td>
                    <td className="w-[52px] px-3 py-3 align-top whitespace-nowrap tabular-nums">{item.ano || "-"}</td>
                    <td className="w-[90px] px-3 py-3 align-top min-w-0"><span className="block truncate" title={item.numero_documento ?? undefined}>{item.numero_documento || "-"}</span></td>
                    <td className="w-[92px] px-3 py-3 align-top whitespace-nowrap tabular-nums">{formatDate(item.data_vencimento)}</td>
                    <td className="w-[92px] px-3 py-3 align-top whitespace-nowrap tabular-nums">{formatCurrencyBRL(Number(item.valor || 0))}</td>
                    <td className="w-[90px] px-3 py-3 align-top min-w-0"><span className="block truncate" title={item.situacao ?? undefined}>{item.situacao || "-"}</span></td>
                    <td className="w-[120px] px-3 py-3 align-top whitespace-nowrap">
                      <span className={cn("inline-block rounded-full px-2 py-0.5 text-[11px] font-medium whitespace-nowrap", MUNICIPAL_TAX_META[item.status_class].tone)} title={getMunicipalTaxClassificationLabel(item)}>{getMunicipalTaxClassificationLabel(item)}</span>
                    </td>
                    {hasServerApi() && (
                      <td className="w-[80px] px-3 py-3 align-top">
                        <Button
                          type="button"
                          variant="outline"
                          size="sm"
                          className="h-7 gap-1 text-xs"
                          disabled={!item.guia_pdf_path}
                          onClick={() =>
                            item.guia_pdf_path &&
                            downloadServerFileByPath(
                              item.guia_pdf_path,
                              item.guia_pdf_path.split(/[\\/]/).pop() || undefined,
                            )
                          }
                        >
                          <Download className="h-3.5 w-3.5" /> PDF
                        </Button>
                      </td>
                    )}
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
        {totalFiltered > 0 && (
          <DataPagination
            currentPage={tablePage}
            totalPages={totalPages}
            totalItems={totalFiltered}
            from={from + 1}
            to={to}
            pageSize={tablePageSize}
            onPageChange={setTablePage}
            onPageSizeChange={(next) => { setTablePageSize(next); setTablePage(1); }}
          />
        )}
      </GlassCard>
    </div>
  )
}

export default function ParalegalPage() {
  const location = useLocation()
  const navigate = useNavigate()
  const topic = getTopicFromPath(location.pathname)

  const [search, setSearch] = useState("")
  const [filter, setFilter] = useState<CertificateFilter>("todos")
  const [municipalFilters, setMunicipalFilters] = useState<MunicipalTaxTableFiltersState>({
    search: "",
    year: "todos",
    status: "todos",
    periodFrom: "",
    periodTo: "",
  })

  const branding = useBrandingOptional()?.branding
  const chartPrimaryColor = (branding?.use_custom_palette && branding?.primary_color) ? branding.primary_color : "#2563EB"
  const { selectedCompanyIds } = useSelectedCompanyIds()
  const { data: companies = [] } = useCompanies()
  const companyIdsFilter = selectedCompanyIds.length > 0 ? selectedCompanyIds : null
  const { data: certificateItems = [], isLoading } = useQuery({
    queryKey: ["paralegal-certificates", companyIdsFilter],
    queryFn: () => getParalegalCertificates(companyIdsFilter),
  })
  const { data: certificateOverview } = useQuery({
    queryKey: ["paralegal-certificate-overview", companyIdsFilter],
    queryFn: () => getParalegalCertificateOverview(companyIdsFilter),
  })
  const { data: salarioMinimoData, isLoading: salarioMinimoLoading } = useQuery({
    queryKey: ["paralegal-salario-minimo"],
    queryFn: fetchSalarioMinimoBCB,
    staleTime: 0,
    gcTime: 5 * 60 * 1000,
    refetchOnMount: "always",
  })
  const { data: municipalDebts = [] } = useQuery({
    queryKey: ["paralegal-municipal-taxes", selectedCompanyIds, municipalFilters],
    queryFn: () =>
      getMunicipalTaxDebts({
        companyIds: companyIdsFilter,
        year: municipalFilters.year,
        status: municipalFilters.status,
        dateFrom: municipalFilters.periodFrom || undefined,
        dateTo: municipalFilters.periodTo || undefined,
        search: municipalFilters.search,
      }),
    enabled: topic !== "taxas-impostos",
  })
  const { data: municipalOverview, isLoading: municipalOverviewLoading } = useQuery({
    queryKey: ["paralegal-municipal-overview", companyIdsFilter, municipalFilters],
    queryFn: () =>
      getMunicipalTaxOverview({
        companyIds: companyIdsFilter,
        year: municipalFilters.year,
        status: municipalFilters.status,
        dateFrom: municipalFilters.periodFrom || undefined,
        dateTo: municipalFilters.periodTo || undefined,
        search: municipalFilters.search,
      }),
  })
  const salarioMinimo = salarioMinimoData ?? MOCK_SALARIO_MINIMO
  const certificateSummary = certificateOverview?.cards ?? {
    total: certificateItems.length,
    ativos: certificateItems.filter((item) => item.certificate_status === "ativo").length,
    venceEmBreve: certificateItems.filter((item) => item.certificate_status === "vence_em_breve").length,
    vencidos: certificateItems.filter((item) => item.certificate_status === "vencido").length,
    semCertificado: certificateItems.filter((item) => item.certificate_status === "sem_certificado").length,
  }
  const municipalSummary = municipalOverview?.cards ?? {
    totalDebitos: 0,
    totalVencido: 0,
    totalAVencer: 0,
    quantidadeDebitos: 0,
    empresasComVencidos: 0,
    empresasProximasVencimento: 0,
    totalValor: 0,
  }

  const filteredCertificates = useMemo(() => {
    const query = search.trim().toLowerCase()
    return certificateItems.filter((item) => {
      const matchesFilter = filter === "todos" || item.certificate_status === filter
      const normalizedDocument = (item.document ?? "").replace(/\D/g, "")
      const normalizedQuery = query.replace(/\D/g, "")
      const matchesSearch = query.length === 0 || item.name.toLowerCase().includes(query) || normalizedDocument.includes(normalizedQuery)
      return matchesFilter && matchesSearch
    })
  }, [certificateItems, filter, search])

  const certificateBarData = useMemo(
    () => (certificateOverview?.byStatus?.length ? certificateOverview.byStatus.map((item) => ({
      name: item.name,
      key: item.key,
      total: item.total,
      fill: CERTIFICATE_STATUS_META[item.key].chartColor,
    })) : [
      { name: "Ativos", key: "ativo", total: certificateSummary.ativos, fill: CERTIFICATE_STATUS_META.ativo.chartColor },
      { name: "Perto de vencer", key: "vence_em_breve", total: certificateSummary.venceEmBreve, fill: CERTIFICATE_STATUS_META.vence_em_breve.chartColor },
      { name: "Vencidos", key: "vencido", total: certificateSummary.vencidos, fill: CERTIFICATE_STATUS_META.vencido.chartColor },
      { name: "Sem certificado", key: "sem_certificado", total: certificateSummary.semCertificado, fill: CERTIFICATE_STATUS_META.sem_certificado.chartColor },
    ]),
    [certificateOverview, certificateSummary]
  )

  const certificatePieData = useMemo(() => certificateBarData.filter((item) => item.total > 0), [certificateBarData])

  const overviewTaxChartData = useMemo(
    () =>
      (municipalOverview?.byStatus?.length
        ? municipalOverview.byStatus.map((item) => ({
            key: item.key,
            name: item.name,
            total: item.total,
            fill: MUNICIPAL_TAX_META[item.key].color,
          }))
        : (["vencido", "a_vencer", "regular"] as MunicipalTaxStatusClass[]).map((status) => ({
        key: status,
        name: MUNICIPAL_TAX_META[status].label,
        total: municipalDebts.filter((item) => item.status_class === status).length,
        fill: MUNICIPAL_TAX_META[status].color,
      }))),
    [municipalDebts, municipalOverview]
  )
  const overviewTaxPieData = useMemo(() => overviewTaxChartData.filter((entry) => entry.total > 0), [overviewTaxChartData])

  const overviewCards = (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
      <StatsCard title="Certificados ativos" value={certificateSummary.ativos.toString()} icon={ShieldCheck} />
      <StatsCard title={`Vencem em ate ${CERTIFICATE_EXPIRY_WARNING_DAYS} dias`} value={certificateSummary.venceEmBreve.toString()} icon={AlertTriangle} />
      <StatsCard title="Certificados vencidos" value={certificateSummary.vencidos.toString()} icon={ShieldAlert} />
      <StatsCard title="Sem certificado" value={certificateSummary.semCertificado.toString()} icon={FileBadge2} />
    </div>
  )

  const certificatesPanel = (
    <div className="space-y-4">
      {overviewCards}
      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[1.3fr_0.9fr]">
        <GlassCard className="p-6">
          <div className="mb-4">
            <h3 className="text-sm font-semibold font-display">Status dos certificados</h3>
            <p className="mt-1 text-xs text-muted-foreground">Dados reais vindos das empresas cadastradas.</p>
          </div>
          <ChartContainer className="h-[280px] w-full" config={{ total: { label: "Empresas", color: chartPrimaryColor } }}>
            <BarChart data={certificateBarData}>
              <CartesianGrid vertical={false} />
              <XAxis dataKey="name" tickLine={false} axisLine={false} />
              <YAxis allowDecimals={false} />
              <ChartTooltip content={<ChartTooltipContent />} />
              <Bar dataKey="total" radius={[10, 10, 0, 0]}>
                {certificateBarData.map((entry) => <Cell key={entry.key} fill={entry.fill} />)}
              </Bar>
            </BarChart>
          </ChartContainer>
        </GlassCard>
        <GlassCard className="p-6">
          <h3 className="mb-4 text-sm font-semibold font-display">Distribuicao</h3>
          <ChartContainer className="h-[280px] w-full" config={{ ativo: { label: "Ativos", color: CERTIFICATE_STATUS_META.ativo.chartColor }, vence_em_breve: { label: "Perto de vencer", color: CERTIFICATE_STATUS_META.vence_em_breve.chartColor }, vencido: { label: "Vencidos", color: CERTIFICATE_STATUS_META.vencido.chartColor }, sem_certificado: { label: "Sem certificado", color: CERTIFICATE_STATUS_META.sem_certificado.chartColor } }}>
            <PieChart>
              <Pie data={certificatePieData} dataKey="total" nameKey="name" innerRadius={58} outerRadius={88} paddingAngle={3}>
                {certificatePieData.map((entry) => <Cell key={entry.key} fill={entry.fill} />)}
              </Pie>
              <ChartTooltip content={<ChartTooltipContent nameKey="key" />} />
            </PieChart>
          </ChartContainer>
        </GlassCard>
      </div>
      <GlassCard className="overflow-hidden">
        <div className="space-y-3 border-b border-border p-4">
          <div>
            <h3 className="text-sm font-semibold font-display">Controle de certificados</h3>
            <p className="mt-1 text-xs text-muted-foreground">Filtre por status e abra a empresa no cadastro quando precisar renovar.</p>
          </div>
          <div className="flex flex-col gap-3 xl:flex-row xl:items-center">
            <Input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Buscar por empresa ou CNPJ..." className="xl:max-w-sm" />
            <div className="flex flex-wrap gap-2">
              {([
                { value: "todos", label: "Todos" },
                { value: "ativo", label: "Ativos" },
                { value: "vence_em_breve", label: "Perto de vencer" },
                { value: "vencido", label: "Vencidos" },
                { value: "sem_certificado", label: "Sem certificado" },
              ] as Array<{ value: CertificateFilter; label: string }>).map((option) => (
                <button key={option.value} type="button" onClick={() => setFilter(option.value)} className={cn("rounded-full border px-3 py-1.5 text-xs font-medium transition-colors focus:outline-none focus:ring-2 focus:ring-ring", filter === option.value ? "border-primary bg-primary text-primary-foreground shadow-sm" : "border-border bg-background/60 text-muted-foreground hover:bg-background hover:text-foreground")}>
                  {option.label}
                </button>
              ))}
            </div>
          </div>
        </div>
        <div className="p-3">
          {isLoading ? (
            <div className="px-4 py-10 text-center text-sm text-muted-foreground">Carregando certificados...</div>
          ) : filteredCertificates.length === 0 ? (
            <div className="px-4 py-10 text-center text-sm text-muted-foreground">Nenhum certificado encontrado para este filtro.</div>
          ) : (
            <div className="space-y-2.5">
              {filteredCertificates.map((item) => (
                <div key={item.id} className={cn("group relative overflow-hidden rounded-2xl border border-border bg-background/40 backdrop-blur-sm", "transition-colors hover:bg-background/60 hover:shadow-sm")}>
                  <div className={cn("absolute left-0 top-0 h-full w-1.5", statusAccentColor(item.certificate_status))} />
                  <div className="pl-4 pr-3 py-3">
                    <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                      <div className="min-w-0">
                        <div className="flex flex-wrap items-center gap-2">
                          <p className="truncate text-[13px] font-semibold">{item.name}</p>
                          <span className={cn("rounded-full px-2.5 py-1 text-[11px] font-medium", CERTIFICATE_STATUS_META[item.certificate_status].tone)}>{CERTIFICATE_STATUS_META[item.certificate_status].label}</span>
                          <span className={cn("rounded-full px-2.5 py-1 text-[11px] font-medium", item.active ? "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300" : "bg-slate-500/15 text-slate-700 dark:text-slate-300")}>{item.active ? "Empresa ativa" : "Empresa inativa"}</span>
                        </div>
                        <div className="mt-2 grid grid-cols-1 gap-2 text-[11px] sm:grid-cols-3">
                          <div className="rounded-xl border border-border bg-background/40 px-2.5 py-1.5"><p className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">CNPJ</p><p className="mt-1 truncate font-medium text-foreground">{item.document ? formatCnpj(item.document) : "Nao informado"}</p></div>
                          <div className="rounded-xl border border-border bg-background/40 px-2.5 py-1.5"><p className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">Validade</p><p className="mt-1 font-medium text-foreground">{formatDate(item.cert_valid_until)}</p></div>
                          <div className="rounded-xl border border-border bg-background/40 px-2.5 py-1.5"><p className="text-[10px] font-medium uppercase tracking-wide text-muted-foreground">Prazo</p><p className="mt-1 font-medium text-foreground">{formatDaysToExpiry(item.days_to_expiry)}</p></div>
                        </div>
                      </div>
                      <div className="flex flex-wrap items-center gap-2 lg:justify-end">
                        <Button type="button" size="sm" className="h-8 px-3 text-xs shadow-sm" onClick={() => navigate(`/empresas?editCompany=${item.id}&focus=certificate&mode=renew`)}>Renovar certificado</Button>
                        <Button type="button" variant="outline" size="sm" className="h-8 px-3 text-xs bg-background/40" onClick={() => navigate(`/empresas?editCompany=${item.id}`)}>Abrir empresa</Button>
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </GlassCard>
    </div>
  )

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold font-display tracking-tight">Paralegal</h1>
        <p className="mt-1 text-sm text-muted-foreground">Certificados, tarefas e taxas municipais.</p>
      </div>
      <div className="flex flex-wrap gap-2">
        {TOPIC_LINKS.map((item) => (
          <Link key={item.path} to={item.path} className={cn("rounded-lg border px-4 py-2 text-xs font-medium transition-colors", topic === item.topic ? "border-primary bg-primary text-primary-foreground" : "border-border bg-card hover:bg-muted")}>
            {item.label}
          </Link>
        ))}
      </div>
      {(topic === "overview" || topic === "clientes") && (
        <div className="space-y-6">
          <section>
            <h2 className="mb-3 text-sm font-semibold font-display text-muted-foreground">Certificados</h2>
            {overviewCards}
            <div className="mt-4">
              <GlassCard className="p-6">
                <h3 className="mb-4 text-sm font-semibold font-display">Status dos certificados</h3>
                <ChartContainer className="h-[260px] w-full" config={{ total: { label: "Empresas", color: chartPrimaryColor } }}>
                  <BarChart data={certificateBarData}>
                    <CartesianGrid vertical={false} />
                    <XAxis dataKey="name" tickLine={false} axisLine={false} />
                    <YAxis allowDecimals={false} />
                    <ChartTooltip content={<ChartTooltipContent />} />
                    <Bar dataKey="total" radius={[10, 10, 0, 0]}>
                      {certificateBarData.map((entry) => <Cell key={entry.key} fill={entry.fill} />)}
                    </Bar>
                  </BarChart>
                </ChartContainer>
              </GlassCard>
            </div>
          </section>

          <section>
            <h2 className="mb-3 text-sm font-semibold font-display text-muted-foreground">Taxas e impostos (Goiania)</h2>
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
              <StatsCard title="Empresas com debitos vencidos" value={municipalSummary.empresasComVencidos.toString()} icon={Building2} />
              <StatsCard title="Debitos a vencer (30 dias)" value={String(municipalOverview?.byStatus.find((item) => item.key === "a_vencer")?.total ?? municipalDebts.filter((item) => item.status_class === "a_vencer").length)} icon={Clock3} />
              <StatsCard title="Total de debitos" value={municipalSummary.quantidadeDebitos.toString()} icon={Landmark} />
              <StatsCard title="Valor total em aberto" value={formatCurrencyBRL(municipalSummary.totalValor)} icon={AlertTriangle} />
            </div>
            <div className="mt-4">
              <GlassCard className="p-6">
                <h3 className="mb-4 text-sm font-semibold font-display">Classificacao dos debitos municipais</h3>
                <ChartContainer className="h-[260px] w-full" config={{ total: { label: "Debitos", color: chartPrimaryColor } }}>
                  {overviewTaxPieData.length === 0 ? (
                    <div className="flex h-[260px] items-center justify-center text-sm text-muted-foreground">Nenhum debito nos filtros atuais.</div>
                  ) : (
                    <PieChart>
                      <Pie data={overviewTaxPieData} dataKey="total" nameKey="name" innerRadius={58} outerRadius={88} paddingAngle={3}>
                        {overviewTaxPieData.map((entry) => <Cell key={entry.key} fill={entry.fill} />)}
                      </Pie>
                      <ChartTooltip content={<ChartTooltipContent />} />
                    </PieChart>
                  )}
                </ChartContainer>
              </GlassCard>
            </div>
          </section>

          <section>
            <h2 className="mb-3 text-sm font-semibold font-display text-muted-foreground">Tarefas</h2>
            <p className="mb-4 text-xs text-muted-foreground">Controle em construcao; dados ilustrativos.</p>
            <TasksPanel />
          </section>
        </div>
      )}
      {topic === "certificados" && certificatesPanel}
      {topic === "tarefas" && <TasksPanel />}
      {topic === "taxas-impostos" && (
        <MunicipalTaxesPanel
          filters={municipalFilters}
          setFilters={setMunicipalFilters}
          companyIdsFilter={companyIdsFilter}
          municipalOverview={municipalOverview}
          municipalOverviewLoading={municipalOverviewLoading}
          chartPrimaryColor={chartPrimaryColor}
        />
      )}
    </div>
  )
}
