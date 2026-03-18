import { useEffect, useMemo, useState } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { Link, useParams } from "react-router-dom"
import { ArrowLeft, Building2, Calculator, ChevronDown, Save, TrendingUp, Wallet } from "lucide-react"
import { Bar, BarChart, CartesianGrid, Cell, Pie, PieChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts"
import { toast } from "sonner"
import { useCompanies } from "@/hooks/useCompanies"
import { useSelectedCompanyIds } from "@/hooks/useSelectedCompanies"
import { StatsCard } from "@/components/dashboard/StatsCard"
import { GlassCard } from "@/components/dashboard/GlassCard"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Progress } from "@/components/ui/progress"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import {
  calculateAndPersistSimpleNational,
  generateRetroactiveMonths,
  getSimpleNationalDraft,
} from "@/services/tributaryIntelligenceService"
import { calculateSimpleNational } from "@/modules/tributary-intelligence/simplesNationalEngine"
import {
  currencyInputValue,
  formatCurrencyBRL,
  formatMonthLabel,
  formatPercentBRL,
  formatPeriodLabel,
  parseCurrencyInput,
} from "@/modules/tributary-intelligence/formatters"
import type {
  PeriodMonth,
  SimpleNationalAnnexCode,
  SimpleNationalCalculationResult,
  SimpleNationalCurrentRevenueAllocationInput,
  TaxIntelligenceTopic,
} from "@/modules/tributary-intelligence/types"

const DONUT_COLORS = ["#2563eb", "#f59e0b", "#10b981", "#0f172a", "#ef4444", "#14b8a6", "#f97316", "#0891b2"]

const HISTORICAL_ANNEX_COLUMNS: SimpleNationalAnnexCode[] = ["I", "II", "III", "IV", "V"]

function buildHistoricalRevenueMap(
  months: PeriodMonth[],
  revenues: Array<{ referenceMonth: string; amount: number }>
) {
  return Object.fromEntries(
    months.map((month) => {
      const amount = revenues.find((item) => item.referenceMonth === month.referenceMonth)?.amount ?? 0
      return [month.referenceMonth, currencyInputValue(amount)]
    })
  ) as Record<string, string>
}

function buildCurrentAllocationMap(
  allocations: SimpleNationalCurrentRevenueAllocationInput[]
) {
  return Object.fromEntries(
    HISTORICAL_ANNEX_COLUMNS.map((annex) => {
      const amount = allocations
        .filter((item) => item.annex === annex)
        .reduce((acc, item) => acc + Number(item.amount ?? 0), 0)

      return [annex, currencyInputValue(amount)]
    })
  ) as Record<SimpleNationalAnnexCode, string>
}

function InputGrid({ title, subtitle, months, values, onChange, footerText }: {
  title: string
  subtitle: string
  months: PeriodMonth[]
  values: Record<string, string>
  onChange: (referenceMonth: string, value: string) => void
  footerText?: string
}) {
  return (
    <GlassCard className="flex h-full flex-col overflow-hidden">
      <div className="border-b border-border p-4">
        <h3 className="text-sm font-semibold font-display">{title}</h3>
        <p className="mt-1 text-xs text-muted-foreground">{subtitle}</p>
      </div>
      <div className="flex-1 overflow-x-auto p-4">
          <table className="min-w-[420px] w-full text-xs">
          <thead>
            <tr className="border-b border-border bg-muted/40">
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">Mês</th>
              <th className="px-3 py-3 text-left font-medium text-muted-foreground">Valor</th>
            </tr>
          </thead>
          <tbody>
            {months.map((month) => (
              <tr key={month.referenceMonth} className="border-b border-border last:border-0">
                <td className="w-[28%] px-3 py-3 font-medium whitespace-nowrap">{formatMonthLabel(month.referenceMonth)}</td>
                <td className="px-3 py-3">
                  <Input value={values[month.referenceMonth] ?? ""} onChange={(event) => onChange(month.referenceMonth, event.target.value)} inputMode="decimal" placeholder="R$ 0,00" />
                </td>
              </tr>
            ))}
          </tbody>
          </table>
        </div>
      {footerText ? (
        <div className="px-4 pb-4">
          <div className="rounded-2xl border border-dashed border-border bg-background/40 p-3 text-xs text-muted-foreground">
            {footerText}
          </div>
        </div>
      ) : null}
    </GlassCard>
  )
}

function StandardInputGrid({ title, subtitle, months, values, onChange, footerText, valueLabel }: {
  title: string
  subtitle: string
  months: PeriodMonth[]
  values: Record<string, string>
  onChange: (referenceMonth: string, value: string) => void
  footerText?: string
  valueLabel: string
}) {
  return (
    <GlassCard className="flex h-full flex-col overflow-hidden">
      <div className="border-b border-border p-4">
        <h3 className="text-sm font-semibold font-display">{title}</h3>
        <p className="mt-1 text-xs text-muted-foreground">{subtitle}</p>
      </div>
      <div className="flex-1 overflow-x-auto p-4">
        <div className="mx-auto w-full max-w-[760px]">
          <div className="min-w-[420px] overflow-hidden rounded-2xl border border-border bg-muted/20">
            <div className="grid grid-cols-[180px_minmax(0,1fr)] border-b border-border bg-muted/40 text-xs text-muted-foreground">
              <div className="px-6 py-3 text-center font-medium">Mês</div>
              <div className="px-6 py-3 text-center font-medium">{valueLabel}</div>
            </div>
            <div className="divide-y divide-border">
              {months.map((month) => (
                <div key={month.referenceMonth} className="grid grid-cols-[180px_minmax(0,1fr)] items-center">
                  <div className="px-6 py-3 text-center text-sm font-medium whitespace-nowrap">
                    {formatMonthLabel(month.referenceMonth)}
                  </div>
                  <div className="px-6 py-3">
                    <Input
                      value={values[month.referenceMonth] ?? ""}
                      onChange={(event) => onChange(month.referenceMonth, event.target.value)}
                      inputMode="decimal"
                      placeholder="R$ 0,00"
                    />
                  </div>
                </div>
              ))}
            </div>
          </div>
          {footerText ? (
            <div className="mt-3 rounded-2xl border border-dashed border-border bg-background/40 p-3 text-xs text-muted-foreground">
              {footerText}
            </div>
          ) : null}
        </div>
      </div>
    </GlassCard>
  )
}

function renderPieLabel(props: {
  cx?: number
  cy?: number
  midAngle?: number
  outerRadius?: number
  fill?: string
  name?: string
  value?: number
}) {
  const { cx = 0, cy = 0, midAngle = 0, outerRadius = 0, fill = "#2563eb", name = "", value = 0 } = props
  const radians = Math.PI / 180
  const sin = Math.sin(-radians * midAngle)
  const cos = Math.cos(-radians * midAngle)
  const startX = cx + (outerRadius + 4) * cos
  const startY = cy + (outerRadius + 4) * sin
  const midX = cx + (outerRadius + 22) * cos
  const midY = cy + (outerRadius + 22) * sin
  const endX = midX + (cos >= 0 ? 54 : -54)
  const endY = midY
  const textAnchor = cos >= 0 ? "start" : "end"

  return (
    <g>
      <path
        d={`M ${startX} ${startY} L ${midX} ${midY} L ${endX} ${endY}`}
        fill="none"
        stroke={fill}
        strokeWidth={2.5}
        strokeLinecap="round"
      />
      <circle cx={endX} cy={endY} r={3} fill={fill} />
      <text
        x={endX + (cos >= 0 ? 8 : -8)}
        y={endY}
        textAnchor={textAnchor}
        dominantBaseline="central"
        fill="hsl(var(--foreground))"
        fontSize={12}
        fontWeight={700}
      >
        {`${name} - ${formatCurrencyBRL(value)}`}
      </text>
    </g>
  )
}

function PlaceholderTopic({ label, description }: { label: string; description: string }) {
  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2">
        <Link to="/inteligencia-tributaria" className="rounded-lg border border-border bg-card p-2 text-muted-foreground hover:bg-muted transition-colors" aria-label="Voltar">
          <ArrowLeft className="h-4 w-4" />
        </Link>
        <div>
          <h1 className="text-2xl font-bold font-display tracking-tight">{label}</h1>
          <p className="mt-1 text-sm text-muted-foreground">{description}</p>
        </div>
      </div>
    </div>
  )
}

export default function InteligenciaTributariaTopicPage() {
  const { topic = "simples-nacional" } = useParams<{ topic: TaxIntelligenceTopic }>()
  const { data: companies = [] } = useCompanies()
  const { selectedCompanyIds } = useSelectedCompanyIds()
  const queryClient = useQueryClient()
  const defaultCompanyId = selectedCompanyIds[0] ?? companies[0]?.id ?? ""
  const now = new Date()

  const [companyId, setCompanyId] = useState(defaultCompanyId)
  const [apurationPeriod, setApurationPeriod] = useState(`${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`)
  const [companyStartDate, setCompanyStartDate] = useState("")
  const [subjectToFactorR, setSubjectToFactorR] = useState(false)
  const [municipalIssRate, setMunicipalIssRate] = useState("5")
  const [currentPeriodRevenueInput, setCurrentPeriodRevenueInput] = useState("")
  const [historicalRevenueMap, setHistoricalRevenueMap] = useState<Record<string, string>>({})
  const [currentRevenueMap, setCurrentRevenueMap] = useState<Record<SimpleNationalAnnexCode, string>>({ I: "", II: "", III: "", IV: "", V: "" })
  const [payrollMap, setPayrollMap] = useState<Record<string, string>>({})
  const [showMemory, setShowMemory] = useState(true)

  const months = useMemo(() => generateRetroactiveMonths(apurationPeriod), [apurationPeriod])
  const resolvedBaseAnnex = useMemo<SimpleNationalAnnexCode>(() => {
    const currentTotals = Object.fromEntries(HISTORICAL_ANNEX_COLUMNS.map((annex) => [annex, parseCurrencyInput(currentRevenueMap[annex] ?? "")])) as Record<SimpleNationalAnnexCode, number>
    const currentAnnexes = [...HISTORICAL_ANNEX_COLUMNS]
      .sort((left, right) => currentTotals[right] - currentTotals[left])
      .filter((item) => currentTotals[item] > 0)

    if (currentAnnexes.length > 0) {
      if (subjectToFactorR && (currentTotals.III > 0 || currentTotals.V > 0)) {
        return currentTotals.III >= currentTotals.V ? "III" : "V"
      }

      return currentAnnexes[0]
    }
    return "III"
  }, [currentRevenueMap, subjectToFactorR])
  const currentServiceRevenue = (parseCurrencyInput(currentRevenueMap.III ?? "") + parseCurrencyInput(currentRevenueMap.V ?? ""))
  const factorREligible = currentServiceRevenue > 0

  const draftQuery = useQuery({
    queryKey: ["simple-national-draft", companyId, apurationPeriod],
    queryFn: () => getSimpleNationalDraft(companyId, apurationPeriod),
    enabled: Boolean(companyId) && topic === "simples-nacional",
  })

  useEffect(() => {
    if (!companyId && defaultCompanyId) setCompanyId(defaultCompanyId)
  }, [companyId, defaultCompanyId])

  useEffect(() => {
    if (!draftQuery.data) return
    const loadedCurrentRevenue = Number(draftQuery.data.currentPeriodRevenue ?? 0)
    const loadedAllocationRevenue = draftQuery.data.currentPeriodAllocations.reduce((acc, item) => acc + Number(item.amount ?? 0), 0)
    setCompanyStartDate(draftQuery.data.companyStartDate ?? "")
    setSubjectToFactorR(draftQuery.data.subjectToFactorR)
    setMunicipalIssRate(draftQuery.data.municipalIssRate == null ? "5" : String(draftQuery.data.municipalIssRate))
    setCurrentPeriodRevenueInput(currencyInputValue(loadedCurrentRevenue > 0 ? loadedCurrentRevenue : loadedAllocationRevenue))
    setHistoricalRevenueMap(buildHistoricalRevenueMap(months, draftQuery.data.revenueEntries))
    setCurrentRevenueMap(buildCurrentAllocationMap(draftQuery.data.currentPeriodAllocations))
    setPayrollMap(Object.fromEntries(draftQuery.data.payrollEntries.map((item) => [item.referenceMonth, currencyInputValue(item.amount)])))
  }, [draftQuery.data, months])

  const serializeDraft = () => {
    const parsedIssRate = Number.parseFloat(municipalIssRate.replace(",", "."))
    const historicalRevenueAllocations = months.map((month) => ({
      id: `${month.referenceMonth}-${resolvedBaseAnnex}`,
      referenceMonth: month.referenceMonth,
      annex: resolvedBaseAnnex,
      amount: parseCurrencyInput(historicalRevenueMap[month.referenceMonth] ?? ""),
    }))
    const currentPeriodAllocations = HISTORICAL_ANNEX_COLUMNS.map((annex) => ({
      id: `current-${annex}`,
      annex,
      amount: parseCurrencyInput(currentRevenueMap[annex] ?? ""),
    }))
    const currentPeriodRevenue = parseCurrencyInput(currentPeriodRevenueInput)

    return {
      companyId,
      apurationPeriod,
      companyStartDate: companyStartDate || null,
      currentPeriodRevenue,
      municipalIssRate: Number.isFinite(parsedIssRate) ? parsedIssRate : null,
      subjectToFactorR,
      baseAnnex: resolvedBaseAnnex,
      activityLabel: `Simulação com faixa pelo RBT12 e apuração pelo Anexo ${resolvedBaseAnnex}`,
      revenueEntries: months.map((month) => ({
        referenceMonth: month.referenceMonth,
        amount: parseCurrencyInput(historicalRevenueMap[month.referenceMonth] ?? ""),
      })),
      historicalRevenueAllocations,
      currentPeriodAllocations,
      payrollEntries: months.map((month) => ({ referenceMonth: month.referenceMonth, amount: parseCurrencyInput(payrollMap[month.referenceMonth] ?? "") })),
      currentPeriodSegments: [],
      payrollComposition: null,
    }
  }

  const saveMutation = useMutation({
    mutationFn: () => calculateAndPersistSimpleNational(serializeDraft()),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["simple-national-draft", companyId, apurationPeriod] }),
        queryClient.invalidateQueries({ queryKey: ["tax-intelligence-overview"] }),
      ])
      toast.success("Simulação salva.")
    },
  })

  if (topic === "lucro-real") return <PlaceholderTopic label="Lucro Real" description="Estrutura inicial pronta para expansão." />
  if (topic === "lucro-presumido") return <PlaceholderTopic label="Lucro Presumido" description="Estrutura inicial pronta para expansão." />

  const draft = useMemo(() => serializeDraft(), [companyId, apurationPeriod, companyStartDate, municipalIssRate, currentPeriodRevenueInput, factorREligible, subjectToFactorR, resolvedBaseAnnex, historicalRevenueMap, currentRevenueMap, payrollMap, months])
  const result = useMemo<SimpleNationalCalculationResult | null>(() => {
    if (!companyId) return null
    return calculateSimpleNational(draft, months)
  }, [companyId, draft, months])
  const factorRValue = result?.factorR != null ? result.factorR * 100 : null
  const factorRProgress = factorRValue == null ? 0 : Math.min((factorRValue / 28) * 100, 100)
  const revenueChart = months.map((month) => ({
    name: month.label,
    value: parseCurrencyInput(historicalRevenueMap[month.referenceMonth] ?? ""),
  }))
  const payrollChart = months.map((month) => ({ name: month.label, value: parseCurrencyInput(payrollMap[month.referenceMonth] ?? "") }))
  const payrollBase12Preview = months.reduce((acc, month) => acc + parseCurrencyInput(payrollMap[month.referenceMonth] ?? ""), 0)
  const employerCpp12Preview = payrollBase12Preview * 0.2
  const fgts12Preview = payrollBase12Preview * 0.08
  const fs12Preview = payrollBase12Preview + employerCpp12Preview + fgts12Preview
  const currentRevenuePreview = HISTORICAL_ANNEX_COLUMNS.reduce((acc, annex) => acc + parseCurrencyInput(currentRevenueMap[annex] ?? ""), 0)
  const updateHistoricalRevenue = (referenceMonth: string, raw: string) => {
    setHistoricalRevenueMap((current) => ({
      ...current,
      [referenceMonth]: raw,
    }))
  }

  const updateCurrentRevenue = (annex: SimpleNationalAnnexCode, raw: string) => {
    setCurrentRevenueMap((current) => ({
      ...current,
      [annex]: raw,
    }))
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2">
        <Link to="/inteligencia-tributaria" className="rounded-lg border border-border bg-card p-2 text-muted-foreground hover:bg-muted transition-colors" aria-label="Voltar">
          <ArrowLeft className="h-4 w-4" />
        </Link>
        <div>
          <h1 className="text-2xl font-bold font-display tracking-tight">Simples Nacional</h1>
          <p className="mt-1 text-sm text-muted-foreground">Motor com proporcionalização, segregação de receitas, exceção do Anexo II e FS12 estruturada.</p>
        </div>
      </div>

      <GlassCard className="p-5">
        <div className="grid grid-cols-1 gap-3 items-end xl:grid-cols-[1.2fr_0.9fr_0.9fr_0.9fr_0.8fr_auto]">
          <div className="space-y-2">
            <Label>Empresa</Label>
            <Select value={companyId || "none"} onValueChange={(value) => setCompanyId(value === "none" ? "" : value)}>
              <SelectTrigger><SelectValue placeholder="Selecione a empresa" /></SelectTrigger>
              <SelectContent>
                <SelectItem value="none">Selecione</SelectItem>
                {companies.map((company) => <SelectItem key={company.id} value={company.id}>{company.name}</SelectItem>)}
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>RPA do mês</Label>
            <Input
              value={currentPeriodRevenueInput}
              onChange={(event) => setCurrentPeriodRevenueInput(event.target.value)}
              inputMode="decimal"
              placeholder="R$ 0,00"
            />
          </div>
          <div className="space-y-2">
            <Label>Data de apuração</Label>
            <Input type="month" value={apurationPeriod} onChange={(event) => setApurationPeriod(event.target.value)} />
          </div>
          <div className="space-y-2">
            <Label>Sujeito ao fator R</Label>
            <Select value={subjectToFactorR ? "sim" : "nao"} onValueChange={(value) => setSubjectToFactorR(value === "sim")}>
              <SelectTrigger><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="sim">Sim</SelectItem>
                <SelectItem value="nao">Não</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div className="space-y-2">
            <Label>ISS do município</Label>
            <Input
              value={municipalIssRate}
              onChange={(event) => setMunicipalIssRate(event.target.value)}
              inputMode="decimal"
              placeholder="2 a 5"
            />
          </div>
          <Button variant="outline" onClick={() => saveMutation.mutate()} disabled={!companyId || saveMutation.isPending}>
            <Save className="mr-2 h-4 w-4" />
            Salvar
          </Button>
        </div>
      </GlassCard>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-1">
        <GlassCard className="p-5 space-y-3">
          <div>
            <p className="text-xs uppercase tracking-[0.2em] text-muted-foreground">Período carregado</p>
            <p className="mt-2 text-2xl font-bold font-display">{formatPeriodLabel(apurationPeriod)}</p>
            <p className="mt-2 text-xs text-muted-foreground">12 meses anteriores ao PA: {months[0]?.label} até {months[months.length - 1]?.label}</p>
          </div>
          <p className="text-xs text-muted-foreground">
            {factorREligible
              ? `Há receitas de serviços sujeitas ao domínio III/V. Se marcar Fator R, o sistema compara FS12/RBT12 contra 28%.`
              : `Sem receitas em III ou V na base informada. O Fator R pode ficar marcado, mas não alterará a simulação atual.`}
          </p>
        </GlassCard>
      </div>

      <div className="grid grid-cols-1 gap-4">
        <GlassCard className="overflow-hidden">
        <div className="border-b border-border p-4">
          <div>
            <h3 className="text-sm font-semibold font-display">Apuração do mês segregada por anexo</h3>
            <p className="mt-1 text-xs text-muted-foreground">O anexo que influencia o cálculo é o da RPA do mês. O RBT12 histórico serve para faixa e alíquota efetiva; a grade abaixo define em qual anexo cada faturamento atual será tributado.</p>
          </div>
        </div>
        <div className="p-4">
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-6">
            {HISTORICAL_ANNEX_COLUMNS.map((annex) => (
              <div key={annex} className="space-y-2">
                <Label className="text-xs text-muted-foreground">{`Anexo ${annex}`}</Label>
                <Input
                  value={currentRevenueMap[annex] ?? ""}
                  onChange={(event) => updateCurrentRevenue(annex, event.target.value)}
                  inputMode="decimal"
                  placeholder="R$ 0,00"
                />
              </div>
            ))}
            <div className="space-y-2 rounded-2xl border border-border bg-background/40 px-4 py-3">
              <p className="text-xs text-muted-foreground">RPA total</p>
              <p className="text-lg font-bold font-display">{formatCurrencyBRL(result?.currentPeriodRevenue ?? currentRevenuePreview)}</p>
            </div>
          </div>
        </div>
      </GlassCard>

      </div>

      <div className="grid grid-cols-1 items-stretch gap-4 xl:grid-cols-2">
      <StandardInputGrid
        title="Receitas dos 12 meses anteriores"
        subtitle="Informe apenas o faturamento total de cada mês. O RBT12 não precisa de separação por anexo; ele serve como base histórica de faixa."
        months={months}
        values={historicalRevenueMap}
        onChange={(referenceMonth, value) => updateHistoricalRevenue(referenceMonth, value)}
        valueLabel="Total do mês"
        footerText="O histórico acima forma apenas o RBT12 da empresa. A definição do anexo da tributação fica na RPA do mês, na grade de apuração atual."
      />

      <StandardInputGrid
        title="Folha total dos 12 meses anteriores"
        subtitle="Informe o valor mensal da folha. O sistema calcula automaticamente CPP patronal de 20% e FGTS de 8% para formar a FS12."
        months={months}
        values={payrollMap}
        onChange={(referenceMonth, value) => setPayrollMap((current) => ({ ...current, [referenceMonth]: value }))}
        valueLabel="Valor"
        footerText="A folha acima compõe a FS12 da empresa. O sistema usa essa base para calcular automaticamente CPP patronal de 20% e FGTS de 8% no Fator R."
      />
      </div>
        <GlassCard className="p-5">
          <div>
            <h3 className="text-sm font-semibold font-display">Resumo automático da FS12</h3>
            <p className="mt-1 text-xs text-muted-foreground">Base padrão usada no Fator R para a folha informada.</p>
          </div>
          <div className="mt-4 grid grid-cols-1 gap-3 md:grid-cols-2">
            <div className="rounded-2xl border border-border bg-background/60 p-4">
              <p className="text-xs text-muted-foreground">Folha base 12 meses</p>
              <p className="mt-2 text-lg font-bold font-display">{formatCurrencyBRL(result?.payrollBase12 ?? payrollBase12Preview)}</p>
            </div>
            <div className="rounded-2xl border border-border bg-background/60 p-4">
              <p className="text-xs text-muted-foreground">CPP patronal 20%</p>
              <p className="mt-2 text-lg font-bold font-display">{formatCurrencyBRL(result?.employerCpp12 ?? employerCpp12Preview)}</p>
            </div>
            <div className="rounded-2xl border border-border bg-background/60 p-4">
              <p className="text-xs text-muted-foreground">FGTS 8%</p>
              <p className="mt-2 text-lg font-bold font-display">{formatCurrencyBRL(result?.fgts12 ?? fgts12Preview)}</p>
            </div>
            <div className="rounded-2xl border border-border bg-background/60 p-4">
              <p className="text-xs text-muted-foreground">FS12 considerada</p>
              <p className="mt-2 text-lg font-bold font-display">{formatCurrencyBRL(result?.fs12 ?? fs12Preview)}</p>
            </div>
          </div>
          <div className="mt-4 rounded-2xl border border-dashed border-border bg-background/40 p-3 text-xs text-muted-foreground">
            Regra padrão aplicada: `CPP patronal = 20%` e `FGTS = 8%` sobre a folha informada. Isso atende ao fluxo direto da simulação; casos excepcionais exigem ajuste normativo específico.
          </div>
        </GlassCard>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatsCard title="DAS estimado" value={formatCurrencyBRL(result?.estimatedDas ?? 0)} icon={Wallet} />
        <StatsCard title="RBT12" value={formatCurrencyBRL(result?.rbt12 ?? 0)} icon={TrendingUp} />
        <StatsCard title="RBT12 para faixa" value={formatCurrencyBRL(result?.rbt12ForBracket ?? 0)} icon={TrendingUp} />
        <StatsCard title="Anexo aplicado" value={result ? `Anexo ${result.appliedAnnex}` : "-"} icon={Building2} />
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-4">
        <StatsCard title="Alíquota nominal" value={result ? formatPercentBRL(result.nominalRate, 2) : "-"} icon={TrendingUp} />
        <StatsCard title="Alíquota efetiva" value={result ? formatPercentBRL(result.effectiveRate, 4) : "-"} icon={TrendingUp} />
        <StatsCard title="Fator R" value={factorRValue == null ? "Não aplicável" : formatPercentBRL(factorRValue, 2)} icon={Calculator} />
        <StatsCard title="Faixa aplicada" value={result ? `${result.bracket.bracket}ª faixa` : "-"} icon={Calculator} />
      </div>

      <GlassCard className="overflow-hidden">
        <div className="border-b border-border p-4">
          <h3 className="text-sm font-semibold font-display">Repartição automática dos tributos do Simples Nacional</h3>
          <p className="mt-1 text-xs text-muted-foreground">Valor da guia distribuído por tributo, com a partilha efetiva usada na simulação atual.</p>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-[760px] w-full text-xs">
            <thead>
              <tr className="border-b border-border bg-muted/40">
                <th className="px-4 py-3 text-left font-medium text-muted-foreground">Tributo</th>
                <th className="px-4 py-3 text-left font-medium text-muted-foreground">Partilha</th>
                <th className="px-4 py-3 text-left font-medium text-muted-foreground">Alíquota aplicada</th>
                <th className="px-3 py-3 text-left font-medium text-muted-foreground">Valor</th>
                <th className="px-4 py-3 text-left font-medium text-muted-foreground">% da guia</th>
              </tr>
            </thead>
            <tbody>
              {(result?.breakdown ?? []).map((item) => (
                <tr key={item.tax} className="border-b border-border last:border-0">
                  <td className="px-4 py-3 font-medium">{item.tax}</td>
                  <td className="px-4 py-3">{formatPercentBRL(item.repartitionPercent, 2)}</td>
                  <td className="px-4 py-3">{formatPercentBRL(item.effectiveRate, 4)}</td>
                  <td className="px-4 py-3">{formatCurrencyBRL(item.amount)}</td>
                  <td className="px-4 py-3">{formatPercentBRL(item.shareOfDas, 2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </GlassCard>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
        <GlassCard className="p-6">
          <h3 className="mb-2 text-sm font-semibold font-display">Histórico de faturamento</h3>
          <p className="mb-4 text-xs text-muted-foreground">Últimos 12 meses anteriores ao PA.</p>
          <div className="h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={revenueChart}>
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" opacity={0.35} />
                <XAxis dataKey="name" tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
                <Tooltip formatter={(value: number) => formatCurrencyBRL(value)} />
                <Bar dataKey="value" fill="hsl(var(--primary))" radius={[8, 8, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </GlassCard>

        <GlassCard className="p-6">
          <h3 className="mb-2 text-sm font-semibold font-display">Histórico de folha</h3>
          <p className="mb-4 text-xs text-muted-foreground">Base mensal complementar da FS12.</p>
          <div className="h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={payrollChart}>
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" opacity={0.35} />
                <XAxis dataKey="name" tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
                <Tooltip formatter={(value: number) => formatCurrencyBRL(value)} />
                <Bar dataKey="value" fill="#0f766e" radius={[8, 8, 0, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </GlassCard>
      </div>

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-3">
        <GlassCard className="p-6 xl:col-span-2">
          <h3 className="mb-2 text-sm font-semibold font-display">Composição da guia</h3>
          <p className="mb-4 text-xs text-muted-foreground">Percentual e valor por tributo dentro do DAS.</p>
          <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
            <div className="max-w-[520px]">
              <div className="text-[11px] uppercase tracking-[0.18em] text-muted-foreground">Total do DAS</div>
              <div className="mt-1 text-xl font-bold font-display">{result ? formatCurrencyBRL(result.estimatedDas) : "R$ 0,00"}</div>
            </div>
          </div>
          <div className="h-[380px]">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart margin={{ top: 36, right: 180, bottom: 36, left: 180 }}>
                <Pie
                  data={result?.breakdown ?? []}
                  dataKey="amount"
                  nameKey="tax"
                  innerRadius={64}
                  outerRadius={106}
                  stroke="none"
                  paddingAngle={1}
                  labelLine={false}
                  label={renderPieLabel}
                  isAnimationActive={false}
                >
                  {(result?.breakdown ?? []).map((item, index) => <Cell key={item.tax} fill={DONUT_COLORS[index % DONUT_COLORS.length]} />)}
                </Pie>
                <Tooltip formatter={(value: number) => formatCurrencyBRL(value)} />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </GlassCard>

        <GlassCard className="p-6">
          <h3 className="mb-2 text-sm font-semibold font-display">Nominal x efetiva</h3>
          <div className="h-[280px]">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={[{ name: "Nominal", value: result?.nominalRate ?? 0 }, { name: "Efetiva", value: result?.effectiveRate ?? 0 }]}>
                <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" opacity={0.35} />
                <XAxis dataKey="name" tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fontSize: 11, fill: "hsl(var(--muted-foreground))" }} axisLine={false} tickLine={false} />
                <Tooltip formatter={(value: number) => formatPercentBRL(value, 4)} />
                <Bar dataKey="value" radius={[8, 8, 0, 0]}>
                  <Cell fill="#0f172a" />
                  <Cell fill="#2563eb" />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </GlassCard>

        <GlassCard className="p-6 space-y-4">
          <div>
            <h3 className="mb-2 text-sm font-semibold font-display">Termômetro do fator R</h3>
            <p className="mb-4 text-xs text-muted-foreground">Linha de corte legal em 28%.</p>
          </div>
          <div className="rounded-2xl border border-border bg-background/60 p-4">
            <div className="mb-2 flex items-center justify-between text-xs text-muted-foreground"><span>Atual</span><span>28%</span></div>
            <Progress value={factorRProgress} className="h-3" />
            <div className="mt-3 flex items-center justify-between gap-3">
              <span className="text-sm font-semibold font-display">{factorRValue == null ? "-" : formatPercentBRL(factorRValue, 2)}</span>
              <span className="text-right text-xs text-muted-foreground">{result?.annexReason ?? "Aguardando cálculo"}</span>
            </div>
            <div className="mt-3 rounded-xl border border-border bg-background/50 px-3 py-2 text-xs text-muted-foreground">
              {result ? `FS12 ${formatCurrencyBRL(result.fs12)} / RBT12 ${formatCurrencyBRL(result.rbt12)} = ${factorRValue == null ? "-" : formatPercentBRL(factorRValue, 2)}` : "Preencha receita e folha para visualizar a fórmula."}
            </div>
          </div>
          <div className="rounded-2xl border border-border bg-background/60 p-4">
            <p className="text-xs text-muted-foreground">Faixa atual</p>
            <p className="mt-2 text-lg font-bold font-display">{result ? `${result.bracket.bracket}ª faixa` : "-"}</p>
            <p className="mt-2 text-xs text-muted-foreground">
              {result ? `${formatCurrencyBRL(result.bracket.rangeStart)} a ${formatCurrencyBRL(result.bracket.rangeEnd)}` : "Sem enquadramento calculado."}
            </p>
          </div>
        </GlassCard>
      </div>

      <GlassCard className="overflow-hidden">
        <div className="flex items-center justify-between border-b border-border p-4">
          <div>
            <h3 className="text-sm font-semibold font-display">Memória de cálculo</h3>
            <p className="mt-1 text-xs text-muted-foreground">Passo a passo completo, sem esconder as premissas usadas pelo motor.</p>
          </div>
          <Button variant="outline" size="sm" onClick={() => setShowMemory((current) => !current)}>
            <ChevronDown className={`mr-2 h-4 w-4 transition-transform ${showMemory ? "rotate-180" : ""}`} />
            {showMemory ? "Recolher" : "Expandir"}
          </Button>
        </div>
        {showMemory && (
          <div className="grid grid-cols-1 gap-4 p-4 xl:grid-cols-[1.2fr_1fr]">
            <div className="space-y-4">
              {["Bloco: Faixa e enquadramento", "Bloco: Segregação da apuração"].map((sectionTitle) => {
                const items = (result?.memory ?? [])
                  .slice(
                    (result?.memory ?? []).findIndex((item) => item.label === sectionTitle) + 1,
                    (() => {
                      const currentIndex = (result?.memory ?? []).findIndex((item) => item.label === sectionTitle)
                      const nextIndex = (result?.memory ?? []).findIndex((item, index) => index > currentIndex && item.label.startsWith("Bloco:"))
                      return nextIndex === -1 ? (result?.memory ?? []).length : nextIndex
                    })()
                  )

                return (
                  <div key={sectionTitle} className="overflow-hidden rounded-2xl border border-border">
                    <div className="border-b border-border bg-muted/40 px-4 py-3">
                      <h4 className="text-sm font-semibold font-display">{sectionTitle.replace("Bloco: ", "")}</h4>
                    </div>
                    <div className="divide-y divide-border">
                      {items.map((item) => (
                        <div key={`${sectionTitle}-${item.label}`} className="grid grid-cols-1 gap-2 px-4 py-3 md:grid-cols-[180px_1fr]">
                          <div className="text-xs font-medium">{item.label}</div>
                          <div>
                            <div className="text-sm font-semibold">{item.value}</div>
                            {item.detail ? <div className="mt-1 text-xs text-muted-foreground">{item.detail}</div> : null}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                )
              })}
            </div>

            <div className="overflow-hidden rounded-2xl border border-border">
              <table className="w-full text-xs">
                <thead>
                  <tr className="border-b border-border bg-muted/40">
                    <th className="px-4 py-3 text-left font-medium text-muted-foreground">Tributo</th>
                    <th className="px-4 py-3 text-left font-medium text-muted-foreground">Partilha</th>
                    <th className="px-4 py-3 text-left font-medium text-muted-foreground">Alíquota efetiva</th>
                    <th className="px-3 py-3 text-left font-medium text-muted-foreground">Valor</th>
                  </tr>
                </thead>
                <tbody>
                  {(result?.breakdown ?? []).map((item) => (
                    <tr key={item.tax} className="border-b border-border last:border-0">
                      <td className="px-4 py-3 font-medium">{item.tax}</td>
                      <td className="px-4 py-3">{item.repartitionPercent > 0 ? formatPercentBRL(item.repartitionPercent, 2) : "Regra especial"}</td>
                      <td className="px-4 py-3">{formatPercentBRL(item.effectiveRate, 4)}</td>
                      <td className="px-4 py-3">{formatCurrencyBRL(item.amount)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
        {(result?.warnings ?? []).length ? (
          <div className="px-4 pb-4">
            <div className="rounded-xl border border-amber-500/30 bg-amber-500/10 px-4 py-3 text-xs text-amber-200">
              {(result?.warnings ?? []).join(" ")}
            </div>
          </div>
        ) : null}
      </GlassCard>
    </div>
  )
}
