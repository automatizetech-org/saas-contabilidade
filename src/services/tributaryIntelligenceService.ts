import { supabase } from "./supabaseClient"
import { calculateSimpleNational } from "@/modules/tributary-intelligence/simplesNationalEngine"
import { formatMonthLabel, roundTo } from "@/modules/tributary-intelligence/formatters"
import { RULE_VERSIONS } from "@/modules/tributary-intelligence/simplesNationalRules"
import type {
  SimpleNationalAnnexCode,
  PeriodMonth,
  SimpleNationalCalculationResult,
  SimpleNationalCurrentRevenueAllocationInput,
  SimpleNationalDraftInput,
  SimpleNationalDraftPayload,
  SimpleNationalHistoricalRevenueAllocationInput,
  SimpleNationalPayrollCompositionInput,
  SimpleNationalRevenueSegmentInput,
  TaxIntelligenceOverview,
} from "@/modules/tributary-intelligence/types"

function toIsoMonth(referenceMonth: string) {
  if (!/^\d{4}-\d{2}$/.test(referenceMonth)) throw new Error("Período inválido. Use YYYY-MM.")
  return referenceMonth
}

function defaultSegments(currentPeriodRevenue = 0): SimpleNationalRevenueSegmentInput[] {
  return [
    {
      id: "segment-standard",
      label: "Receita padrão",
      kind: "standard",
      market: "internal",
      amount: currentPeriodRevenue,
    },
  ]
}

function defaultHistoricalRevenueAllocations(
  months: PeriodMonth[],
  baseAnnex: "I" | "II" | "III" | "IV" | "V" = "I"
): SimpleNationalHistoricalRevenueAllocationInput[] {
  return months.map((month, index) => ({
    id: `historical-${index}`,
    referenceMonth: month.referenceMonth,
    annex: baseAnnex,
    amount: 0,
  }))
}

function defaultCurrentPeriodAllocations(
  baseAnnex: SimpleNationalAnnexCode = "III"
): SimpleNationalCurrentRevenueAllocationInput[] {
  return [
    {
      id: "current-1",
      annex: baseAnnex,
      amount: 0,
    },
  ]
}

function defaultPayrollComposition(): SimpleNationalPayrollCompositionInput {
  return {
    employeesAmount: 0,
    proLaboreAmount: 0,
    individualContractorsAmount: 0,
    thirteenthSalaryAmount: 0,
    employerCppAmount: 0,
    fgtsAmount: 0,
    excludedProfitDistributionAmount: 0,
    excludedRentAmount: 0,
    excludedInternsAmount: 0,
    excludedMeiAmount: 0,
  }
}

export function generateRetroactiveMonths(apurationPeriod: string): PeriodMonth[] {
  const [year, month] = toIsoMonth(apurationPeriod).split("-").map(Number)

  return Array.from({ length: 12 }, (_, index) => {
    const date = new Date(year, month - 2 - (11 - index), 1)
    const referenceMonth = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, "0")}`

    return {
      referenceMonth,
      label: formatMonthLabel(referenceMonth),
    }
  })
}

async function syncDefaultRuleVersions() {
  const payload = RULE_VERSIONS.map((item) => ({
    regime: "simples_nacional",
    scope: item.scope,
    version_code: item.versionCode,
    effective_from: item.effectiveFrom,
    effective_to: item.effectiveTo,
    title: item.title,
    source_reference: item.sourceReference,
    source_url: item.sourceUrl,
    payload: { summary: item.summary },
  }))

  const { error } = await supabase
    .from("tax_rule_versions")
    .upsert(payload, { onConflict: "regime,scope,version_code" })

  if (error) throw error
}

async function upsertDraft(input: SimpleNationalDraftInput) {
  const { data: periodRow, error: periodError } = await supabase
    .from("simple_national_periods")
    .upsert(
      {
        company_id: input.companyId,
        apuration_period: input.apurationPeriod,
        company_start_date: input.companyStartDate,
        current_period_revenue: input.currentPeriodRevenue,
        municipal_iss_rate: input.municipalIssRate,
        subject_to_factor_r: input.subjectToFactorR,
        base_annex: input.baseAnnex,
        activity_label: input.activityLabel,
      },
      { onConflict: "company_id,apuration_period" }
    )
    .select("id, updated_at")
    .single()

  if (periodError) throw periodError

  const revenueRows = input.revenueEntries.map((entry) => ({
    period_id: periodRow.id,
    company_id: input.companyId,
    reference_month: entry.referenceMonth,
    entry_type: "revenue",
    amount: entry.amount,
  }))
  const payrollRows = input.payrollEntries.map((entry) => ({
    period_id: periodRow.id,
    company_id: input.companyId,
    reference_month: entry.referenceMonth,
    entry_type: "payroll",
    amount: entry.amount,
  }))

  const { error: deleteEntriesError } = await supabase
    .from("simple_national_entries")
    .delete()
    .eq("period_id", periodRow.id)

  if (deleteEntriesError) throw deleteEntriesError

  const { error: entriesError } = await supabase
    .from("simple_national_entries")
    .insert([...revenueRows, ...payrollRows])

  if (entriesError) throw entriesError

  const { error: deleteHistoricalAllocationsError } = await supabase
    .from("simple_national_historical_revenue_allocations")
    .delete()
    .eq("period_id", periodRow.id)

  if (deleteHistoricalAllocationsError) throw deleteHistoricalAllocationsError

  const historicalAllocationRows = input.historicalRevenueAllocations
    .filter((item) => Number(item.amount ?? 0) > 0)
    .map((item) => ({
      period_id: periodRow.id,
      company_id: input.companyId,
      reference_month: item.referenceMonth,
      annex_code: item.annex,
      amount: item.amount,
    }))

  if (historicalAllocationRows.length > 0) {
    const { error: insertHistoricalAllocationsError } = await supabase
      .from("simple_national_historical_revenue_allocations")
      .insert(historicalAllocationRows)

    if (insertHistoricalAllocationsError) throw insertHistoricalAllocationsError
  }

  const { error: deleteSegmentsError } = await supabase
    .from("simple_national_revenue_segments")
    .delete()
    .eq("period_id", periodRow.id)

  if (deleteSegmentsError) throw deleteSegmentsError

  const segmentRows = input.currentPeriodAllocations.map((segment, index) => ({
    period_id: periodRow.id,
    company_id: input.companyId,
    segment_code: segment.annex,
    market_type: "internal",
    description: `Apuração do mês - Anexo ${segment.annex}`,
    amount: segment.amount,
    display_order: index,
  }))

  if (segmentRows.length > 0) {
    const { error: insertSegmentsError } = await supabase
      .from("simple_national_revenue_segments")
      .insert(segmentRows)

    if (insertSegmentsError) throw insertSegmentsError
  }

  const { error: deleteCompositionError } = await supabase
    .from("simple_national_payroll_compositions")
    .delete()
    .eq("period_id", periodRow.id)

  if (deleteCompositionError) throw deleteCompositionError

  if (input.payrollComposition) {
    const { error: insertCompositionError } = await supabase
      .from("simple_national_payroll_compositions")
      .insert({
        period_id: periodRow.id,
        company_id: input.companyId,
        employees_amount: input.payrollComposition.employeesAmount,
        pro_labore_amount: input.payrollComposition.proLaboreAmount,
        individual_contractors_amount: input.payrollComposition.individualContractorsAmount,
        thirteenth_salary_amount: input.payrollComposition.thirteenthSalaryAmount,
        employer_cpp_amount: input.payrollComposition.employerCppAmount,
        fgts_amount: input.payrollComposition.fgtsAmount,
        excluded_profit_distribution_amount: input.payrollComposition.excludedProfitDistributionAmount,
        excluded_rent_amount: input.payrollComposition.excludedRentAmount,
        excluded_interns_amount: input.payrollComposition.excludedInternsAmount,
        excluded_mei_amount: input.payrollComposition.excludedMeiAmount,
      })

    if (insertCompositionError) throw insertCompositionError
  }

  return periodRow
}

export async function saveSimpleNationalDraft(input: SimpleNationalDraftInput) {
  await syncDefaultRuleVersions()
  return upsertDraft(input)
}

export async function getSimpleNationalDraft(companyId: string, apurationPeriod: string): Promise<SimpleNationalDraftPayload> {
  await syncDefaultRuleVersions()
  const months = generateRetroactiveMonths(apurationPeriod)

  const { data: periodRow, error: periodError } = await supabase
    .from("simple_national_periods")
    .select("id, company_id, apuration_period, company_start_date, current_period_revenue, municipal_iss_rate, subject_to_factor_r, base_annex, activity_label, updated_at")
    .eq("company_id", companyId)
    .eq("apuration_period", apurationPeriod)
    .maybeSingle()

  if (periodError) throw periodError

  if (!periodRow) {
    return {
      companyId,
      apurationPeriod,
      companyStartDate: null,
      currentPeriodRevenue: 0,
      municipalIssRate: null,
      subjectToFactorR: false,
      baseAnnex: "III",
      activityLabel: "Simulação do Simples Nacional",
      revenueEntries: months.map((month) => ({ referenceMonth: month.referenceMonth, amount: 0 })),
      historicalRevenueAllocations: defaultHistoricalRevenueAllocations(months),
      payrollEntries: months.map((month) => ({ referenceMonth: month.referenceMonth, amount: 0 })),
      currentPeriodAllocations: defaultCurrentPeriodAllocations(),
      currentPeriodSegments: defaultSegments(),
      payrollComposition: defaultPayrollComposition(),
      months,
      lastCalculation: null,
      updatedAt: null,
    }
  }

  const [{ data: entries, error: entriesError }, { data: historicalAllocations, error: historicalAllocationsError }, { data: segments, error: segmentsError }, { data: payrollCompositionRow, error: payrollCompositionError }, { data: calcRow, error: calcError }] = await Promise.all([
    supabase
      .from("simple_national_entries")
      .select("reference_month, entry_type, amount")
      .eq("period_id", periodRow.id),
    supabase
      .from("simple_national_historical_revenue_allocations")
      .select("reference_month, annex_code, amount")
      .eq("period_id", periodRow.id),
    supabase
      .from("simple_national_revenue_segments")
      .select("id, segment_code, market_type, description, amount, display_order")
      .eq("period_id", periodRow.id)
      .order("display_order", { ascending: true }),
    supabase
      .from("simple_national_payroll_compositions")
      .select("*")
      .eq("period_id", periodRow.id)
      .maybeSingle(),
    supabase
      .from("simple_national_calculations")
      .select("result_payload")
      .eq("period_id", periodRow.id)
      .maybeSingle(),
  ])

  if (entriesError) throw entriesError
  if (historicalAllocationsError) throw historicalAllocationsError
  if (segmentsError) throw segmentsError
  if (payrollCompositionError) throw payrollCompositionError
  if (calcError) throw calcError

  const amountByType = {
    revenue: new Map<string, number>(),
    payroll: new Map<string, number>(),
  }

  for (const entry of entries ?? []) {
    amountByType[entry.entry_type as "revenue" | "payroll"].set(entry.reference_month, Number(entry.amount ?? 0))
  }

  return {
    companyId,
    apurationPeriod,
    companyStartDate: periodRow.company_start_date,
    currentPeriodRevenue: Number(periodRow.current_period_revenue ?? 0),
    municipalIssRate: periodRow.municipal_iss_rate == null ? null : Number(periodRow.municipal_iss_rate),
    subjectToFactorR: Boolean(periodRow.subject_to_factor_r),
    baseAnnex: periodRow.base_annex as "I" | "II" | "III" | "IV" | "V",
    activityLabel: periodRow.activity_label ?? "",
    revenueEntries: months.map((month) => ({
      referenceMonth: month.referenceMonth,
      amount: amountByType.revenue.get(month.referenceMonth) ?? 0,
    })),
    historicalRevenueAllocations: (historicalAllocations ?? []).length > 0
      ? (historicalAllocations ?? []).map((item, index) => ({
        id: `historical-${index}`,
        referenceMonth: item.reference_month,
        annex: item.annex_code as "I" | "II" | "III" | "IV" | "V",
        amount: Number(item.amount ?? 0),
      }))
      : months.map((month) => ({
        id: `historical-${month.referenceMonth}`,
        referenceMonth: month.referenceMonth,
        annex: periodRow.base_annex as "I" | "II" | "III" | "IV" | "V",
        amount: amountByType.revenue.get(month.referenceMonth) ?? 0,
      })),
    payrollEntries: months.map((month) => ({
      referenceMonth: month.referenceMonth,
      amount: amountByType.payroll.get(month.referenceMonth) ?? 0,
    })),
    currentPeriodAllocations: (segments ?? []).length > 0
      ? (segments ?? []).map((segment, index) => ({
        id: `current-${index + 1}`,
        annex: (["I", "II", "III", "IV", "V"].includes(segment.segment_code) ? segment.segment_code : periodRow.base_annex) as SimpleNationalAnnexCode,
        amount: Number(segment.amount ?? 0),
      }))
      : defaultCurrentPeriodAllocations(periodRow.base_annex as SimpleNationalAnnexCode),
    currentPeriodSegments: [],
    payrollComposition: payrollCompositionRow
      ? {
        employeesAmount: Number(payrollCompositionRow.employees_amount ?? 0),
        proLaboreAmount: Number(payrollCompositionRow.pro_labore_amount ?? 0),
        individualContractorsAmount: Number(payrollCompositionRow.individual_contractors_amount ?? 0),
        thirteenthSalaryAmount: Number(payrollCompositionRow.thirteenth_salary_amount ?? 0),
        employerCppAmount: Number(payrollCompositionRow.employer_cpp_amount ?? 0),
        fgtsAmount: Number(payrollCompositionRow.fgts_amount ?? 0),
        excludedProfitDistributionAmount: Number(payrollCompositionRow.excluded_profit_distribution_amount ?? 0),
        excludedRentAmount: Number(payrollCompositionRow.excluded_rent_amount ?? 0),
        excludedInternsAmount: Number(payrollCompositionRow.excluded_interns_amount ?? 0),
        excludedMeiAmount: Number(payrollCompositionRow.excluded_mei_amount ?? 0),
      }
      : defaultPayrollComposition(),
    months,
    lastCalculation: (calcRow?.result_payload as SimpleNationalCalculationResult | null) ?? null,
    updatedAt: periodRow.updated_at,
  }
}

export async function calculateAndPersistSimpleNational(input: SimpleNationalDraftInput): Promise<SimpleNationalCalculationResult> {
  await syncDefaultRuleVersions()
  const months = generateRetroactiveMonths(input.apurationPeriod)
  const periodRow = await upsertDraft(input)
  const result = calculateSimpleNational(input, months)

  const { error } = await supabase
    .from("simple_national_calculations")
    .upsert(
      {
        period_id: periodRow.id,
        company_id: input.companyId,
        rule_version_code: result.ruleVersionCode,
        result_payload: result,
        memory_payload: result.memory,
      },
      { onConflict: "period_id" }
    )

  if (error) throw error

  return result
}

export async function getTaxIntelligenceOverview(companyIds: string[] | null): Promise<TaxIntelligenceOverview> {
  await syncDefaultRuleVersions()

  try {
    const { data, error } = await supabase.rpc("get_tax_intelligence_overview_summary", {
      company_ids: companyIds && companyIds.length > 0 ? companyIds : null,
    })
    if (error) throw error

    const payload = (data ?? {}) as TaxIntelligenceOverview
    return {
      cards: {
        calculosSalvos: Number(payload.cards?.calculosSalvos ?? 0),
        mediaDas: Number(payload.cards?.mediaDas ?? 0),
        mediaAliquotaEfetiva: Number(payload.cards?.mediaAliquotaEfetiva ?? 0),
        empresasAtivas: Number(payload.cards?.empresasAtivas ?? 0),
      },
      byTopic: payload.byTopic ?? [],
      byMonth: (payload.byMonth ?? []).map((item) => ({
        name: /^\d{4}-\d{2}$/.test(item.name) ? formatMonthLabel(item.name) : item.name,
        value: Number(item.value ?? 0),
      })),
      annexDistribution: (payload.annexDistribution ?? []).map((item) => ({
        name: item.name,
        value: Number(item.value ?? 0),
      })),
      recentCalculations: (payload.recentCalculations ?? []).map((item) => ({
        ...item,
        estimatedDas: Number(item.estimatedDas ?? 0),
      })),
    }
  } catch {
    // Fallback local enquanto a migration ainda não foi aplicada.
  }

  const companyFilter = companyIds && companyIds.length > 0 ? companyIds : null

  let calculationsQuery = supabase
    .from("simple_national_calculations")
    .select("id, company_id, result_payload, updated_at")
    .order("updated_at", { ascending: false })

  if (companyFilter) calculationsQuery = calculationsQuery.in("company_id", companyFilter)

  const { data: calculations, error } = await calculationsQuery
  if (error) throw error

  const rows = calculations ?? []
  const companyIdsInRows = [...new Set(rows.map((row) => row.company_id))]
  const { data: companies } = companyIdsInRows.length > 0
    ? await supabase.from("companies").select("id, name").in("id", companyIdsInRows)
    : { data: [] as Array<{ id: string; name: string }> }

  const names = new Map((companies ?? []).map((company) => [company.id, company.name]))
  const annexMap = new Map<string, number>()
  const monthMap = new Map<string, number>()
  let totalDas = 0
  let totalRate = 0

  for (const row of rows) {
    const payload = row.result_payload as SimpleNationalCalculationResult | null
    if (!payload) continue
    totalDas += Number(payload.estimatedDas ?? 0)
    totalRate += Number(payload.effectiveRate ?? 0)
    annexMap.set(payload.appliedAnnex, (annexMap.get(payload.appliedAnnex) ?? 0) + 1)
    monthMap.set(payload.apurationPeriod, (monthMap.get(payload.apurationPeriod) ?? 0) + 1)
  }

  const byMonth = [...monthMap.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .slice(-6)
    .map(([name, value]) => ({ name: formatMonthLabel(name), value }))

  return {
    cards: {
      calculosSalvos: rows.length,
      mediaDas: rows.length > 0 ? roundTo(totalDas / rows.length, 2) : 0,
      mediaAliquotaEfetiva: rows.length > 0 ? roundTo(totalRate / rows.length, 4) : 0,
      empresasAtivas: new Set(rows.map((row) => row.company_id)).size,
    },
    byTopic: [
      { name: "Visão Geral", value: 1 },
      { name: "Simples Nacional", value: Math.max(rows.length, 1) },
      { name: "Lucro Real", value: 1 },
      { name: "Lucro Presumido", value: 1 },
    ],
    byMonth,
    annexDistribution: [...annexMap.entries()].map(([name, value]) => ({ name: `Anexo ${name}`, value })),
    recentCalculations: rows.slice(0, 5).map((row) => {
      const payload = row.result_payload as SimpleNationalCalculationResult
      return {
        id: row.id,
        companyName: names.get(row.company_id) ?? "Empresa",
        apurationPeriod: payload.apurationPeriod,
        appliedAnnex: payload.appliedAnnex,
        estimatedDas: payload.estimatedDas,
        updatedAt: row.updated_at,
      }
    }),
  }
}
