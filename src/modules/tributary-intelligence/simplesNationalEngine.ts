import { formatCurrencyBRL, formatPercentBRL, formatPeriodLabel, roundTo } from "./formatters"
import { RULE_VERSIONS, SIMPLE_NATIONAL_RULES } from "./simplesNationalRules"
import type {
  AnnexBracket,
  CalculationMemoryItem,
  PeriodMonth,
  RevenueSegmentCalculationResult,
  SimpleNationalCalculationResult,
  SimpleNationalDraftInput,
  SimpleNationalEntryInput,
  SimpleNationalPayrollCompositionInput,
  SimpleNationalAnnexCode,
  SimpleNationalRevenueSegmentInput,
  TaxBreakdownItem,
} from "./types"

const FACTOR_R_THRESHOLD = 0.28
const EMPLOYER_CPP_RATE = 0.2
const FGTS_RATE = 0.08
const TAX_ORDER: TaxBreakdownItem["tax"][] = ["IRPJ", "CSLL", "COFINS", "PIS/PASEP", "CPP", "ISS", "IPI", "ICMS"]

function sumEntries(entries: SimpleNationalEntryInput[]) {
  return roundTo(entries.reduce((acc, entry) => acc + Number(entry.amount || 0), 0), 2)
}

function parseIsoMonth(reference: string) {
  const [year, month] = reference.split("-").map(Number)
  return { year, month }
}

function monthsBetweenInclusive(startDate: string, apurationPeriod: string) {
  const start = new Date(`${startDate}T00:00:00`)
  const { year, month } = parseIsoMonth(apurationPeriod)
  const apurationStart = new Date(year, month - 1, 1)
  return (apurationStart.getFullYear() - start.getFullYear()) * 12 + (apurationStart.getMonth() - start.getMonth()) + 1
}

function resolveStartupMetrics(input: SimpleNationalDraftInput) {
  if (!input.companyStartDate) {
    return { isStartupPeriod: false, startupMonthOrdinal: null }
  }

  const startupMonthOrdinal = monthsBetweenInclusive(input.companyStartDate, input.apurationPeriod)
  return {
    isStartupPeriod: startupMonthOrdinal >= 1 && startupMonthOrdinal <= 12,
    startupMonthOrdinal,
  }
}

function resolveRbt12ForBracket(input: SimpleNationalDraftInput, months: PeriodMonth[], rbt12: number) {
  const startup = resolveStartupMetrics(input)

  if (!startup.isStartupPeriod || !input.companyStartDate || startup.startupMonthOrdinal == null) {
    return startup.startupMonthOrdinal == null
      ? { rbt12ForBracket: rbt12, ...startup }
      : { rbt12ForBracket: rbt12, ...startup }
  }

  if (startup.startupMonthOrdinal === 1) {
    return {
      rbt12ForBracket: roundTo(input.currentPeriodRevenue * 12, 2),
      ...startup,
    }
  }

  const startMonth = input.companyStartDate.slice(0, 7)
  const historicalRevenue = months
    .filter((month) => month.referenceMonth >= startMonth && month.referenceMonth < input.apurationPeriod)
    .reduce((acc, month) => {
      const entry = input.revenueEntries.find((item) => item.referenceMonth === month.referenceMonth)
      return acc + Number(entry?.amount ?? 0)
    }, 0)

  const monthsInActivityBeforeCurrent = Math.max(startup.startupMonthOrdinal - 1, 1)
  const averageRevenue = historicalRevenue / monthsInActivityBeforeCurrent

  return {
    rbt12ForBracket: roundTo(averageRevenue * 12, 2),
    ...startup,
  }
}

function resolveBracket(annex: SimpleNationalAnnexCode, rbt12: number): AnnexBracket {
  const bracket = SIMPLE_NATIONAL_RULES[annex].brackets.find(
    (item) => rbt12 >= item.rangeStart && rbt12 <= item.rangeEnd
  )

  if (!bracket) {
    return SIMPLE_NATIONAL_RULES[annex].brackets[SIMPLE_NATIONAL_RULES[annex].brackets.length - 1]
  }

  return bracket
}

function resolvePayrollCompositionTotals(payrollComposition: SimpleNationalPayrollCompositionInput | null) {
  if (!payrollComposition) return null

  const includedTotal = roundTo(
    payrollComposition.employeesAmount +
    payrollComposition.proLaboreAmount +
    payrollComposition.individualContractorsAmount +
    payrollComposition.thirteenthSalaryAmount +
    payrollComposition.employerCppAmount +
    payrollComposition.fgtsAmount,
    2
  )

  const excludedTotal = roundTo(
    payrollComposition.excludedProfitDistributionAmount +
    payrollComposition.excludedRentAmount +
    payrollComposition.excludedInternsAmount +
    payrollComposition.excludedMeiAmount,
    2
  )

  return { includedTotal, excludedTotal }
}

function resolvePayrollTotals(
  payrollEntries: SimpleNationalEntryInput[],
  payrollComposition: SimpleNationalPayrollCompositionInput | null
) {
  const payrollBase12 = sumEntries(payrollEntries)
  const payrollCompositionTotals = resolvePayrollCompositionTotals(payrollComposition)

  if (payrollCompositionTotals) {
    const employerCpp12 = roundTo(payrollComposition.employerCppAmount, 2)
    const fgts12 = roundTo(payrollComposition.fgtsAmount, 2)

    return {
      payrollBase12,
      employerCpp12,
      fgts12,
      fs12: payrollCompositionTotals.includedTotal,
      payrollCompositionTotals,
      payrollMethod: "composition" as const,
    }
  }

  const employerCpp12 = roundTo(payrollBase12 * EMPLOYER_CPP_RATE, 2)
  const fgts12 = roundTo(payrollBase12 * FGTS_RATE, 2)

  return {
    payrollBase12,
    employerCpp12,
    fgts12,
    fs12: roundTo(payrollBase12 + employerCpp12 + fgts12, 2),
    payrollCompositionTotals: null,
    payrollMethod: "automatic" as const,
  }
}

function resolveAppliedAnnex(input: SimpleNationalDraftInput, rbt12: number, fs12: number) {
  return resolveAppliedAnnexForBase(input.baseAnnex, input.subjectToFactorR, rbt12, fs12)
}

function resolveAppliedAnnexForBase(
  baseAnnex: SimpleNationalAnnexCode,
  subjectToFactorR: boolean,
  rbt12: number,
  fs12: number
) {
  const factorREligible = baseAnnex === "III" || baseAnnex === "V"

  if (!subjectToFactorR || !factorREligible) {
    return {
      factorR: null,
      factorRQualified: null,
      appliedAnnex: baseAnnex,
      annexReason: `Atividade apurada diretamente no Anexo ${baseAnnex}.`,
    }
  }

  const factorR = rbt12 > 0 ? fs12 / rbt12 : 0
  const factorRQualified = factorR >= FACTOR_R_THRESHOLD

  return {
    factorR,
    factorRQualified,
    appliedAnnex: factorRQualified ? "III" : "V",
    annexReason: factorRQualified
      ? "Fator R igual ou superior a 28%; atividade deslocada para o Anexo III."
      : "Fator R inferior a 28%; atividade permanece no Anexo V.",
  }
}

function buildSixthBracketWarning(appliedAnnex: SimpleNationalAnnexCode) {
  if (appliedAnnex === "I" || appliedAnnex === "II") {
    return "Receita acima de R$ 3,6 milhões exige monitorar recolhimento de ICMS fora do DAS conforme a regra vigente."
  }

  return "Receita acima de R$ 3,6 milhões exige monitorar recolhimento de ISS fora do DAS conforme a regra vigente."
}

function buildTaxesFromDistribution(
  distribution: Record<string, number>,
  effectiveRate: number,
  revenue: number,
  note?: string
) {
  return TAX_ORDER
    .filter((tax) => distribution[tax] != null)
    .map((tax) => {
      const repartitionPercent = distribution[tax] ?? 0
      const effectiveTaxRate = roundTo((effectiveRate * repartitionPercent) / 100, 6)
      const amount = roundTo((revenue * effectiveTaxRate) / 100, 2)

      return {
        tax,
        repartitionPercent,
        effectiveRate: effectiveTaxRate,
        amount,
        shareOfDas: 0,
        note,
      } satisfies TaxBreakdownItem
    })
}

function buildTaxesWithFixedIss(
  appliedAnnex: "III" | "IV" | "V",
  effectiveRate: number,
  revenue: number,
  municipalIssRate: number,
  note?: string
) {
  const fixedRules = SIMPLE_NATIONAL_RULES[appliedAnnex].cappedIss
  if (!fixedRules || effectiveRate <= 0) {
    return buildTaxesFromDistribution(
      SIMPLE_NATIONAL_RULES[appliedAnnex].distribution[1],
      effectiveRate,
      revenue,
      note
    )
  }

  const clampedIssRate = roundTo(Math.min(Math.max(municipalIssRate, 2), 5), 6)
  const remainingEffectiveRate = Math.max(roundTo(effectiveRate - clampedIssRate, 6), 0)
  const items: TaxBreakdownItem[] = []

  for (const tax of TAX_ORDER) {
    if (tax === "ISS") {
      const amount = roundTo((revenue * clampedIssRate) / 100, 2)
      items.push({
        tax,
        repartitionPercent: effectiveRate > 0 ? roundTo((clampedIssRate / effectiveRate) * 100, 2) : 0,
        effectiveRate: clampedIssRate,
        amount,
        shareOfDas: 0,
        note,
      })
      continue
    }

    const formulaShare = fixedRules.formulas[tax]
    if (formulaShare == null) continue
    const effectiveTaxRate = roundTo((remainingEffectiveRate * formulaShare) / 100, 6)
    const amount = roundTo((revenue * effectiveTaxRate) / 100, 2)

    items.push({
      tax,
      repartitionPercent: effectiveRate > 0 ? roundTo((effectiveTaxRate / effectiveRate) * 100, 2) : 0,
      effectiveRate: effectiveTaxRate,
      amount,
      shareOfDas: 0,
      note,
    })
  }

  return items
}

function applyDasShares(items: TaxBreakdownItem[]) {
  const totalDas = roundTo(items.reduce((acc, item) => acc + item.amount, 0), 2)
  return items.map((item) => ({
    ...item,
    shareOfDas: totalDas > 0 ? roundTo((item.amount / totalDas) * 100, 2) : 0,
  }))
}

function buildSpecialAnnexIiIpiIssDistribution(bracket: AnnexBracket) {
  const annexIi = SIMPLE_NATIONAL_RULES.II.distribution[bracket.bracket]
  const annexIii = SIMPLE_NATIONAL_RULES.III.distribution[bracket.bracket]
  const distribution = { ...annexIi }

  delete distribution.ICMS
  distribution.ISS = annexIii.ISS ?? 0

  return distribution
}

function buildSegmentBreakdown(
  appliedAnnex: SimpleNationalAnnexCode,
  bracket: AnnexBracket,
  effectiveRate: number,
  segment: SimpleNationalRevenueSegmentInput,
  municipalIssRate: number | null
) {
  if ((appliedAnnex === "III" || appliedAnnex === "IV" || appliedAnnex === "V") && municipalIssRate != null) {
    return applyDasShares(
      buildTaxesWithFixedIss(
        appliedAnnex,
        effectiveRate,
        segment.amount,
        municipalIssRate,
        `ISS fixado em ${formatPercentBRL(municipalIssRate, 2)} conforme parâmetro municipal informado.`
      )
    )
  }

  if (segment.kind === "annex_ii_ipi_iss") {
    if (appliedAnnex !== "II") {
      return applyDasShares(
        buildTaxesFromDistribution(
          SIMPLE_NATIONAL_RULES[appliedAnnex].distribution[bracket.bracket],
          effectiveRate,
          segment.amount,
          "Segmento marcado como IPI + ISS fora do Anexo II; tratado pela partilha padrão do anexo aplicado."
        )
      )
    }

    return applyDasShares(
      buildTaxesFromDistribution(
        buildSpecialAnnexIiIpiIssDistribution(bracket),
        effectiveRate,
        segment.amount,
        "Receita com incidência simultânea de IPI e ISS no Anexo II; ICMS substituído pelo ISS conforme orientação oficial."
      )
    )
  }

  return applyDasShares(
    buildTaxesFromDistribution(
      SIMPLE_NATIONAL_RULES[appliedAnnex].distribution[bracket.bracket],
      effectiveRate,
      segment.amount
    )
  )
}

function groupBreakdown(items: TaxBreakdownItem[]) {
  const grouped = new Map<TaxBreakdownItem["tax"], TaxBreakdownItem>()

  for (const item of items) {
    const existing = grouped.get(item.tax)
    if (!existing) {
      grouped.set(item.tax, { ...item })
      continue
    }

    existing.amount = roundTo(existing.amount + item.amount, 2)
    existing.effectiveRate = roundTo(existing.effectiveRate + item.effectiveRate, 6)
    existing.repartitionPercent = roundTo(existing.repartitionPercent + item.repartitionPercent, 2)
    existing.note = existing.note ?? item.note
  }

  return applyDasShares([...grouped.values()])
}

function buildSegmentResults(
  input: SimpleNationalDraftInput,
  appliedAnnex: SimpleNationalAnnexCode,
  bracket: AnnexBracket,
  effectiveRate: number,
  rbt12ForBracket: number,
  fs12: number
) {
  const monthlyAllocations = input.currentPeriodAllocations.filter((item) => item.amount > 0)
  const operationalSegments = monthlyAllocations.length > 0
    ? monthlyAllocations.map((item) => {
      const annexResolution = resolveAppliedAnnexForBase(item.annex, input.subjectToFactorR, rbt12ForBracket, fs12)
      const segmentBracket = resolveBracket(annexResolution.appliedAnnex, rbt12ForBracket)
      const nominalRate = segmentBracket.nominalRate
      const deduction = segmentBracket.deduction
      const effectiveRateByAnnex = rbt12ForBracket > 0
        ? roundTo((((rbt12ForBracket * (nominalRate / 100)) - deduction) / rbt12ForBracket) * 100, 6)
        : nominalRate

      return {
        id: item.id,
        label: `Apuração do mês - Anexo ${annexResolution.appliedAnnex}`,
        annex: annexResolution.appliedAnnex,
        amount: item.amount,
        nominalRate,
        deduction,
        effectiveRateByAnnex,
      }
    })
    : [{
      id: "default",
      label: `Apuração do mês - Anexo ${appliedAnnex}`,
      annex: appliedAnnex,
      amount: input.currentPeriodRevenue,
      nominalRate: bracket.nominalRate,
      deduction: bracket.deduction,
      effectiveRateByAnnex: effectiveRate,
    }]

  const results: RevenueSegmentCalculationResult[] = operationalSegments.map((segment) => {
    const breakdown = buildSegmentBreakdown(segment.annex, resolveBracket(segment.annex, rbt12ForBracket), segment.effectiveRateByAnnex, {
      id: segment.id,
      label: segment.label,
      annex: segment.annex,
      kind: "standard",
      market: "internal",
      amount: segment.amount,
    }, input.municipalIssRate)
    const estimatedDas = roundTo(breakdown.reduce((acc, item) => acc + item.amount, 0), 2)

    return {
      id: segment.id,
      label: segment.label,
      annex: segment.annex,
      kind: "standard",
      market: "internal",
      amount: segment.amount,
      nominalRate: segment.nominalRate,
      deduction: segment.deduction,
      effectiveRate: segment.effectiveRateByAnnex,
      estimatedDas,
      breakdown,
      note: `Receita segregada e calculada pela tabela do Anexo ${segment.annex}.`,
    }
  })

  return {
    segmentResults: results,
    breakdown: groupBreakdown(results.flatMap((item) => item.breakdown)),
    currentPeriodRevenue: roundTo(results.reduce((acc, item) => acc + item.amount, 0), 2),
  }
}

function buildMemory(params: {
  apurationPeriod: string
  months: PeriodMonth[]
  rbt12: number
  rbt12ForBracket: number
  payrollBase12: number
  employerCpp12: number
  fgts12: number
  fs12: number
  currentPeriodRevenue: number
  municipalIssRate: number | null
  factorR: number | null
  appliedAnnex: SimpleNationalAnnexCode
  annexReason: string
  bracket: AnnexBracket
  effectiveRate: number
  estimatedDas: number
  isStartupPeriod: boolean
  startupMonthOrdinal: number | null
  breakdown: TaxBreakdownItem[]
  payrollCompositionTotals: ReturnType<typeof resolvePayrollCompositionTotals>
  payrollMethod: "automatic" | "composition"
  segmentResults: RevenueSegmentCalculationResult[]
}) {
  const memory: CalculationMemoryItem[] = [
    { label: "Bloco: Faixa e enquadramento", value: "-" },
    {
      label: "Período de apuração",
      value: formatPeriodLabel(params.apurationPeriod),
      detail: `${params.months[0]?.label ?? "-"} até ${params.months[params.months.length - 1]?.label ?? "-"}`,
    },
    { label: "RBT12", value: formatCurrencyBRL(params.rbt12) },
    {
      label: "RBT12 para faixa",
      value: formatCurrencyBRL(params.rbt12ForBracket),
      detail: params.isStartupPeriod
        ? `Empresa em início de atividade no ${params.startupMonthOrdinal}º mês de apuração.`
        : "Mesma base do RBT12 histórico.",
    },
    { label: "Folha base 12 meses", value: formatCurrencyBRL(params.payrollBase12) },
    { label: "CPP patronal 12 meses", value: formatCurrencyBRL(params.employerCpp12), detail: params.payrollMethod === "automatic" ? "Calculada automaticamente a 20% sobre a folha informada." : "Valor informado na composição avançada." },
    { label: "FGTS 12 meses", value: formatCurrencyBRL(params.fgts12), detail: params.payrollMethod === "automatic" ? "Calculado automaticamente a 8% sobre a folha informada." : "Valor informado na composição avançada." },
    { label: "FS12", value: formatCurrencyBRL(params.fs12) },
    { label: "ISS municipal", value: params.municipalIssRate == null ? "Partilha padrão" : formatPercentBRL(params.municipalIssRate, 2) },
    params.payrollCompositionTotals
      ? {
        label: "FS12 incluída",
        value: formatCurrencyBRL(params.payrollCompositionTotals.includedTotal),
        detail: `Excluídos: ${formatCurrencyBRL(params.payrollCompositionTotals.excludedTotal)}`,
      }
      : { label: "FS12 incluída", value: formatCurrencyBRL(params.fs12), detail: "Composição detalhada não preenchida." },
    { label: "RPA do mês", value: formatCurrencyBRL(params.currentPeriodRevenue) },
    {
      label: "Fator R",
      value: params.factorR == null ? "Não aplicável" : formatPercentBRL(params.factorR * 100, 2),
      detail: params.annexReason,
    },
    { label: "Anexo aplicado", value: `Anexo ${params.appliedAnnex}` },
    {
      label: "Faixa aplicada",
      value: `${params.bracket.bracket}ª faixa`,
      detail: `${formatCurrencyBRL(params.bracket.rangeStart)} a ${formatCurrencyBRL(params.bracket.rangeEnd)}`,
    },
    { label: "Alíquota nominal", value: formatPercentBRL(params.bracket.nominalRate, 2) },
    { label: "Parcela a deduzir", value: formatCurrencyBRL(params.bracket.deduction) },
    { label: "Alíquota efetiva", value: formatPercentBRL(params.effectiveRate, 4) },
    { label: "DAS estimado", value: formatCurrencyBRL(params.estimatedDas) },
    { label: "Bloco: Segregação da apuração", value: "-" },
  ]

  for (const segment of params.segmentResults) {
    memory.push({
      label: `Segmento: ${segment.label}`,
      value: formatCurrencyBRL(segment.amount),
      detail: `${segment.market === "external" ? "Mercado externo" : "Mercado interno"} | ${formatPercentBRL(segment.effectiveRate, 4)} | DAS ${formatCurrencyBRL(segment.estimatedDas)}`,
    })
  }

  for (const item of params.breakdown) {
    memory.push({
      label: item.tax,
      value: formatCurrencyBRL(item.amount),
      detail: `${formatPercentBRL(item.effectiveRate, 4)} da receita do mês | ${formatPercentBRL(item.shareOfDas, 2)} do DAS`,
    })
  }

  return memory
}

export function calculateSimpleNational(input: SimpleNationalDraftInput, months: PeriodMonth[]): SimpleNationalCalculationResult {
  const rbt12 = sumEntries(input.revenueEntries)
  const startupMetrics = resolveRbt12ForBracket(input, months, rbt12)
  const payrollTotals = resolvePayrollTotals(input.payrollEntries, input.payrollComposition)
  const annexResolution = resolveAppliedAnnex(input, startupMetrics.rbt12ForBracket, payrollTotals.fs12)
  const bracket = resolveBracket(annexResolution.appliedAnnex, startupMetrics.rbt12ForBracket)
  const nominalRate = bracket.nominalRate
  const deduction = bracket.deduction
  const effectiveRate = startupMetrics.rbt12ForBracket > 0
    ? roundTo((((startupMetrics.rbt12ForBracket * (nominalRate / 100)) - deduction) / startupMetrics.rbt12ForBracket) * 100, 6)
    : nominalRate

  const segmentCalculation = buildSegmentResults(
    input,
    annexResolution.appliedAnnex,
    bracket,
    effectiveRate,
    startupMetrics.rbt12ForBracket,
    payrollTotals.fs12
  )
  const estimatedDas = roundTo(segmentCalculation.breakdown.reduce((acc, item) => acc + item.amount, 0), 2)
  const warnings: string[] = []

  if (bracket.bracket === 6) {
    warnings.push(buildSixthBracketWarning(annexResolution.appliedAnnex))
  }

  if (!input.payrollComposition) {
    warnings.push("FS12 foi apurada automaticamente com CPP patronal de 20% e FGTS de 8% sobre a folha informada. Use composição avançada apenas se houver exceções.")
  }

  if (startupMetrics.isStartupPeriod) {
    warnings.push("Empresa em início de atividade: a faixa foi definida pela RBT12 proporcionalizada.")
  }

  if (
    input.currentPeriodSegments.some((segment) => segment.amount > 0) &&
    roundTo(input.currentPeriodRevenue, 2) !== roundTo(segmentCalculation.currentPeriodRevenue, 2)
  ) {
    warnings.push("A RPA do período foi calculada pela soma das receitas segregadas, pois ela diverge do campo resumo informado.")
  }

  const memory = buildMemory({
    apurationPeriod: input.apurationPeriod,
    months,
    rbt12,
    rbt12ForBracket: startupMetrics.rbt12ForBracket,
    payrollBase12: payrollTotals.payrollBase12,
    employerCpp12: payrollTotals.employerCpp12,
    fgts12: payrollTotals.fgts12,
    fs12: payrollTotals.fs12,
    currentPeriodRevenue: segmentCalculation.currentPeriodRevenue,
    municipalIssRate: input.municipalIssRate,
    factorR: annexResolution.factorR,
    appliedAnnex: annexResolution.appliedAnnex,
    annexReason: annexResolution.annexReason,
    bracket,
    effectiveRate,
    estimatedDas,
    isStartupPeriod: startupMetrics.isStartupPeriod,
    startupMonthOrdinal: startupMetrics.startupMonthOrdinal,
    breakdown: segmentCalculation.breakdown,
    payrollCompositionTotals: payrollTotals.payrollCompositionTotals,
    payrollMethod: payrollTotals.payrollMethod,
    segmentResults: segmentCalculation.segmentResults,
  })

  return {
    ruleVersionCode: RULE_VERSIONS[0].versionCode,
    apurationPeriod: input.apurationPeriod,
    months,
    rbt12,
    rbt12ForBracket: startupMetrics.rbt12ForBracket,
    isStartupPeriod: startupMetrics.isStartupPeriod,
    startupMonthOrdinal: startupMetrics.startupMonthOrdinal,
    payrollBase12: payrollTotals.payrollBase12,
    employerCpp12: payrollTotals.employerCpp12,
    fgts12: payrollTotals.fgts12,
    fs12: payrollTotals.fs12,
    currentPeriodRevenue: segmentCalculation.currentPeriodRevenue,
    municipalIssRate: input.municipalIssRate,
    factorR: annexResolution.factorR,
    factorRThreshold: FACTOR_R_THRESHOLD,
    factorRQualified: annexResolution.factorRQualified,
    baseAnnex: input.baseAnnex,
    appliedAnnex: annexResolution.appliedAnnex,
    annexReason: annexResolution.annexReason,
    bracket,
    nominalRate,
    deduction,
    effectiveRate,
    estimatedDas,
    segmentResults: segmentCalculation.segmentResults,
    breakdown: segmentCalculation.breakdown,
    memory,
    warnings,
  }
}
