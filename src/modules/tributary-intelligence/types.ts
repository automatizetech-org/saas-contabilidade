export type TaxIntelligenceTopic =
  | "visao-geral"
  | "simples-nacional"
  | "lucro-real"
  | "lucro-presumido"

export type SimpleNationalAnnexCode = "I" | "II" | "III" | "IV" | "V"

export type RuleVersionScope =
  | "simples_nacional_das"
  | "reforma_consumo_2026"

export interface PeriodMonth {
  referenceMonth: string
  label: string
}

export interface SimpleNationalEntryInput {
  referenceMonth: string
  amount: number
}

export interface SimpleNationalHistoricalRevenueAllocationInput {
  id: string
  referenceMonth: string
  annex: SimpleNationalAnnexCode
  amount: number
}

export interface SimpleNationalCurrentRevenueAllocationInput {
  id: string
  annex: SimpleNationalAnnexCode
  amount: number
}

export type SimpleNationalRevenueSegmentKind =
  | "standard"
  | "annex_ii_ipi_iss"

export type SimpleNationalRevenueMarket = "internal" | "external"

export interface SimpleNationalRevenueSegmentInput {
  id: string
  label: string
  annex?: SimpleNationalAnnexCode
  kind: SimpleNationalRevenueSegmentKind
  market: SimpleNationalRevenueMarket
  amount: number
}

export interface SimpleNationalPayrollCompositionInput {
  employeesAmount: number
  proLaboreAmount: number
  individualContractorsAmount: number
  thirteenthSalaryAmount: number
  employerCppAmount: number
  fgtsAmount: number
  excludedProfitDistributionAmount: number
  excludedRentAmount: number
  excludedInternsAmount: number
  excludedMeiAmount: number
}

export interface SimpleNationalDraftInput {
  companyId: string
  apurationPeriod: string
  companyStartDate: string | null
  currentPeriodRevenue: number
  municipalIssRate: number | null
  subjectToFactorR: boolean
  baseAnnex: SimpleNationalAnnexCode
  activityLabel: string
  revenueEntries: SimpleNationalEntryInput[]
  historicalRevenueAllocations: SimpleNationalHistoricalRevenueAllocationInput[]
  currentPeriodAllocations: SimpleNationalCurrentRevenueAllocationInput[]
  payrollEntries: SimpleNationalEntryInput[]
  currentPeriodSegments: SimpleNationalRevenueSegmentInput[]
  payrollComposition: SimpleNationalPayrollCompositionInput | null
}

export interface AnnexBracket {
  bracket: number
  rangeStart: number
  rangeEnd: number
  nominalRate: number
  deduction: number
}

export interface TaxBreakdownItem {
  tax: "IRPJ" | "CSLL" | "COFINS" | "PIS/PASEP" | "CPP" | "ISS" | "ICMS" | "IPI"
  repartitionPercent: number
  effectiveRate: number
  amount: number
  shareOfDas: number
  note?: string
}

export interface RevenueSegmentCalculationResult {
  id: string
  label: string
  annex: SimpleNationalAnnexCode
  kind: SimpleNationalRevenueSegmentKind
  market: SimpleNationalRevenueMarket
  amount: number
  nominalRate: number
  deduction: number
  effectiveRate: number
  estimatedDas: number
  breakdown: TaxBreakdownItem[]
  note?: string
}

export interface CalculationMemoryItem {
  label: string
  value: string
  detail?: string
}

export interface SimpleNationalCalculationResult {
  ruleVersionCode: string
  apurationPeriod: string
  months: PeriodMonth[]
  rbt12: number
  rbt12ForBracket: number
  isStartupPeriod: boolean
  startupMonthOrdinal: number | null
  payrollBase12: number
  employerCpp12: number
  fgts12: number
  fs12: number
  currentPeriodRevenue: number
  municipalIssRate: number | null
  factorR: number | null
  factorRThreshold: number
  factorRQualified: boolean | null
  baseAnnex: SimpleNationalAnnexCode
  appliedAnnex: SimpleNationalAnnexCode
  annexReason: string
  bracket: AnnexBracket
  nominalRate: number
  deduction: number
  effectiveRate: number
  estimatedDas: number
  segmentResults: RevenueSegmentCalculationResult[]
  breakdown: TaxBreakdownItem[]
  memory: CalculationMemoryItem[]
  warnings: string[]
}

export interface SimpleNationalDraftPayload extends SimpleNationalDraftInput {
  months: PeriodMonth[]
  lastCalculation: SimpleNationalCalculationResult | null
  updatedAt: string | null
}

export interface TaxIntelligenceOverview {
  cards: {
    calculosSalvos: number
    mediaDas: number
    mediaAliquotaEfetiva: number
    empresasAtivas: number
  }
  byTopic: Array<{ name: string; value: number }>
  byMonth: Array<{ name: string; value: number }>
  annexDistribution: Array<{ name: string; value: number }>
  recentCalculations: Array<{
    id: string
    companyName: string
    apurationPeriod: string
    appliedAnnex: string
    estimatedDas: number
    updatedAt: string
  }>
}
