import { describe, expect, it } from "vitest"
import { calculateSimpleNational } from "./simplesNationalEngine"
import { generateRetroactiveMonths } from "@/services/tributaryIntelligenceService"

function buildEntries(months: ReturnType<typeof generateRetroactiveMonths>, amount: number) {
  return months.map((month) => ({ referenceMonth: month.referenceMonth, amount }))
}

function buildInput(overrides: Record<string, unknown> = {}) {
  return {
    companyId: "company-1",
    apurationPeriod: "2026-01",
    companyStartDate: null,
    currentPeriodRevenue: 30000,
    municipalIssRate: null,
    subjectToFactorR: false,
    baseAnnex: "I",
    activityLabel: "Atividade padrão",
    revenueEntries: [] as Array<{ referenceMonth: string; amount: number }>,
    historicalRevenueAllocations: [],
    currentPeriodAllocations: [],
    payrollEntries: [] as Array<{ referenceMonth: string; amount: number }>,
    currentPeriodSegments: [],
    payrollComposition: null,
    ...overrides,
  }
}

describe("calculateSimpleNational", () => {
  it("gera os 12 meses anteriores ao PA sem incluir o mês de apuração", () => {
    const months = generateRetroactiveMonths("2026-01")

    expect(months).toHaveLength(12)
    expect(months[0]?.referenceMonth).toBe("2025-01")
    expect(months[11]?.referenceMonth).toBe("2025-12")
  })

  it("aplica Anexo I para comércio e informa a faixa correta", () => {
    const months = generateRetroactiveMonths("2026-01")
    const result = calculateSimpleNational(buildInput({
      revenueEntries: buildEntries(months, 20000),
      payrollEntries: buildEntries(months, 3000),
    }), months)

    expect(result.appliedAnnex).toBe("I")
    expect(result.bracket.bracket).toBe(2)
    expect(result.nominalRate).toBe(7.3)
    expect(result.deduction).toBe(5940)
    expect(result.effectiveRate).toBe(4.825)
    expect(result.estimatedDas).toBe(1447.49)
    expect(result.breakdown.some((item) => item.tax === "ICMS")).toBe(true)
  })

  it("aplica Anexo II com a tabela correta na apuração segregada do mês", () => {
    const months = generateRetroactiveMonths("2026-01")
    const result = calculateSimpleNational(buildInput({
      companyId: "company-2",
      baseAnnex: "II",
      currentPeriodRevenue: 50000,
      activityLabel: "Indústria mista",
      revenueEntries: buildEntries(months, 50000),
      payrollEntries: buildEntries(months, 5000),
      currentPeriodAllocations: [
        { id: "current-II", annex: "II", amount: 50000 },
      ],
    }), months)

    expect(result.appliedAnnex).toBe("II")
    expect(result.bracket.bracket).toBe(3)
    expect(result.breakdown.some((item) => item.tax === "IPI")).toBe(true)
    expect(result.breakdown.some((item) => item.tax === "ICMS")).toBe(true)
    expect(result.segmentResults[0].annex).toBe("II")
  })

  it("fixa o ISS municipal entre 2% e 5% na partilha de serviços", () => {
    const months = generateRetroactiveMonths("2026-01")
    const result = calculateSimpleNational(buildInput({
      companyId: "company-2b",
      baseAnnex: "III",
      municipalIssRate: 3,
      currentPeriodRevenue: 50000,
      revenueEntries: buildEntries(months, 20000),
      payrollEntries: buildEntries(months, 5000),
    }), months)

    const iss = result.breakdown.find((item) => item.tax === "ISS")
    expect(iss).toBeTruthy()
    expect(iss?.effectiveRate).toBeCloseTo(3, 6)
  })

  it("proporcionaliza o RBT12 em início de atividade", () => {
    const months = generateRetroactiveMonths("2026-01")
    const result = calculateSimpleNational(buildInput({
      companyId: "company-3",
      companyStartDate: "2025-11-01",
      currentPeriodRevenue: 15000,
      revenueEntries: [
        { referenceMonth: "2025-11", amount: 10000 },
        { referenceMonth: "2025-12", amount: 12000 },
        ...months.filter((item) => !["2025-11", "2025-12"].includes(item.referenceMonth)).map((item) => ({ referenceMonth: item.referenceMonth, amount: 0 })),
      ],
      payrollEntries: buildEntries(months, 0),
    }), months)

    expect(result.isStartupPeriod).toBe(true)
    expect(result.startupMonthOrdinal).toBe(3)
    expect(result.rbt12ForBracket).toBe(132000)
  })

  it("usa composição estruturada da FS12 no fator R", () => {
    const months = generateRetroactiveMonths("2026-01")
    const result = calculateSimpleNational(buildInput({
      companyId: "company-4",
      baseAnnex: "V",
      currentPeriodRevenue: 50000,
      subjectToFactorR: true,
      activityLabel: "Serviços com fator R",
      revenueEntries: buildEntries(months, 20000),
      payrollEntries: buildEntries(months, 1000),
      payrollComposition: {
        employeesAmount: 60000,
        proLaboreAmount: 12000,
        individualContractorsAmount: 0,
        thirteenthSalaryAmount: 4000,
        employerCppAmount: 6000,
        fgtsAmount: 5000,
        excludedProfitDistributionAmount: 10000,
        excludedRentAmount: 3000,
        excludedInternsAmount: 2000,
        excludedMeiAmount: 1000,
      },
    }), months)

    expect(result.fs12).toBe(87000)
    expect(result.factorRQualified).toBe(true)
    expect(result.appliedAnnex).toBe("III")
  })

  it("calcula automaticamente CPP patronal e FGTS sobre a folha informada", () => {
    const months = generateRetroactiveMonths("2026-01")
    const result = calculateSimpleNational(buildInput({
      companyId: "company-4a",
      revenueEntries: buildEntries(months, 10000),
      payrollEntries: buildEntries(months, 1000),
    }), months)

    expect(result.payrollBase12).toBe(12000)
    expect(result.employerCpp12).toBe(2400)
    expect(result.fgts12).toBe(960)
    expect(result.fs12).toBe(15360)
  })

  it("permite fator R partindo do Anexo III e desloca para o Anexo V quando ficar abaixo de 28%", () => {
    const months = generateRetroactiveMonths("2026-01")
    const result = calculateSimpleNational(buildInput({
      companyId: "company-4b",
      baseAnnex: "III",
      currentPeriodRevenue: 50000,
      subjectToFactorR: true,
      activityLabel: "Serviços sujeitos ao fator R",
      revenueEntries: buildEntries(months, 20000),
      payrollEntries: buildEntries(months, 3000),
    }), months)

    expect(result.factorRQualified).toBe(false)
    expect(result.appliedAnnex).toBe("V")
    expect(result.annexReason).toContain("Anexo V")
  })

  it("mantém Anexo V quando fator R fica abaixo de 28%", () => {
    const months = generateRetroactiveMonths("2026-01")
    const result = calculateSimpleNational(buildInput({
      companyId: "company-5",
      baseAnnex: "V",
      currentPeriodRevenue: 50000,
      subjectToFactorR: true,
      activityLabel: "Serviços com fator R",
      revenueEntries: buildEntries(months, 20000),
      payrollEntries: buildEntries(months, 3000),
    }), months)

    expect(result.appliedAnnex).toBe("V")
    expect(result.factorRQualified).toBe(false)
    expect(result.factorR).toBeCloseTo(0.192, 6)
    expect(result.bracket.bracket).toBe(2)
  })

  it("gera aviso de ICMS fora do DAS na faixa 6 do Anexo I", () => {
    const months = generateRetroactiveMonths("2026-01")
    const result = calculateSimpleNational(buildInput({
      companyId: "company-6",
      currentPeriodRevenue: 100000,
      revenueEntries: buildEntries(months, 310000),
      payrollEntries: buildEntries(months, 70000),
    }), months)

    expect(result.bracket.bracket).toBe(6)
    expect(result.warnings.some((warning) => warning.includes("ICMS fora do DAS"))).toBe(true)
  })

  it("mantém a RPA do mês separada do RBT12 histórico", () => {
    const months = generateRetroactiveMonths("2026-01")
    const result = calculateSimpleNational(buildInput({
      companyId: "company-7",
      currentPeriodRevenue: 90000,
      revenueEntries: buildEntries(months, 10000),
      payrollEntries: buildEntries(months, 2000),
      currentPeriodSegments: [],
    }), months)

    expect(result.rbt12).toBe(120000)
    expect(result.currentPeriodRevenue).toBe(90000)
    expect(result.estimatedDas).toBeGreaterThan(0)
  })
})
