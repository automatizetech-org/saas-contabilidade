import type { AnnexBracket, RuleVersionScope, SimpleNationalAnnexCode } from "./types"

export interface RuleVersionDefinition {
  scope: RuleVersionScope
  versionCode: string
  effectiveFrom: string
  effectiveTo: string | null
  title: string
  sourceReference: string
  sourceUrl: string
  summary: string
}

export interface AnnexRuleSet {
  annex: SimpleNationalAnnexCode
  brackets: AnnexBracket[]
  distribution: Record<number, Record<string, number>>
  cappedIss?: {
    bracket: number
    thresholdEffectiveRate: number
    issFixedRate: number
    formulas: Record<string, number>
  }
}

export const RULE_VERSIONS: RuleVersionDefinition[] = [
  {
    scope: "simples_nacional_das",
    versionCode: "SN-LC123-2018-vigente",
    effectiveFrom: "2018-01-01",
    effectiveTo: null,
    title: "Simples Nacional DAS vigente",
    sourceReference: "LC 123/2006, art. 18 e Anexos I a V",
    sourceUrl: "https://www.planalto.gov.br/ccivil_03/leis/lcp/lcp123.htm",
    summary: "Faixas, aliquotas nominais, parcelas a deduzir, fator R e partilha vigente desde 01/01/2018.",
  },
  {
    scope: "reforma_consumo_2026",
    versionCode: "RT-2026-IBS-CBS-testes",
    effectiveFrom: "2026-01-01",
    effectiveTo: "2026-12-31",
    title: "Transicao IBS/CBS 2026",
    sourceReference: "LC 214/2025 e Ato Conjunto RFB/CGIBS 1/2025",
    sourceUrl: "https://www.gov.br/receitafederal/pt-br/assuntos/noticias/2025/dezembro/receita-federal-e-comite-gestor-do-ibs-definem-regras-relativas-a-obrigacoes-acessorias-da-reforma-tributaria-para-inicio-de-2026",
    summary: "2026 e ano de testes com obrigacoes acessorias e dispensa de recolhimento quando cumpridas as exigencias aplicaveis.",
  },
]

export const SIMPLE_NATIONAL_RULES: Record<SimpleNationalAnnexCode, AnnexRuleSet> = {
  I: {
    annex: "I",
    brackets: [
      { bracket: 1, rangeStart: 0, rangeEnd: 180000, nominalRate: 4, deduction: 0 },
      { bracket: 2, rangeStart: 180000.01, rangeEnd: 360000, nominalRate: 7.3, deduction: 5940 },
      { bracket: 3, rangeStart: 360000.01, rangeEnd: 720000, nominalRate: 9.5, deduction: 13860 },
      { bracket: 4, rangeStart: 720000.01, rangeEnd: 1800000, nominalRate: 10.7, deduction: 22500 },
      { bracket: 5, rangeStart: 1800000.01, rangeEnd: 3600000, nominalRate: 14.3, deduction: 87300 },
      { bracket: 6, rangeStart: 3600000.01, rangeEnd: 4800000, nominalRate: 19, deduction: 378000 },
    ],
    distribution: {
      1: { "IRPJ": 5.5, "CSLL": 3.5, "COFINS": 12.74, "PIS/PASEP": 2.76, "CPP": 41.5, "ICMS": 34 },
      2: { "IRPJ": 5.5, "CSLL": 3.5, "COFINS": 12.74, "PIS/PASEP": 2.76, "CPP": 41.5, "ICMS": 34 },
      3: { "IRPJ": 5.5, "CSLL": 3.5, "COFINS": 12.74, "PIS/PASEP": 2.76, "CPP": 42, "ICMS": 33.5 },
      4: { "IRPJ": 5.5, "CSLL": 3.5, "COFINS": 12.74, "PIS/PASEP": 2.76, "CPP": 42, "ICMS": 33.5 },
      5: { "IRPJ": 5.5, "CSLL": 3.5, "COFINS": 12.74, "PIS/PASEP": 2.76, "CPP": 42, "ICMS": 33.5 },
      6: { "IRPJ": 13.5, "CSLL": 10, "COFINS": 28.27, "PIS/PASEP": 6.13, "CPP": 42.1, "ICMS": 0 },
    },
  },
  II: {
    annex: "II",
    brackets: [
      { bracket: 1, rangeStart: 0, rangeEnd: 180000, nominalRate: 4.5, deduction: 0 },
      { bracket: 2, rangeStart: 180000.01, rangeEnd: 360000, nominalRate: 7.8, deduction: 5940 },
      { bracket: 3, rangeStart: 360000.01, rangeEnd: 720000, nominalRate: 10, deduction: 13860 },
      { bracket: 4, rangeStart: 720000.01, rangeEnd: 1800000, nominalRate: 11.2, deduction: 22500 },
      { bracket: 5, rangeStart: 1800000.01, rangeEnd: 3600000, nominalRate: 14.7, deduction: 85500 },
      { bracket: 6, rangeStart: 3600000.01, rangeEnd: 4800000, nominalRate: 30, deduction: 720000 },
    ],
    distribution: {
      1: { "IRPJ": 5.5, "CSLL": 3.5, "COFINS": 11.51, "PIS/PASEP": 2.49, "CPP": 37.5, "IPI": 7.5, "ICMS": 32 },
      2: { "IRPJ": 5.5, "CSLL": 3.5, "COFINS": 11.51, "PIS/PASEP": 2.49, "CPP": 37.5, "IPI": 7.5, "ICMS": 32 },
      3: { "IRPJ": 5.5, "CSLL": 3.5, "COFINS": 11.51, "PIS/PASEP": 2.49, "CPP": 37.5, "IPI": 7.5, "ICMS": 32 },
      4: { "IRPJ": 5.5, "CSLL": 3.5, "COFINS": 11.51, "PIS/PASEP": 2.49, "CPP": 37.5, "IPI": 7.5, "ICMS": 32 },
      5: { "IRPJ": 5.5, "CSLL": 3.5, "COFINS": 11.51, "PIS/PASEP": 2.49, "CPP": 37.5, "IPI": 7.5, "ICMS": 32 },
      6: { "IRPJ": 8.5, "CSLL": 7.5, "COFINS": 20.96, "PIS/PASEP": 4.54, "CPP": 23.5, "IPI": 35, "ICMS": 0 },
    },
  },
  III: {
    annex: "III",
    brackets: [
      { bracket: 1, rangeStart: 0, rangeEnd: 180000, nominalRate: 6, deduction: 0 },
      { bracket: 2, rangeStart: 180000.01, rangeEnd: 360000, nominalRate: 11.2, deduction: 9360 },
      { bracket: 3, rangeStart: 360000.01, rangeEnd: 720000, nominalRate: 13.5, deduction: 17640 },
      { bracket: 4, rangeStart: 720000.01, rangeEnd: 1800000, nominalRate: 16, deduction: 35640 },
      { bracket: 5, rangeStart: 1800000.01, rangeEnd: 3600000, nominalRate: 21, deduction: 125640 },
      { bracket: 6, rangeStart: 3600000.01, rangeEnd: 4800000, nominalRate: 33, deduction: 648000 },
    ],
    distribution: {
      1: { "IRPJ": 4, "CSLL": 3.5, "COFINS": 12.82, "PIS/PASEP": 2.78, "CPP": 43.4, "ISS": 33.5 },
      2: { "IRPJ": 4, "CSLL": 3.5, "COFINS": 14.05, "PIS/PASEP": 3.05, "CPP": 43.4, "ISS": 32 },
      3: { "IRPJ": 4, "CSLL": 3.5, "COFINS": 13.64, "PIS/PASEP": 2.96, "CPP": 43.4, "ISS": 32.5 },
      4: { "IRPJ": 4, "CSLL": 3.5, "COFINS": 13.64, "PIS/PASEP": 2.96, "CPP": 43.4, "ISS": 32.5 },
      5: { "IRPJ": 4, "CSLL": 3.5, "COFINS": 12.82, "PIS/PASEP": 2.78, "CPP": 43.4, "ISS": 33.5 },
      6: { "IRPJ": 13.5, "CSLL": 10, "COFINS": 28.27, "PIS/PASEP": 6.13, "CPP": 42.1, "ISS": 0 },
    },
    cappedIss: {
      bracket: 5,
      thresholdEffectiveRate: 14.92537,
      issFixedRate: 5,
      formulas: { "IRPJ": 6.02, "CSLL": 5.26, "COFINS": 19.28, "PIS/PASEP": 4.18, "CPP": 65.26 },
    },
  },
  IV: {
    annex: "IV",
    brackets: [
      { bracket: 1, rangeStart: 0, rangeEnd: 180000, nominalRate: 4.5, deduction: 0 },
      { bracket: 2, rangeStart: 180000.01, rangeEnd: 360000, nominalRate: 9, deduction: 8100 },
      { bracket: 3, rangeStart: 360000.01, rangeEnd: 720000, nominalRate: 10.2, deduction: 12420 },
      { bracket: 4, rangeStart: 720000.01, rangeEnd: 1800000, nominalRate: 14, deduction: 39780 },
      { bracket: 5, rangeStart: 1800000.01, rangeEnd: 3600000, nominalRate: 22, deduction: 183780 },
      { bracket: 6, rangeStart: 3600000.01, rangeEnd: 4800000, nominalRate: 33, deduction: 828000 },
    ],
    distribution: {
      1: { "IRPJ": 18.8, "CSLL": 15.2, "COFINS": 17.67, "PIS/PASEP": 3.83, "ISS": 44.5 },
      2: { "IRPJ": 19.8, "CSLL": 15.2, "COFINS": 20.55, "PIS/PASEP": 4.45, "ISS": 40 },
      3: { "IRPJ": 20.8, "CSLL": 15.2, "COFINS": 19.73, "PIS/PASEP": 4.27, "ISS": 40 },
      4: { "IRPJ": 17.8, "CSLL": 19.2, "COFINS": 18.9, "PIS/PASEP": 4.1, "ISS": 40 },
      5: { "IRPJ": 18.8, "CSLL": 19.2, "COFINS": 18.08, "PIS/PASEP": 3.92, "ISS": 40 },
      6: { "IRPJ": 53.5, "CSLL": 21.5, "COFINS": 20.55, "PIS/PASEP": 4.45, "ISS": 0 },
    },
    cappedIss: {
      bracket: 5,
      thresholdEffectiveRate: 12.5,
      issFixedRate: 5,
      formulas: { "IRPJ": 31.33, "CSLL": 32, "COFINS": 30.13, "PIS/PASEP": 6.54 },
    },
  },
  V: {
    annex: "V",
    brackets: [
      { bracket: 1, rangeStart: 0, rangeEnd: 180000, nominalRate: 15.5, deduction: 0 },
      { bracket: 2, rangeStart: 180000.01, rangeEnd: 360000, nominalRate: 18, deduction: 4500 },
      { bracket: 3, rangeStart: 360000.01, rangeEnd: 720000, nominalRate: 19.5, deduction: 9900 },
      { bracket: 4, rangeStart: 720000.01, rangeEnd: 1800000, nominalRate: 20.5, deduction: 17100 },
      { bracket: 5, rangeStart: 1800000.01, rangeEnd: 3600000, nominalRate: 23, deduction: 62100 },
      { bracket: 6, rangeStart: 3600000.01, rangeEnd: 4800000, nominalRate: 30.5, deduction: 540000 },
    ],
    distribution: {
      1: { "IRPJ": 25, "CSLL": 15, "COFINS": 14.1, "PIS/PASEP": 3.05, "CPP": 28.85, "ISS": 14 },
      2: { "IRPJ": 23, "CSLL": 15, "COFINS": 14.1, "PIS/PASEP": 3.05, "CPP": 27.85, "ISS": 17 },
      3: { "IRPJ": 24, "CSLL": 15, "COFINS": 14.92, "PIS/PASEP": 3.23, "CPP": 23.85, "ISS": 19 },
      4: { "IRPJ": 21, "CSLL": 15, "COFINS": 15.74, "PIS/PASEP": 3.41, "CPP": 23.85, "ISS": 21 },
      5: { "IRPJ": 23, "CSLL": 12.5, "COFINS": 14.1, "PIS/PASEP": 3.05, "CPP": 23.85, "ISS": 23.5 },
      6: { "IRPJ": 35, "CSLL": 15, "COFINS": 16.03, "PIS/PASEP": 3.47, "CPP": 30.5, "ISS": 0 },
    },
    cappedIss: {
      bracket: 5,
      thresholdEffectiveRate: 12.5,
      issFixedRate: 5,
      formulas: { "IRPJ": 31.33, "CSLL": 32, "COFINS": 30.13, "PIS/PASEP": 6.54, "CPP": 0 },
    },
  },
}
