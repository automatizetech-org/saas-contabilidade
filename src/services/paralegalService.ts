import { getCompaniesForUser } from "./companiesService"
import type { Company } from "./profilesService"

export type CertificateStatus = "ativo" | "vence_em_breve" | "vencido" | "sem_certificado"

export type ParalegalCertificateItem = Company & {
  has_certificate: boolean
  days_to_expiry: number | null
  certificate_status: CertificateStatus
}

export type ParalegalCertificateSummary = {
  total: number
  ativos: number
  venceEmBreve: number
  vencidos: number
  semCertificado: number
}

const DAY_IN_MS = 24 * 60 * 60 * 1000
export const CERTIFICATE_EXPIRY_WARNING_DAYS = 30

function getDaysToExpiry(certValidUntil: string | null): number | null {
  if (!certValidUntil) return null
  const target = new Date(`${certValidUntil}T00:00:00`)
  const today = new Date()
  const base = new Date(today.getFullYear(), today.getMonth(), today.getDate())
  return Math.ceil((target.getTime() - base.getTime()) / DAY_IN_MS)
}

function getCertificateStatus(company: Company): Pick<ParalegalCertificateItem, "has_certificate" | "days_to_expiry" | "certificate_status"> {
  const hasCertificate = Boolean(company.cert_blob_b64 && company.auth_mode === "certificate")
  const daysToExpiry = hasCertificate ? getDaysToExpiry(company.cert_valid_until) : null

  if (!hasCertificate) {
    return {
      has_certificate: false,
      days_to_expiry: null,
      certificate_status: "sem_certificado",
    }
  }

  if (daysToExpiry == null || daysToExpiry < 0) {
    return {
      has_certificate: true,
      days_to_expiry: daysToExpiry,
      certificate_status: "vencido",
    }
  }

  if (daysToExpiry <= CERTIFICATE_EXPIRY_WARNING_DAYS) {
    return {
      has_certificate: true,
      days_to_expiry: daysToExpiry,
      certificate_status: "vence_em_breve",
    }
  }

  return {
    has_certificate: true,
    days_to_expiry: daysToExpiry,
    certificate_status: "ativo",
  }
}

export async function getParalegalCertificates(companyIds: string[] | null = null): Promise<ParalegalCertificateItem[]> {
  const companies = await getCompaniesForUser("all")
  const filtered = companyIds?.length ? companies.filter((company) => companyIds.includes(company.id)) : companies

  return filtered
    .map((company) => ({
      ...company,
      ...getCertificateStatus(company),
    }))
    .sort((a, b) => {
      const order: Record<CertificateStatus, number> = {
        vence_em_breve: 0,
        vencido: 1,
        ativo: 2,
        sem_certificado: 3,
      }
      const byStatus = order[a.certificate_status] - order[b.certificate_status]
      if (byStatus !== 0) return byStatus
      const aDays = a.days_to_expiry ?? Number.POSITIVE_INFINITY
      const bDays = b.days_to_expiry ?? Number.POSITIVE_INFINITY
      if (aDays !== bDays) return aDays - bDays
      return a.name.localeCompare(b.name)
    })
}

export function getParalegalCertificateSummary(items: ParalegalCertificateItem[]): ParalegalCertificateSummary {
  return items.reduce<ParalegalCertificateSummary>(
    (acc, item) => {
      acc.total += 1
      if (item.certificate_status === "ativo") acc.ativos += 1
      if (item.certificate_status === "vence_em_breve") acc.venceEmBreve += 1
      if (item.certificate_status === "vencido") acc.vencidos += 1
      if (item.certificate_status === "sem_certificado") acc.semCertificado += 1
      return acc
    },
    {
      total: 0,
      ativos: 0,
      venceEmBreve: 0,
      vencidos: 0,
      semCertificado: 0,
    }
  )
}
