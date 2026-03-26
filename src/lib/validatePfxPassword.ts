/**
 * Valida se a senha informada abre o certificado .pfx (PKCS#12).
 * Retorna { valid, validUntil, cnpj } — validUntil é a data de vencimento em ISO (YYYY-MM-DD).
 * `cnpj` (14 dígitos) é extraído do conteúdo textual do certificado quando possível.
 */
import forge from "node-forge"

export function validatePfxPassword(pfxBase64: string, password: string): boolean {
  const r = getPfxInfo(pfxBase64, password)
  return r.valid
}

function onlyDigits(s: string) {
  return s.replace(/\D/g, "")
}

function findCnpjInText(s: string): string | undefined {
  const d = onlyDigits(s)
  const m = d.match(/(\d{14})/)
  return m?.[1]
}

function findExactCnpjValue(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined
  const digits = onlyDigits(value)
  return digits.length === 14 ? digits : undefined
}

function collectNestedStrings(value: unknown, output: string[]) {
  if (typeof value === "string") {
    if (value) output.push(value)
    return
  }
  if (Array.isArray(value)) {
    for (const item of value) collectNestedStrings(item, output)
    return
  }
  if (value && typeof value === "object") {
    for (const item of Object.values(value as Record<string, unknown>)) {
      collectNestedStrings(item, output)
    }
  }
}

function extractCnpjFromSubjectAltName(cert: forge.pki.Certificate | undefined): string | undefined {
  if (!cert) return undefined
  try {
    const exts = (cert as unknown as { extensions?: unknown[] }).extensions ?? []
    const exactCandidates: string[] = []
    const looseCandidates: string[] = []
    for (const ext of exts as Array<Record<string, unknown>>) {
      if (ext.name !== "subjectAltName") continue
      const altNames = Array.isArray(ext.altNames) ? ext.altNames : []
      for (const alt of altNames) {
        const nested: string[] = []
        collectNestedStrings(alt, nested)
        for (const item of nested) {
          const exact = findExactCnpjValue(item)
          if (exact) exactCandidates.push(exact)
          const loose = findCnpjInText(item)
          if (loose) looseCandidates.push(loose)
        }
      }
    }
    return exactCandidates[0] || looseCandidates[0]
  } catch {
    return undefined
  }
}

function extractCnpjFromCommonName(cert: forge.pki.Certificate | undefined): string | undefined {
  if (!cert) return undefined
  try {
    const attrs = cert.subject?.attributes ?? []
    const commonName = attrs.find((attr) => attr?.shortName === "CN" || attr?.name === "commonName")?.value
    if (typeof commonName !== "string" || !commonName) return undefined
    const suffixMatch = commonName.match(/:(\d{14})\s*$/)
    if (suffixMatch?.[1]) return suffixMatch[1]
    return findCnpjInText(commonName)
  } catch {
    return undefined
  }
}

function extractCnpjFromCert(cert: forge.pki.Certificate | undefined): string | undefined {
  if (!cert) return undefined

  const sanCnpj = extractCnpjFromSubjectAltName(cert)
  if (sanCnpj) return sanCnpj

  const commonNameCnpj = extractCnpjFromCommonName(cert)
  if (commonNameCnpj) return commonNameCnpj

  const candidates: string[] = []

  try {
    const attrs = cert.subject?.attributes ?? []
    for (const a of attrs) {
      if (typeof a?.value === "string" && a.value) candidates.push(a.value)
    }
  } catch {
    // ignore
  }

  try {
    const exts = (cert as unknown as { extensions?: unknown[] }).extensions ?? []
    for (const ext of exts as Array<Record<string, unknown>>) {
      for (const v of Object.values(ext)) {
        if (typeof v === "string" && v) candidates.push(v)
      }
      // subjectAltName em node-forge geralmente tem `altNames`
      const altNames = ext.altNames
      if (Array.isArray(altNames)) {
        for (const alt of altNames) {
          if (alt && typeof alt === "object") {
            for (const v of Object.values(alt as Record<string, unknown>)) {
              if (typeof v === "string" && v) candidates.push(v)
              else if (typeof v === "number") candidates.push(String(v))
            }
          }
        }
      }
    }
  } catch {
    // ignore
  }

  for (const c of candidates) {
    const m = findCnpjInText(c)
    if (m) return m
  }
  return undefined
}

export function getPfxInfo(
  pfxBase64: string,
  password: string
): { valid: boolean; validUntil?: string; cnpj?: string } {
  if (!pfxBase64 || !password) return { valid: false }
  try {
    const binary = atob(pfxBase64)
    const p12Asn1 = forge.asn1.fromDer(binary)
    const p12 = forge.pkcs12.pkcs12FromAsn1(p12Asn1, password)
    const certBags = p12.getBags({ bagType: forge.pki.oids.certBag })
    const bags = certBags[forge.pki.oids.certBag]
    const cert = bags?.[0]?.cert
    const cnpj = extractCnpjFromCert(cert)
    if (!cert?.validity?.notAfter) return { valid: true, cnpj }
    const d = cert.validity.notAfter
    const validUntil = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`
    return { valid: true, validUntil, cnpj }
  } catch {
    return { valid: false }
  }
}
