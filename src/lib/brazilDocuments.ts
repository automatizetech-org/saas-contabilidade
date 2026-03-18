export function onlyDigits(value: string | null | undefined): string {
  return String(value ?? "").replace(/\D/g, "")
}

function isRepeatedDigits(value: string): boolean {
  return /^(\d)\1+$/.test(value)
}

export function isValidCpf(value: string | null | undefined): boolean {
  const digits = onlyDigits(value)
  if (digits.length !== 11 || isRepeatedDigits(digits)) return false

  let sum = 0
  for (let index = 0; index < 9; index += 1) {
    sum += Number(digits[index]) * (10 - index)
  }
  let remainder = (sum * 10) % 11
  if (remainder === 10) remainder = 0
  if (remainder !== Number(digits[9])) return false

  sum = 0
  for (let index = 0; index < 10; index += 1) {
    sum += Number(digits[index]) * (11 - index)
  }
  remainder = (sum * 10) % 11
  if (remainder === 10) remainder = 0
  return remainder === Number(digits[10])
}

export function isValidCnpj(value: string | null | undefined): boolean {
  const digits = onlyDigits(value)
  if (digits.length !== 14 || isRepeatedDigits(digits)) return false

  const calculateDigit = (base: string, factors: number[]) => {
    const total = base
      .split("")
      .reduce((sum, digit, index) => sum + Number(digit) * factors[index], 0)
    const remainder = total % 11
    return remainder < 2 ? 0 : 11 - remainder
  }

  const firstDigit = calculateDigit(digits.slice(0, 12), [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
  if (firstDigit !== Number(digits[12])) return false

  const secondDigit = calculateDigit(digits.slice(0, 13), [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2])
  return secondDigit === Number(digits[13])
}

export function isValidCpfOrCnpj(value: string | null | undefined): boolean {
  const digits = onlyDigits(value)
  if (digits.length === 11) return isValidCpf(digits)
  if (digits.length === 14) return isValidCnpj(digits)
  return false
}

export function formatCpf(value: string | null | undefined): string {
  const digits = onlyDigits(value)
  if (digits.length !== 11) return String(value ?? "")
  return digits.replace(/(\d{3})(\d{3})(\d{3})(\d{2})/, "$1.$2.$3-$4")
}

export function formatCnpj(value: string | null | undefined): string {
  const digits = onlyDigits(value)
  if (digits.length !== 14) return String(value ?? "")
  return digits.replace(/(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})/, "$1.$2.$3/$4-$5")
}

export function formatCpfInput(value: string | null | undefined): string {
  const digits = onlyDigits(value).slice(0, 11)
  if (digits.length <= 3) return digits
  if (digits.length <= 6) return `${digits.slice(0, 3)}.${digits.slice(3)}`
  if (digits.length <= 9) return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6)}`
  return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6, 9)}-${digits.slice(9)}`
}

export function formatCnpjInput(value: string | null | undefined): string {
  const digits = onlyDigits(value).slice(0, 14)
  if (digits.length <= 2) return digits
  if (digits.length <= 5) return `${digits.slice(0, 2)}.${digits.slice(2)}`
  if (digits.length <= 8) return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5)}`
  if (digits.length <= 12) return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8)}`
  return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8, 12)}-${digits.slice(12)}`
}

export function formatCpfOrCnpjInput(value: string | null | undefined): string {
  const digits = onlyDigits(value)
  return digits.length <= 11 ? formatCpfInput(digits) : formatCnpjInput(digits)
}
