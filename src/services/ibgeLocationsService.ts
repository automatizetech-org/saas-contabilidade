export type IbgeState = {
  code: string
  name: string
}

export type IbgeCity = {
  name: string
}

type IbgeStateApiRow = {
  sigla?: string
  nome?: string
}

type IbgeCityApiRow = {
  nome?: string
}

const IBGE_BASE_URL = "https://servicodados.ibge.gov.br/api/v1/localidades"

export async function getBrazilStates(): Promise<IbgeState[]> {
  const response = await fetch(`${IBGE_BASE_URL}/estados?orderBy=nome`, {
    method: "GET",
    headers: { Accept: "application/json" },
  })
  if (!response.ok) throw new Error("Não foi possível carregar os estados.")
  const rows = (await response.json()) as IbgeStateApiRow[]
  return rows
    .map((row) => ({
      code: String(row.sigla ?? "").trim().toUpperCase(),
      name: String(row.nome ?? "").trim(),
    }))
    .filter((row) => row.code && row.name)
}

export async function getCitiesByState(stateCode: string): Promise<IbgeCity[]> {
  const uf = String(stateCode ?? "").trim().toUpperCase()
  if (!uf) return []
  const response = await fetch(`${IBGE_BASE_URL}/estados/${uf}/municipios?orderBy=nome`, {
    method: "GET",
    headers: { Accept: "application/json" },
  })
  if (!response.ok) throw new Error("Não foi possível carregar os municípios.")
  const rows = (await response.json()) as IbgeCityApiRow[]
  return rows
    .map((row) => ({ name: String(row.nome ?? "").trim() }))
    .filter((row) => row.name)
}
