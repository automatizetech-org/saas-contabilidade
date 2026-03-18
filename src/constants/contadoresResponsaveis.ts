export const CONTADORES_RESPONSAVEIS = [
  { nome: "ELIANDERSON GOMES FLEURY", cpf: "71361170115" },
  { nome: "EDER GOMES FLEURY", cpf: "86873598100" },
] as const;

export function formatContadorResponsavelLabel(contador: { nome: string; cpf: string }) {
  return `${contador.nome} — CPF ${contador.cpf.replace(/(\d{3})(\d{3})(\d{3})(\d{2})/, "$1.$2.$3-$4")}`;
}
