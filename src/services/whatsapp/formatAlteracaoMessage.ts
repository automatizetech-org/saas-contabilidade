/**
 * Módulo WhatsApp — Alteração Empresarial (só formatação de texto).
 * Envio: whatsappApi (office-server + escritório ou WHATSAPP_API legado).
 */

import { formatCNPJ, formatCurrencyBRL } from "@/lib/validators";
import { QUALIFICACAO_DISPLAY } from "@/services/bcbSalarioMinimoService";
import type { QualificacaoPlano } from "@/services/bcbSalarioMinimoService";

export interface WhatsAppFormPayload {
  /** Tipo do formulário: abertura | alteracao_contratual | suspensao | baixa */
  tipo_formulario: string;
  razao_social: string;
  cnpj: string;
  qualificacao_plano: string;
  data_abertura: string;
  tipo_atividade: string;
  inscricao_estadual: string;
  inscricao_municipal: string;
  competencia_inicial: string;
  tributacao: string;
  possui_st: string;
  /** Possui Retenção de Impostos (sim/nao/nao_informado) */
  possui_retencao_impostos: string;
  socios: Array<{ nome_socio: string; cpf_socio: string }>;
  /** Lista de contatos: cada um com nome, e-mail e telefone */
  contatos: Array<{ nome_contato: string; email_contato: string; telefone_contato: string }>;
  possui_prolabore: string;
  valor_prolabore?: string;
  possui_empregados: string;
  possui_contabilidade: string;
  tipo_contabilidade: string;
  regime_contabil: string;
  possui_parcelamento: string;
  /** Um ou mais tipos de parcelamento (lista) */
  tipo_parcelamento?: string;
  tipos_parcelamento?: string[];
  contador_responsavel_cpf?: string;
  contador_responsavel_nome?: string;
  valor_honorario: string;
  vencimento_honorario: string;
  data_primeiro_honorario: string;
  observacao: string;
}

const SIM_NAO_LABEL: Record<string, string> = {
  sim: "Sim",
  nao: "Não",
  nao_informado: "Não informado",
};

const TIPO_FORMULARIO_LABEL: Record<string, string> = {
  abertura: "ABERTURA",
  alteracao_contratual: "ALTERAÇÃO CONTRATUAL",
  suspensao: "SUSPENSÃO",
  baixa: "BAIXA",
};

/**
 * Formata o formulário de Alteração Empresarial para envio no WhatsApp.
 * Usa ao máximo a formatação do WhatsApp: *negrito*, _itálico_, `monoespaço` e separadores.
 */
export function formatAlteracaoMessage(form: WhatsAppFormPayload): string {
  const lines: string[] = [];
  const sep = "─────────────────────";
  const v = (s: string) => (s?.trim() ? s.trim() : "—");

  const tipoFormLabel = form.tipo_formulario?.trim()
    ? (TIPO_FORMULARIO_LABEL[form.tipo_formulario.trim()] ?? form.tipo_formulario.trim())
    : "—";
  lines.push("*📋 " + tipoFormLabel + "*");
  lines.push(sep);
  lines.push("");

  lines.push("*1️⃣ Identificação da Empresa*");
  lines.push(`  _Razão Social:_\n  ${v(form.razao_social)}`);
  lines.push(`  _CNPJ:_ \`${formatCNPJ(form.cnpj) || v(form.cnpj)}\``);
  const qualDisplay = QUALIFICACAO_DISPLAY[form.qualificacao_plano?.trim().toUpperCase() as QualificacaoPlano];
  const qualTexto = qualDisplay ? `${qualDisplay.emoji} ${form.qualificacao_plano?.trim()}` : v(form.qualificacao_plano);
  lines.push(`  _Qualificação do Plano:_ ${qualTexto}`);
  lines.push(`  _Data de Abertura:_ \`${v(form.data_abertura)}\``);
  lines.push(`  _Tipo de Atividade:_ ${v(form.tipo_atividade)}`);
  lines.push("");
  lines.push(sep);
  lines.push("");

  lines.push("*2️⃣ Inscrições e Enquadramento*");
  lines.push(`  _Inscrição Estadual:_ \`${v(form.inscricao_estadual)}\``);
  lines.push(`  _Inscrição Municipal:_ ${v(form.inscricao_municipal)}`);
  lines.push(`  _Competência Inicial:_ \`${v(form.competencia_inicial)}\``);
  lines.push(`  _Tributação:_ ${v(form.tributacao)}`);
  lines.push(`  _Possui Substituição Tributária:_ *${SIM_NAO_LABEL[form.possui_st] ?? form.possui_st}*`);
  lines.push(`  _Possui Retenção de Impostos:_ *${SIM_NAO_LABEL[form.possui_retencao_impostos] ?? form.possui_retencao_impostos}*`);
  lines.push("");
  lines.push(sep);
  lines.push("");

  lines.push("*3️⃣ Dados Societários e Contato*");
  form.socios.forEach((s, i) => {
    if (s.nome_socio || s.cpf_socio) {
      const label = form.socios.length > 1 ? `Sócio ${i + 1}` : "Sócio";
      lines.push(`  _${label}:_`);
      lines.push(`    Nome: ${v(s.nome_socio)}`);
      lines.push(`    CPF: \`${v(s.cpf_socio)}\``);
    }
  });
  if (!form.socios?.length || form.socios.every((s) => !s.nome_socio && !s.cpf_socio)) {
    lines.push("  _Sócio:_ —");
  }
  (form.contatos ?? []).forEach((c, i) => {
    if (c.nome_contato || c.email_contato || c.telefone_contato) {
      const label = (form.contatos?.length ?? 0) > 1 ? `Contato ${i + 1}` : "Contato";
      lines.push(`  _${label}:_`);
      lines.push(`    Nome: ${v(c.nome_contato)}`);
      lines.push(`    E-mail: \`${v(c.email_contato)}\``);
      lines.push(`    Telefone: \`${v(c.telefone_contato)}\``);
    }
  });
  if (!form.contatos?.length || form.contatos.every((c) => !c.nome_contato && !c.email_contato && !c.telefone_contato)) {
    lines.push("  _Contato:_ —");
  }
  lines.push("");
  lines.push(sep);
  lines.push("");

  lines.push("*4️⃣ Kits de obrigações*");
  lines.push("");
  lines.push("  • Pró-labore: *" + (SIM_NAO_LABEL[form.possui_prolabore] ?? form.possui_prolabore) + "*");
  if (form.possui_prolabore === "sim") {
    lines.push("    _Valor do Pró-labore:_ `R$ " + (formatCurrencyBRL(form.valor_prolabore) || "0,00") + "`");
  }
  lines.push("  • Empregados: *" + (SIM_NAO_LABEL[form.possui_empregados] ?? form.possui_empregados) + "*");
  lines.push("  • Contabilidade: *" + (SIM_NAO_LABEL[form.possui_contabilidade] ?? form.possui_contabilidade) + "*");
  if (form.possui_contabilidade === "sim" && form.tipo_contabilidade) {
    lines.push("    _Tipo:_ " + form.tipo_contabilidade);
  }
  lines.push("  • Regime Contábil: " + v(form.regime_contabil));
  lines.push("  • Parcelamento: *" + (SIM_NAO_LABEL[form.possui_parcelamento] ?? form.possui_parcelamento) + "*");
  const parcelamentos = (form.tipos_parcelamento ?? (form.tipo_parcelamento ? [form.tipo_parcelamento] : [])).filter((t) => t?.trim());
  if (form.possui_parcelamento === "sim" && parcelamentos.length > 0) {
    lines.push("  _Tipos:_");
    parcelamentos.forEach((t) => {
      lines.push("    – " + t.trim());
    });
  }
  const contadorResponsavel = form.contador_responsavel_nome?.trim()
    ? `${form.contador_responsavel_nome.trim()}${form.contador_responsavel_cpf ? ` (CPF ${form.contador_responsavel_cpf})` : ""}`
    : (form.contador_responsavel_cpf ? `CPF ${form.contador_responsavel_cpf}` : "-");
  lines.push("  - Contador Responsavel: " + contadorResponsavel);
  lines.push("");
  lines.push(sep);
  lines.push("");

  lines.push("*5️⃣ Honorários*");
  lines.push(`  _Valor do Honorário Mensal:_ \`R$ ${formatCurrencyBRL(form.valor_honorario) || "0,00"}\``);
  lines.push(`  _Data de Vencimento:_ dia \`${v(form.vencimento_honorario)}\``);
  lines.push(`  _Data do Primeiro Honorário:_ \`${v(form.data_primeiro_honorario)}\``);

  if (form.observacao?.trim()) {
    lines.push("");
    lines.push(sep);
    lines.push("");
    lines.push("*📌 Observação*");
    lines.push(form.observacao.trim());
  }

  return lines.join("\n");
}
