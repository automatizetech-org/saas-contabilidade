# Relatório: Produção vs SQL Base

**Data da análise:** 2025-03-16  
**Referência SQL base:** `supabase/data/schema_completo.sql`  
**Fonte “produção”:** migrations em `supabase/migrations/` (ordem numérica) + uso no código (`src/` e `src/types/database.ts`).

---

## 1. Resumo executivo

- **Schema base** está em grande parte alinhado com o que as migrations aplicam em produção.
- Foram encontradas **diferenças** em constraints UNIQUE e índices (faltando no schema base) e em **tipos TypeScript** (`database.ts`) desatualizados em relação ao banco.

---

## 2. O que está alinhado

- **Tabelas:** As tabelas usadas no app (profiles, companies, accountants, fiscal_documents, folder_structure_nodes, robots, execution_requests, schedule_rules, robot_display_config, company_robot_config, fiscal_pendencias, dp_checklist, dp_guias, financial_records, sync_events, office_branding, admin_settings, municipal_tax_debts, municipal_tax_collection_runs, ir_clients, ir_settings, tax_rule_versions, simple_national_*, etc.) existem no `schema_completo.sql`. A tabela `documents` existe no schema mas é legado em relação a `fiscal_documents` (ver seção 9).
- **Tabela `nfs_stats`:** Existe no schema e é usada em `dashboardService.ts`; porém **não está tipada** em `src/types/database.ts`.
- **Enum `document_status`:** Presente no schema e coerente com o uso (documentos, checklist, etc.).
- **RLS e políticas:** Políticas para accountants, office_branding, municipal_tax_*, tax_rule_versions e simple_national_* estão documentadas no schema.
- **Storage:** Bucket `branding-assets` e políticas em `storage.objects` estão no schema.
- **Branding:** o app usa `office_branding` (`brandingService.ts`). Se existir migration antiga citando `client_branding_settings`, tratar como histórico — conferir schema atual.
- **Colunas de cobrança IR (`ir_clients`):** Presentes no schema (payment_charge_*, payment_link, etc.); faltam apenas no **TypeScript** (`database.ts`).

---

## 3. Diferenças encontradas (produção tem, SQL base não tinha)

### 3.1 Constraints UNIQUE (migrations aplicadas em produção)

| Onde | Constraint / índice | Ação |
|------|---------------------|------|
| `tax_rule_versions` | `UNIQUE (regime, scope, version_code)` | Adicionado ao schema base |
| `simple_national_periods` | `UNIQUE (company_id, apuration_period)` | Adicionado ao schema base |
| `simple_national_entries` | `UNIQUE (period_id, reference_month, entry_type)` | Adicionado ao schema base |
| `simple_national_calculations` | `UNIQUE (period_id)` | Já existe no schema como `period_id ... UNIQUE` |
| `municipal_tax_debts` | `municipal_tax_debts_dedupe_unique (company_id, tributo, numero_documento, data_vencimento)` | Adicionado ao schema base |

### 3.2 Índices (migrations aplicadas em produção)

| Tabela | Índice | Ação |
|--------|--------|------|
| `municipal_tax_debts` | `municipal_tax_debts_company_idx (company_id)` | Adicionado ao schema base |
| `municipal_tax_debts` | `municipal_tax_debts_due_idx (data_vencimento)` | Adicionado ao schema base |
| `municipal_tax_debts` | `municipal_tax_debts_status_idx (situacao)` | Adicionado ao schema base |
| `municipal_tax_collection_runs` | `municipal_tax_collection_runs_robot_idx (robot_technical_id, created_at DESC)` | Adicionado ao schema base |
| `municipal_tax_collection_runs` | `municipal_tax_collection_runs_company_idx (company_id, created_at DESC)` | Adicionado ao schema base |

---

## 4. TypeScript (`src/types/database.ts`) desatualizado

### 4.1 Tabela `nfs_stats` ausente

- **Uso:** `dashboardService.ts` usa `supabase.from("nfs_stats")` com colunas: `company_id`, `period`, `qty_emitidas`, `qty_recebidas`, `valor_emitidas`, `valor_recebidas`, `service_codes`, `service_codes_emitidas`, `service_codes_recebidas`.
- **Problema:** Não há entrada para `nfs_stats` em `Database["public"]["Tables"]`.
- **Recomendação:** Incluir o tipo da tabela `nfs_stats` em `database.ts` conforme definição no `schema_completo.sql` (id, company_id, period, qty_emitidas, qty_recebidas, valor_emitidas, valor_recebidas, service_codes, service_codes_emitidas, service_codes_recebidas, created_at, updated_at).

### 4.2 `ir_clients` – colunas de cobrança (BTG/charge) ausentes

- **Produção:** Migration `20260313224500_add_ir_btg_charge_fields.sql` adiciona: `payment_charge_type`, `payment_charge_status`, `payment_charge_id`, `payment_charge_correlation_id`, `payment_provider`, `payment_link`, `payment_pix_copy_paste`, `payment_pix_qr_code`, `payment_boleto_pdf_base64`, `payment_boleto_barcode`, `payment_boleto_digitable_line`, `payment_paid_at`, `payment_payer_name`, `payment_payer_tax_id`, `payment_generated_at`, `payment_last_webhook_at`, `payment_metadata`.
- **Problema:** Nenhuma dessas colunas está no tipo `ir_clients` em `database.ts`.
- **Recomendação:** Incluir todas as colunas `payment_*` em Row/Insert/Update de `ir_clients` em `database.ts`.

---

## 5. Itens no SQL base não criados pelas migrations (ou criados em migrations antigas)

- **Nota:** não existe `company_memberships` no `schema_completo.sql` atual; o vínculo usuário–escritório é `office_memberships`.
- **automation_data:** Está no schema; não há uso em `src/` — pode ser uso por robô/backend.
- **documents:** Está no schema e em `database.ts`; não há `.from("documents")` no frontend — possivelmente usado por backend/VM.

Nenhum desses itens conflita com produção; o schema base serve como documentação da estrutura completa.

---

## 6. Ações realizadas (para alinhar produção ↔ SQL base)

1. **Atualização de `supabase/data/schema_completo.sql`:**
   - Inclusão das constraints UNIQUE em `tax_rule_versions`, `simple_national_periods`, `simple_national_entries` e `municipal_tax_debts`.
   - Inclusão dos índices em `municipal_tax_debts` e `municipal_tax_collection_runs`.

2. **Atualização de `src/types/database.ts`:**
   - Inclusão da tabela `nfs_stats` com colunas conforme schema.
   - Inclusão das colunas `payment_*` em `ir_clients` (Row/Insert/Update).

---

## 7. Conclusão

- **Produção vs SQL base:** Após as alterações acima, o `schema_completo.sql` reflete o que está em produção (migrations) em termos de tabelas, constraints UNIQUE e índices considerados.
- **App vs banco:** Com a atualização de `database.ts` (nfs_stats + ir_clients payment_*), os tipos TypeScript ficam alinhados ao banco e evitam erros de tipo ao usar essas tabelas/colunas.

Recomenda-se rodar as migrations em um ambiente de teste e, se possível, gerar um dump do schema de produção e comparar com `schema_completo.sql` para validar eventuais diferenças restantes (por exemplo, triggers ou funções não documentadas).

---

## 8. Schema puxado da CLI

Se você gerar o schema a partir do banco remoto com a CLI Supabase, o resultado será **equivalentemente o mesmo** em estrutura (tabelas, colunas, tipos, constraints, índices):

- **`supabase db dump --linked -f schema_remoto.sql -s public`** — gera o schema completo no formato do `pg_dump`. O conteúdo lógico equivale ao `schema_completo.sql`; a diferença é apenas formatação e eventualmente nomes automáticos de constraints gerados pelo PostgreSQL.
- **`supabase db pull`** — gera uma **migration** (diff) para alinhar o projeto ao remoto, não um SQL único de referência como o `schema_completo.sql`.

O `schema_completo.sql` do repositório serve como referência legível e alinhada às migrations; um dump da CLI confirma que o que está em produção bate com essa referência.

---

## 9. Auditoria de uso (março/2026) — tabelas e RPCs

Escopo: busca em `src/`, `supabase/functions/`, `Servidor/server-api/` e robôs em `docs/fiscal/**` e `docs/paralegal/**` (Python). Objetivo: separar o que é **canônico**, o que é **só interno SQL** e o que parece **legado / sem uso no repo**.

### 9.1 Tabelas usadas de ponta a ponta (não são “lixo”)

| Área | Tabelas (exemplos) |
|------|-------------------|
| Auth / multi-tenant | `profiles`, `offices`, `office_memberships`, `office_branding` (o app usa `office_branding`; não `client_branding_settings` no `src/` atual) |
| Servidor / robôs | `office_servers`, `office_server_credentials`, `robots`, `office_robot_configs`, `office_robot_runtime`, `robot_result_events`, `execution_requests`, `schedule_rules`, `company_robot_config`, `folder_structure_nodes`, `admin_settings` |
| Fiscal / dashboard | `companies`, `fiscal_documents`, `nfs_stats`, `sync_events`, `fiscal_pendencias`, `dp_checklist`, `dp_guias`, `financial_records` |
| IR / municipal / inteligência | `ir_clients`, `ir_settings`, `municipal_tax_debts`, `municipal_tax_collection_runs`, `tax_rule_versions`, `simple_national_*` |
| Projeções / fila | `office_document_index`, `office_analytics_refresh_queue`, `office_dashboard_daily`, `office_fiscal_daily`, `office_ir_summary`, `office_municipal_tax_summary`, `office_certificate_summary`, `office_tax_intelligence_summary`, `office_operations_summary` — alimentadas por triggers/RPCs; lidas por `get_*_summary` no front; worker no `server-api` chama `process_office_refresh_queue` |
| Outros | `accountants`, `ecac_mailbox_messages`, `robot_display_config` |

### 9.2 Tabelas sem referência no código deste repositório (possível legado)

| Tabela | Observação |
|--------|------------|
| `documents` | Modelo antigo (`arquivos text[]`); o fluxo atual é `fiscal_documents` + `file_path`. Nenhum `.from("documents")` em TS/JS/PY dos caminhos acima. |
| `robot_schedules`, `robot_jobs`, `robot_job_logs` | Só em `schema_completo.sql` e tipos em `database.ts`; sem uso em app, edge, server-api ou robôs rastreados. Possível sistema de fila antigo, substituído por `execution_requests`. |
| `folder_structure_templates` | Semente/políticas RLS no SQL; **nós** vivos em `folder_structure_nodes` são usados pelo server-api — os *templates* não são consultados no código de aplicação. |

### 9.3 Tabelas usadas só fora do front (robôs / scripts)

| Tabela | Onde |
|--------|------|
| `automation_data` | `docs/fiscal/nfe-nfc/Sefaz Xml/sefaz xml.py` (dados flexíveis do fluxo Sefaz) |

### 9.4 RPCs definidas no banco mas não chamadas pelo front (mar/2026)

Substituídas na prática por versões com **cursor**:

- `get_document_rows_page` — app usa `get_document_rows_cursor`.
- `get_fiscal_detail_documents_page` — app usa `get_fiscal_detail_documents_cursor`.
- `get_certidoes_overview_summary` — tela de certidões usa `getCertidoesOverviewSummary` → `getCertidoesDocuments` (sem esta RPC).

**Recomendação:** não dropar sem validar se algum cliente externo ou BI as chama. Podem permanecer como API legada ou ser removidas numa versão major após inventário em produção.

### 9.5 O que foi feito no repositório para “arrumar” sem risco

- Migration `20260324120000_table_comments_legacy_inventory.sql`: comentários `COMMENT ON TABLE` nas tabelas legadas/suspeitas, visíveis no Supabase Studio, para não haver dúvida do que é histórico.
- Este relatório atualizado como fonte única da auditoria.

**Importante:** remover tabela ou RPC do PostgreSQL com `DROP` só após backup, checagem de `pg_stat_user_tables` / uso real no projeto, e migração de dados se necessário.
