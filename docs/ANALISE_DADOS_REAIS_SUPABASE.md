# Análise: Dados reais vs mock/placeholder no projeto

**Data da análise:** 2026-03-19

## Resumo executivo

- **Maioria do projeto:** lê dados reais do Supabase (RPCs e tabelas).
- **Pontos em aberto (dados não reais):**
  1. **Paralegal – Tarefas e Clientes por tier:** usam arrays mock estáticos (MOCK_TASKS, MOCK_CLIENT_TIERS).
  2. **Inteligência Tributária – Lucro Real / Lucro Presumido:** apenas placeholder de UI (sem backend ainda).
- **Dev sem .env:** `supabaseClient` usa URL/key placeholder para o app montar; login falha até configurar (comportamento esperado).

---

## Onde os dados JÁ SÃO REAIS (Supabase)

### Dashboard
- **get_dashboard_overview_summary** (RPC): totais de notas, arquivos, processados hoje, DP, contábil, sync, gráficos. Deduplicação por chave/id na migration.
- **getRecentFiscalDocuments**: lista recente vinda de `fiscal_documents` com deduplicação por chave/id.
- Fallback legacy: usa `fiscal_documents` + deduplicação no front.

### Fiscal
- **get_fiscal_overview_analytics_summary** (RPC): totais no período, emitidos hoje, empresas com emissão, gráficos por tipo/status/mês/empresa. Deduplicação por chave/id.
- **get_fiscal_detail_summary** (RPC): totais por tipo (NFS/NFE-NFC) na página de detalhe.
- **get_fiscal_detail_documents_cursor** (RPC): listagem paginada com deduplicação por file_path.
- **get_fiscal_detail_document_zip_paths** (RPC): caminhos para download em ZIP.
- **getNfsStatsByDateRange** / **get_nfs_stats_range_summary**: dados de NFS (nfs_stats).

### Documentos (hub unificado)
- **get_document_rows_cursor** / **get_document_rows_zip_paths**: documentos fiscais e outros.
- **getUnifiedDocumentsPage**: combina fiscal_documents, certidões, dp_guias, municipal_tax_debts.

### Certidões
- **get_certidoes_overview_summary** (RPC): resumo e listagem de certidões.
- **getFiscalDetailDocumentsPage** (cursor) para aba Certidões.

### IR (Imposto de Renda)
- **get_ir_overview_summary** (RPC): cards e gráficos (clientes, recebidos, a pagar, valor total).
- **getIrClients**: tabela `ir_clients`. Fallback do overview calcula a partir dessa lista (dados reais).

### Paralegal (parcial)
- **get_paralegal_certificate_overview_summary** (RPC): certificados por empresa (status ativo/vencido/etc.).
- **paralegalService**: certificados derivados de `companies` (cert_valid_until, auth_mode).
- **Salário mínimo:** `bcbSalarioMinimoService` (dados reais; fallback 1518 se indisponível).
- **Taxas e impostos municipais:** `get_municipal_tax_overview_summary`, `get_municipal_tax_debts_page`, tabela `municipal_tax_debts` – tudo real.

### Inteligência Tributária
- **get_tax_intelligence_overview_summary** (RPC): visão geral.
- **Simples Nacional:** empresas do Supabase, draft e simulação salvos no Supabase; cálculos locais com dados reais.

### Operações
- **get_operations_overview_summary** (RPC): resumo de operações.
- **getRobots**, **getRecentExecutionRequests**: tabelas/APIs reais.

### Empresas, contadores, admin
- **companies**, **accountants**, **profiles**, **offices**, **office_memberships**: todas as telas usam Supabase.
- **Admin:** usuários, escritório, servidor, empresas – dados reais.

### Outros serviços
- **tributaryIntelligenceService**, **municipalTaxesService**, **operationsService**, **irService**, **documentsService**, **dashboardService** (após correções): leituras baseadas em RPCs ou tabelas do Supabase com contagens/métricas consistentes (notas = documento único por chave/id onde aplicável).

---

## Pontos em aberto (dados NÃO reais)

### 1. Paralegal – Tarefas (tab “Tarefas”)
- **Arquivo:** `src/pages/ParalegalPage.tsx`
- **O que é mock:** `MOCK_TASKS` – array estático com 5 tarefas (alteração contratual, baixa estadual, etc.).
- **Onde aparece:** cards “Tarefas no front”, “Prioridade alta”, “Vencem hoje”, “Atrasadas” e a lista de tarefas no painel de Tarefas.
- **Motivo:** não existe tabela/RPC de “tarefas paralegal” no Supabase hoje. Para virar real é preciso criar tabela (ex.: `paralegal_tasks`) e RPC (ou leitura direta) e trocar o uso de `MOCK_TASKS` por essa fonte.

### 2. Paralegal – Clientes por tier (honorário / carteira)
- **Arquivo:** `src/pages/ParalegalPage.tsx`
- **O que é mock:** `MOCK_CLIENT_TIERS` – array estático com empresa, honorário e carteira para classificação DIAMANTE/OURO/PRATA/BRONZE.
- **Onde aparece:** cards por tier e gráficos do painel “Clientes” (qualificação por honorário vs salário mínimo).
- **Motivo:** não existe tabela de “honorário por empresa” ou “carteira” no projeto. O salário mínimo já é real (BCB). Para virar real seria necessário ter uma tabela (ex.: honorários/carteira por empresa) e alimentar os cards/gráficos a partir dela.

### 3. Inteligência Tributária – Lucro Real e Lucro Presumido
- **Arquivo:** `src/pages/InteligenciaTributariaTopicPage.tsx`
- **O que é:** `PlaceholderTopic` – apenas mensagem “Estrutura inicial pronta para expansão” para os tópicos `lucro-real` e `lucro-presumido`.
- **Motivo:** funcionalidade ainda não implementada (sem backend nem cálculos). Simples Nacional já está implementado e usa dados reais.

---

## Supabase client em desenvolvimento

- **Arquivo:** `src/services/supabaseClient.ts`
- **Comportamento:** se `.env` não tiver `SUPABASE_URL` e `SUPABASE_ANON_KEY`, usa URL e key placeholder para o app montar; o login falha até configurar.
- **Conclusão:** esperado para desenvolvimento; não é “dado fake” de negócio.

---

## Conclusão

| Área                    | Dados reais? | Observação                                      |
|-------------------------|-------------|--------------------------------------------------|
| Dashboard               | Sim         | RPC + fallback com deduplicação                  |
| Fiscal (geral + detalhe)| Sim         | RPCs com deduplicação por chave/id               |
| Documentos (hub)        | Sim         | RPCs e tabelas                                  |
| Certidões               | Sim         | RPC + companies                                 |
| IR                      | Sim         | RPC + ir_clients                                |
| Paralegal – Certificados| Sim         | RPC + companies                                 |
| Paralegal – Taxas/Impostos | Sim     | municipal_tax_*                                  |
| Paralegal – Tarefas     | Não         | MOCK_TASKS (sem tabela no Supabase)              |
| Paralegal – Clientes tier | Não       | MOCK_CLIENT_TIERS (sem tabela de honorário)      |
| Int. Trib. – Simples Nacional | Sim   | Supabase + cálculos                             |
| Int. Trib. – Lucro Real / Presumido | Não  | Só placeholder de UI                             |
| Operações, Empresas, Admin | Sim    | Supabase                                        |

Para ter **todos** os dados reais no projeto, faltam:
1. Modelo (tabela + RPC ou leitura) para **tarefas paralegal** e substituir `MOCK_TASKS`.
2. Modelo (tabela + leitura) para **honorário/carteira por empresa** e substituir `MOCK_CLIENT_TIERS`.
3. Implementação futura de **Lucro Real** e **Lucro Presumido** (backend + UI).
