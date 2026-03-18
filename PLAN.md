# Plano de Escala e Hardening para 100 Escritórios + Picos

## Resumo
Levar o sistema do estado atual para um alvo operacional de **100 escritórios**, cada um com **até 1.000 empresas** e **100 mil+ documentos por escritório**, com:
- isolamento multi-tenant preservado por `office_id`
- cards e gráficos servidos por RPC segura com leitura barata
- listas de documentos em **cursor pagination** real, sem page-number custoso
- atualização visual em **5-15s**
- validação por teste de carga antes de qualquer promessa comercial de “rodar liso”

A implementação será em 4 frentes: **segurança de execução**, **modelo de leitura escalável**, **atualização quase em tempo real**, **teste/observabilidade**.

## Mudanças de Implementação
### 1. Fechar a superfície de segurança antes de escalar
- Restringir `EXECUTE` das RPCs novas para `authenticated` e revogar de `anon` onde couber.
- Padronizar `security definer` + `set search_path = public` + escopo por `public.current_office_id()` em todas as RPCs analíticas e de paginação.
- Adicionar testes de abuso para RPCs com `company_ids` de outro escritório, `detail_kind` inválido, filtros arbitrários e chamadas sem sessão.
- Substituir `ngrok` como caminho de produção por endpoint estável do conector/VM com domínio fixo, TLS estável e segredo do conector obrigatório em todas as rotas expostas.
- Aplicar rate limit e timeout explícitos nas edge functions e no `server-api`, com foco em `office-server`, downloads, ZIPs e paginações pesadas.

### 2. Trocar leitura pesada por projeções escaláveis
- Manter as RPCs como interface pública, mas mudar a implementação delas para ler de **tabelas de projeção** e não de `UNION ALL`/scan direto sobre tabelas operacionais em runtime.
- Criar um catálogo unificado de documentos, por exemplo `office_document_index`, contendo apenas o necessário para listagem:
  - `office_id`, `source`, `category_key`, `company_id`, `document_date`, `created_at`, `status`, `type`, `origem`, `modelo`, `tipo_certidao`, `file_path`, `chave`, `search_text_normalized`
- Alimentar esse catálogo para:
  - `fiscal_documents`
  - `dp_guias`
  - `municipal_tax_debts`
  - último estado válido de certidões derivado de `sync_events`
- Reescrever `get_document_rows_page` e `get_fiscal_detail_documents_page` para **cursor pagination**:
  - entrada: filtros + `cursor_sort_date` + `cursor_id` + `limit`
  - saída: `rows[]`, `next_cursor`, `has_more`
  - remover dependência de `row_number()` e `count(*) over()` nessas listas grandes
- Reescrever `get_municipal_tax_debts_page` no mesmo modelo quando a tabela for tratada como lista operacional grande; se permanecer lista secundária, manter page-size pequeno e contagem separada.
- Adicionar índices no catálogo:
  - `(office_id, document_date desc, id desc)`
  - `(office_id, category_key, document_date desc, id desc)`
  - `(office_id, company_id, document_date desc, id desc)`
  - trigram/GIN para `search_text_normalized`
  - parciais por `source` e por `file extension` se necessário

### 3. Fazer cards e gráficos lerem de resumo barato
- Manter o contrato “cards/gráficos por RPC”, mas trocar a origem para **tabelas resumo por módulo**:
  - `office_dashboard_daily`
  - `office_fiscal_daily`
  - `office_ir_summary`
  - `office_municipal_tax_summary`
  - `office_certificate_summary`
  - `office_tax_intelligence_summary`
  - `office_operations_summary`
- Atualizar esses resumos por **fila de refresh** em lote, não por full scan:
  - triggers leves nas tabelas-fonte só enfileiram `office_id`/`company_id`/`module`
  - um worker/cron processa a fila a cada 5-10s e recompõe os agregados daquele escopo
- As RPCs atuais passam a:
  - ler dos resumos quando disponíveis
  - cair para recomputação controlada apenas em bootstrap/recovery
- Padronizar os dashboards do frontend para polling inteligente:
  - `refetchInterval` de 10s com aba visível
  - 30-60s com aba oculta
  - invalidação imediata após ações locais relevantes
- Não expandir realtime websocket global agora; o alvo escolhido é **quase tempo real em 5-15s** com polling barato e previsível.

### 4. Corrigir os pontos do frontend que ainda não escalam
- Remover das páginas grandes qualquer dependência de “página exata” com total em tempo real; listas grandes passam a usar cursor.
- Adaptar `DocumentosPage` e `FiscalDetailPage` para:
  - próxima/anterior por cursor
  - filtros persistidos
  - export atual baseada em seleção ou filtro, não em “todos carregados no client”
- Revisar `ParalegalPage`, `IRPage`, `OperacoesPage` e telas equivalentes para garantir que:
  - cards/gráficos não recaiam em `.select()` pesado quando a RPC falhar
  - listas detalhadas não materializem datasets grandes no browser
- Manter CRUD simples em client + RLS onde o volume não justificar backend especial:
  - empresas
  - contadores
  - configurações
  - perfis

## APIs, Interfaces e Tipos
- RPCs de lista grande passam a usar cursor em vez de page-number:
  - substituir `page_number`/`page_size` por `limit`, `cursor_sort_date`, `cursor_id`
  - retorno inclui `next_cursor`, `has_more`
- RPCs de overview continuam retornando JSON agregado, mas sua implementação passa a ler de resumo persistido.
- Tipos em `src/types/database.ts` precisam refletir os novos contratos de cursor.
- Services do frontend precisam padronizar:
  - `items`
  - `nextCursor`
  - `hasMore`
  - `refreshAt`
- O `server-api` e `office-server` devem tratar limites explícitos de lote, timeout e rate limit como parte da interface operacional.

## Testes e Critérios de Aceitação
- Carga sintética base:
  - 100 escritórios
  - 1.000 empresas por escritório
  - 100.000 documentos fiscais por escritório
  - débitos municipais, guias DP, certidões e IR em volume proporcional
  - pico com 300 sessões autenticadas concorrentes + 20 sincronizações simultâneas
- Testes obrigatórios:
  - abuso multi-tenant em todas as RPCs novas
  - navegação profunda por cursor nas listas de documentos
  - busca textual com filtros combinados
  - polling simultâneo de dashboards em múltiplos escritórios
  - sync concorrente atualizando catálogo e resumos sem vazar entre escritórios
- Metas mínimas de aceite:
  - dashboard/card RPC p95 < 500 ms
  - gráfico agregado p95 < 700 ms
  - primeira página de documentos p95 < 700 ms
  - próxima página por cursor p95 < 400 ms
  - erro < 1% sob carga-alvo
  - zero vazamento entre escritórios nos testes de abuso
- Observabilidade mínima:
  - logs estruturados por `office_id`, rota RPC, duração e erro
  - monitor de timeout/erro para edge functions e `server-api`
  - captura de plano de execução (`EXPLAIN ANALYZE`) para RPCs críticas
  - script de carga versionado no repo

## Assumptions e Defaults
- Meta oficial adotada: **100 escritórios + picos**.
- Frescor escolhido: **5-15s**, não sub-segundo.
- Paginação escolhida: **cursor rápida**, não página exata.
- “Segurança total” não será tratada como promessa absoluta; o aceite será por hardening + testes de abuso + observabilidade.
- `ngrok` não entra como solução final para essa meta de produção; ele fica apenas como transição/homologação.
