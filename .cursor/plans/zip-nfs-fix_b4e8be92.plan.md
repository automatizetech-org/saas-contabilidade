---
name: zip-nfs-fix
overview: Corrigir ZIP/Next pagination e alinhar contagem por `file_path` em `/fiscal/nfs` e `/fiscal/nfe-nfc` para baixar/mostrar exatamente os arquivos únicos do período filtrado.
todos:
  - id: zip-fiscal-use-cursor-paths
    content: Alterar `src/pages/FiscalDetailPage.tsx` para que ZIP em NFS/NFE-NFC use `getFiscalDetailDocumentPathsForZip` (cursor) em vez de `getUnifiedDocumentsZipPaths`.
    status: pending
  - id: fallback-cursor-dedupe
    content: "Atualizar `src/services/documentsService.ts` no `catch` do `getFiscalDetailDocumentsPage` para: (1) aplicar filtro por cursor e (2) deduplicar por `file_path` antes de paginar (limit/hasMore/nextCursor)."
    status: pending
  - id: pagination-hasMore-nextCursor
    content: No `src/pages/FiscalDetailPage.tsx`, ajustar `CursorPagination` para receber `hasMore={Boolean(documentsPage?.nextCursor)}` ao invés de depender de `documentsPage?.hasMore`.
    status: pending
  - id: sql-dedupe-filepath
    content: Criar nova migration SQL para atualizar `public.get_fiscal_detail_documents_cursor` deduplicando por `file_path` antes de aplicar cursor e limitar/hasMore/nextCursor (para consistência mesmo quando o RPC não falha).
    status: pending
  - id: verify-nfs-nfecnf
    content: "Testar manualmente: `/fiscal/nfs` e `/fiscal/nfe-nfc` com período/mês atual e filtros `XML e PDF` + `Todas`; validar Next (não repete) e ZIP (bate com arquivos físicos únicos do período)."
    status: pending
isProject: false
---

## Objetivo

Garantir que:

- O ZIP em `/fiscal/nfs` e `/fiscal/nfe-nfc` baixe todos os `file_path` únicos correspondentes ao que a tabela considera.
- O botão `Next` avance corretamente (sem repetir a mesma página) e só apareça quando existe `nextCursor` real.
- A tabela mostre contagem alinhada com o ZIP/pasta do servidor, ou seja: 1 linha por `file_path` (arquivo físico único), conforme sua preferência `B`.

## Diagnóstico (pontos que explicam o comportamento atual)

1. O `Next` repete a mesma lista ao clicar.
  - Isso é típico quando a chamada ao RPC `get_fiscal_detail_documents_cursor` falha e o código cai no `catch` do `getFiscalDetailDocumentsPage`, mas o `catch` atual NÃO aplica filtro por cursor.
  - Resultado: a “página 2” volta a renderizar o slice inicial.
2. O ZIP em NFS/NFE-NFC usa uma RPC “unificada” (`get_document_rows_zip_paths`) baseada em `office_document_index`.
  - Quando o cursor RPC está falhando e a tabela renderiza via fallback (lendo da fonte direta), o ZIP continua dependente do index unificado e pode ficar incompleto.
3. NFE-NFC mostra mais linhas na tabela do que arquivos físicos no servidor.
  - Isso sugere duplicidade (vários `source_record_id`/documentos apontando para o mesmo `file_path`).
  - Sua preferência é alinhar para 1 linha por `file_path`.

## Estratégia

- Alinhar origem do ZIP com a mesma lógica da tabela: gerar lista de `file_path` via paginação/cursor (função de service `getFiscalDetailDocumentPathsForZip`).
- Corrigir fallback para respeitar cursor e deduplicar por `file_path` antes de paginar.
- Ajustar `hasMore`/`Next` para depender do `nextCursor` (evita inconsistências visuais).
- (Recomendado) Deduplicar também no SQL do RPC cursor para que a tabela seja consistente mesmo quando o RPC não falha.

## Fluxo proposto (visão geral)

```mermaid
flowchart TD
  UI[UI (/fiscal/nfs ou /fiscal/nfe-nfc)] -->|render tabela| Q1[getFiscalDetailDocumentsPage]
  UI -->|ZIP click| Q2[getFiscalDetailDocumentPathsForZip]
  Q2 -->|itera cursor| Q1
  Q1 -->|RPC ok| RPC[(get_fiscal_detail_documents_cursor)]
  Q1 -->|RPC falha| FB[catch: fallback com cursor + dedupe por file_path]
```



## Mudanças específicas

### 1) ZIP usar a mesma “fonte” da tabela (corrige ZIP incompleto)

- Arquivo: `src/pages/FiscalDetailPage.tsx`
- Substituir o onClick atual que chama `getUnifiedDocumentsZipPaths` (unified RPC) pelo uso de:
  - `getFiscalDetailDocumentPathsForZip` (já existe em `src/services/documentsService.ts`)
- Manter dedupe por `file_path` (modo `B`) com base no retorno da função.

### 2) Corrigir fallback do cursor para não repetir páginas

- Arquivo: `src/services/documentsService.ts`
- No `catch` de `getFiscalDetailDocumentsPage`:
  - Aplicar filtro pelo cursor (`filters.cursor`) sobre a lista ordenada (tuple por `sortDate`/`createdAt`/`id`, equivalente ao SQL).
  - Deduplicar por `file_path` (arquivo físico único) antes de calcular `items.slice(0, limit)`, `hasMore` e `nextCursor`.

### 3) `Next` só quando houver `nextCursor`

- Arquivo: `src/pages/FiscalDetailPage.tsx`
- Ajustar `hasMore` para `Boolean(documentsPage?.nextCursor)`.

### 4) Deduplicar no SQL do cursor (consistência quando RPC funciona)

- Arquivo SQL (recomendado via nova migration):
  - Função a alterar: `public.get_fiscal_detail_documents_cursor`.
- Implementar dedupe por `file_path` no CTE de `base`, antes de aplicar filtro de cursor e paginação.
  - Resultado: tabela já vem com 1 linha por `file_path` e contagem/paginação ficam coerentes.

### 5) Validação

- Abrir `/fiscal/nfs` com filtros `XML e PDF` + `Todas`.
  - Confirmar que `Next` avança e nunca repete a página.
  - Confirmar que ZIP contém exatamente os mesmos arquivos físicos únicos do período.
- Repetir em `/fiscal/nfe-nfc`.
  - Confirmar que a tabela exibe 1 linha por `file_path` e que ZIP bate com a pasta física.

## Arquivos envolvidos

- `src/pages/FiscalDetailPage.tsx`
- `src/services/documentsService.ts`
- Nova migration SQL para ajustar `public.get_fiscal_detail_documents_cursor` (para dedupe por `file_path`).

