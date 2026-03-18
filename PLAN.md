# Plano Final Fechado para SaaS Vendável com 1 VM por Escritório, `ngrok` Fixo, Conector Robusto de Arquivos, Branding por Escritório e Segurança de Produção

## Resumo
Objetivo: deixar o produto pronto para venda imediata, com arquitetura multi-tenant real por `escritório`, uma VM por escritório, execução local dos robôs, SaaS centralizado em Vercel/Supabase, URL pública fixa por escritório, conector robusto para navegar qualquer estrutura de pastas abaixo de uma `base_path` escolhida no admin, branding isolado por escritório e baseline de segurança de produção pensado com visão de hacker.

Decisões travadas:
- O tenant principal é `escritório`.
- Cada escritório terá `1 VM ativa` e `1 conector ativo` em produção.
- Cada escritório terá `1 URL pública fixa` própria, armazenada em banco.
- O frontend em produção nunca resolve endpoint de VM por `.env`.
- O backend/control-plane resolve a VM correta por `office_id`.
- Toda operação sensível contra a VM passa pelo backend do SaaS.
- O conector da VM usa `Bearer token` exclusivo por escritório na v1.
- A `base_path` do escritório é configurada no admin e armazenada em `office_servers.base_path`.
- O conector deve operar qualquer estrutura de pastas abaixo da `base_path`, sem depender de layout rígido.
- O vínculo de negócio do arquivo com a empresa vem do processo de ingestão/robô/metadado persistido, nunca apenas do nome da pasta.
- O branding é por `office_id`, nunca por empresa.
- Se o escritório não tiver branding customizado, usa branding padrão da plataforma.
- Segurança forte, auditoria, limites operacionais e isolamento cross-tenant são requisito de go-live.

## Arquitetura e Fluxos Principais
### Tenancy, identidade e autorização
- Formalizar `offices`, `office_memberships`, `office_servers`, `office_branding`, `robot_schedules`, `robot_jobs`, `robot_job_logs` e adicionar `office_id` em `companies`.
- Manter a cadeia obrigatória:
  - `user -> office_memberships -> offices`
  - `company -> office_id`
  - `branding -> office_id`
  - `documents/jobs/logs/stats -> company_id` e `office_id`
- Separar papéis:
  - `super_admin` para plataforma
  - `owner/admin/operator/viewer` para escritório
- Toda leitura ou mutação deve validar a relação real entre `user`, `office`, `company`, `document`, `job`, `office_server` e `office_branding`.
- `selectedCompanyIds` é apenas filtro visual; nunca fonte de autorização.

### URL pública da VM por escritório
- A URL pública de cada VM fica em `office_servers.public_base_url`.
- A URL é fixa por escritório no setup atual.
- Não haverá `SERVER_API_URL` ou `WHATSAPP_API` globais de produção no Vercel.
- Estrutura mínima de `office_servers`:
  - `id`
  - `office_id`
  - `public_base_url`
  - `status`
  - `is_active`
  - `last_seen_at`
  - `connector_version`
  - `base_path`
  - `server_secret_hash`
  - `created_at`
  - `updated_at`
- Permitir histórico de servidores por escritório, mas apenas `1 is_active = true` por vez.

### Branding por escritório
- O branding será armazenado em tabela própria `office_branding`.
- Estrutura mínima de `office_branding`:
  - `id`
  - `office_id`
  - `display_name`
  - `logo_file_path`
  - `favicon_file_path`
  - `primary_color`
  - `secondary_color`
  - `accent_color`
  - `created_at`
  - `updated_at`
- O branding pertence ao escritório inteiro, não a empresas individuais.
- Apenas `owner`, `admin` do escritório e `super_admin` podem editar branding.
- `operator` e `viewer` apenas visualizam.
- Se não existir branding customizado para o escritório, o frontend usa o branding padrão da plataforma.
- Assets de branding devem ficar em storage com caminho escopado por `office_id`.
- Uploads de branding devem validar:
  - tipo de arquivo permitido
  - tamanho máximo
  - dimensões máximas/mínimas quando aplicável
- RLS e regras de storage devem impedir leitura ou alteração de branding de outro escritório.
- Branding futuro de domínio/subdomínio customizado pode ser evolução posterior, mas não é requisito da v1 vendável.

### Resolução de requisições para a VM correta
- Fluxo padrão:
  1. usuário autenticado chama o SaaS
  2. backend resolve `office_id`
  3. backend valida a relação com `company_id`/`document_id`/`job_id`
  4. backend busca `office_servers.public_base_url`
  5. backend chama a VM correta com credencial do escritório
  6. backend devolve a resposta ao frontend
- Regra absoluta: frontend nunca chama `ngrok` direto em produção.
- Downloads, sync, execução de job, ações administrativas e leitura sensível passam sempre pelo backend do SaaS.

### Agendamento, fila e execução
- O SaaS persiste agendas em `robot_schedules`.
- O cron do Supabase cria/disponibiliza `robot_jobs`.
- Cada job contém:
  - `office_id`
  - `company_ids`
  - `status`
  - `attempt_count`
  - `claimed_at`
  - `claimed_by_server_id`
  - `timeout_at`
  - `last_error`
  - `created_at`
- Claim de job deve ser atômico.
- O conector da VM busca apenas jobs do próprio `office_id`, faz claim, executa localmente e devolve:
  - status
  - logs
  - caminhos dos arquivos
  - resultados por empresa
  - totais e estatísticas
- O backend valida que qualquer `company_id` enviado pelo conector pertence ao `office_id` autenticado.

### Downloads e arquivos
- O arquivo físico fica somente na VM do escritório.
- O banco guarda:
  - `office_id`
  - `company_id`
  - `file_path`
  - `filename`
  - `extension`
  - `size`
  - `hash`
  - `detected_at`
  - metadados e status
- Todo download valida:
  - `user -> office -> company -> document -> office_server`
- A VM nunca expõe diretório bruto.
- O backend do SaaS faz `streaming proxy`; não persiste arquivo no control-plane.
- ZIPs e downloads unitários são montados apenas com arquivos autorizados daquele escritório.

## Contratos Fechados de API
### Endpoints internos do SaaS
- `GET /api/office-server/status`
  - retorna status, versão, heartbeat e capacidade do conector do escritório ativo
- `POST /api/office-server/test-connection`
  - testa conectividade com a VM do escritório ativo
- `POST /api/jobs/:jobId/reprocess`
  - reprocessa job autorizado do escritório ativo
- `POST /api/files/download`
  - body: `document_id`
  - faz proxy autenticado para a VM e stream do arquivo
- `POST /api/files/download-zip`
  - body: `document_ids[]`
  - valida autorização e faz proxy/stream do ZIP
- `POST /api/files/sync`
  - body: parâmetros da operação manual permitida
  - dispara sync autorizado do escritório ativo
- `GET /api/branding`
  - retorna branding do escritório ativo com fallback para branding padrão
- `POST /api/branding`
  - cria ou atualiza branding do escritório ativo quando autorizado

### Endpoints do conector da VM
- `POST /connector/heartbeat`
  - recebe autenticação do escritório
  - atualiza status do conector
- `POST /connector/jobs/pull`
  - consulta jobs pendentes do próprio escritório
- `POST /connector/jobs/:jobId/claim`
  - claim controlado e idempotente
- `POST /connector/jobs/:jobId/complete`
  - envia resultado, logs e arquivos gerados
- `POST /connector/files/download`
  - body: `relative_path` autorizado
  - retorna stream do arquivo
- `POST /connector/files/download-zip`
  - body: `relative_paths[]`
  - retorna stream do ZIP
- `POST /connector/files/tree`
  - lista árvore ou subset abaixo da `base_path`
- `POST /connector/files/resolve`
  - valida e resolve caminho relativo com segurança
- Todos os endpoints do conector exigem `Authorization: Bearer <secret>`.

## Conector da VM e Server de Arquivos Robusto
### Requisito de robustez para `base_path`
- O conector da VM será baseado no server existente da pasta `Servidor`, endurecido para produção.
- A `base_path` operacional será configurada na tela admin e persistida em `office_servers.base_path`.
- Fonte de verdade da `base_path`: o SaaS.
- A VM sincroniza essa configuração a partir do SaaS e só opera com a `base_path` validada.
- Ao atualizar `base_path`, o SaaS deve testar acesso antes de salvar como ativa e disparar reindexação controlada.

### Modelo operacional do conector
- O server da VM trata `base_path` como raiz autorizada única.
- Toda operação recebe apenas caminhos relativos a essa raiz.
- O server deve conseguir:
  - listar árvore
  - resolver caminho relativo seguro
  - identificar tipo de item
  - baixar arquivo
  - montar ZIP
  - escanear/indexar estrutura
- O sistema não presume taxonomia fixa de pastas.
- O vínculo do arquivo com a empresa vem do robô/processo de ingestão/metadado persistido, nunca só da pasta.
- O conector deve tolerar:
  - nomes de pasta diferentes
  - profundidades diferentes
  - categorias diferentes
  - reorganização manual da árvore local

### Segurança do filesystem
- Toda resolução de caminho deve:
  - normalizar separadores
  - resolver `.` e `..`
  - rejeitar caminho absoluto do cliente
  - garantir permanência dentro da `base_path`
- Bloquear:
  - path traversal
  - qualquer symlink na v1
  - caminhos UNC inseguros
  - caminhos com encoding malicioso
- O server nunca retorna estrutura acima da `base_path`.
- Logs não devem expor caminhos absolutos sem necessidade.

### Limites operacionais obrigatórios
- Definir paginação para listagem de árvore.
- Definir profundidade máxima de exploração por request.
- Definir timeout de operações de leitura/ZIP.
- Definir tamanho máximo de ZIP por request.
- Definir quantidade máxima de arquivos por request.
- Definir limite de concorrência por escritório para operações de filesystem.
- Esses limites devem valer desde a v1 para reduzir abuso e DoS.

## Segurança de Produção com Visão de Hacker
### Regras gerais
- Nenhuma confiança em IDs, paths ou hints do cliente.
- Nenhuma autorização baseada em frontend.
- Nenhum endpoint aceita `company_id`, `office_id`, `file_path`, `document_id` ou `office_server_id` sem validação relacional forte.
- Não pode existir acesso cross-tenant por manipulação de request, troca de ID, reuso de token ou endpoint esquecido.

### Supabase e banco
- Reescrever RLS para remover qualquer policy aberta demais.
- Nenhuma tabela de negócio pode ter `USING (true)` ou `WITH CHECK (true)` para `authenticated` ou `anon`.
- Separar acesso de:
  - usuário final
  - admin da plataforma
  - conector da VM
- `service_role` só em backend/edge functions/control-plane.
- Adicionar constraints e índices para impedir:
  - vínculos duplicados
  - empresas fora do escritório
  - jobs duplicados
  - múltiplos servidores ativos indevidos
  - claims inconsistentes
  - múltiplos registros de branding ativo para o mesmo escritório, se houver necessidade de unicidade lógica

### Autenticação e sessão
- Login por e-mail com mensagens neutras.
- Rate limit por IP e por identidade.
- Cooldown progressivo ou lockout.
- Reset de senha seguro.
- Sessão com expiração e refresh controlado.
- MFA obrigatório ou fortemente recomendado para `super_admin`.
- Nenhuma alteração de perfil pode elevar privilégio.

### Autenticação do SaaS para o conector da VM
- A autenticação v1 será por `Bearer token` exclusivo por escritório.
- O segredo nasce no SaaS/control-plane.
- O segredo é exibido uma única vez no provisionamento.
- No banco, armazenar apenas `server_secret_hash`.
- Toda chamada do SaaS para a VM exige `Authorization: Bearer <secret>`.
- Rotação de segredo invalida imediatamente o anterior.
- `HMAC` fica como evolução futura.

### Registro da VM no sistema
- O registro inicial de `office_server` será manual pela plataforma.
- Recomendação adotada:
  - criação inicial por `super_admin` ou painel da plataforma
  - preenchimento de `public_base_url`
  - preenchimento de `base_path`
  - geração do segredo do conector
- O escritório não cria seu próprio `office_server` sem fluxo controlado.
- Heartbeat só atualiza status; não cria vínculo novo automaticamente.

### Heartbeat
- Heartbeat a cada `1 minuto`.
- Payload mínimo:
  - `office_server_id`
  - `connector_version`
  - `status`
  - `last_job_at`
  - `host_fingerprint`
  - `base_path_fingerprint` não sensível
- Sem heartbeat dentro da janela definida: marcar `status = offline`.
- O heartbeat também valida versão mínima permitida.

### Logs e retenção
- Separar:
  - log operacional do escritório
  - log de segurança global da plataforma
- Admin local vê apenas logs do próprio escritório.
- Plataforma vê logs globais e incidentes.
- Definir retenção operacional mínima desde a v1.
- Não logar segredos, tokens, senhas, certificados, caminhos absolutos desnecessários ou payloads sensíveis completos.

### Failover e indisponibilidade
- Se a VM estiver offline:
  - jobs ficam pendentes ou reprogramáveis
  - downloads dependentes da VM falham com mensagem operacional clara
  - status do escritório marca `server_offline`
  - alerta operacional dispara para a plataforma
- Não haverá failover cross-tenant.
- Isolamento vence disponibilidade improvisada.

### Versionamento do conector
- O conector deve usar versionamento semântico explícito.
- O SaaS mantém versão mínima suportada.
- Heartbeat compara versão do conector com versão mínima permitida.
- Versão insegura ou incompatível pode ser bloqueada para operações críticas.

## Operação e Painéis
### Painel do escritório
- Deve incluir:
  - empresas
  - usuários
  - branding
  - servidor ativo
  - `public_base_url`
  - `base_path`
  - status do conector
  - último heartbeat
  - versão do conector
  - rotação de segredo
  - teste de conexão
  - agendamentos
  - fila e histórico de jobs
- Tela de branding deve permitir:
  - editar nome exibido
  - enviar/trocar logo
  - enviar/trocar favicon
  - ajustar cores
  - restaurar branding padrão
- O admin local só vê recursos do próprio escritório.

### Painel da plataforma
- Deve incluir:
  - escritórios
  - plano/status
  - status do servidor
  - cadastro/edição de `public_base_url`
  - cadastro/edição de `base_path`
  - reset/rotação de segredo
  - visão consolidada de jobs e falhas
  - bloqueio de conector inseguro ou offline
  - visão e suporte sobre branding por escritório
- Apenas `super_admin` opera recursos globais.

### Provisionamento para vender amanhã
- Checklist operacional:
  1. criar `office`
  2. criar usuário admin inicial
  3. vincular admin em `office_memberships`
  4. criar registro em `office_servers`
  5. preencher `public_base_url`
  6. preencher `base_path`
  7. gerar segredo do conector
  8. configurar branding inicial ou deixar padrão da plataforma
  9. instalar/configurar VM
  10. subir conector
  11. validar heartbeat
  12. testar conexão e download
  13. ativar escritório
- Artefato de onboarding técnico da VM:
  - script instalador
  - configuração local da VM
  - serviço Windows/PM2
  - healthcheck local
  - persistência segura de credenciais e `base_path`

## Testes e Validação de Go-Live
- Testar isolamento entre dois escritórios com dados misturados no mesmo banco.
- Testar que usuário do escritório A não acessa recursos do B alterando body, query, path ou IDs.
- Testar IDOR em documentos, empresas, jobs, branding, office servers e usuários.
- Testar claim concorrente do mesmo job.
- Testar expiração, retry e idempotência de job.
- Testar dois escritórios agendando no mesmo minuto.
- Testar download e ZIP sempre via VM correta.
- Testar `streaming proxy` sem persistência indevida no SaaS.
- Testar brute force, enumeração e mensagens neutras no login.
- Testar rotação de segredo com invalidação imediata.
- Testar heartbeat, servidor offline e bloqueio de versão insegura.
- Testar alteração de `public_base_url` sem impactar outros escritórios.
- Testar alteração de `base_path`, validação pré-save e reindexação controlada.
- Testar branding com:
  - fallback para padrão da plataforma
  - isolamento por `office_id`
  - upload válido e inválido
  - bloqueio de acesso cross-tenant
  - leitura correta dos assets por escritório
- Testar server da VM contra:
  - árvores rasas e profundas
  - nomes de pasta arbitrários
  - múltiplos tipos de arquivo
  - reorganização manual de pastas
  - path traversal
  - symlink
  - caminhos absolutos
  - encoded traversal
- Checklist final:
  - sem policies abertas
  - sem `service_role` no frontend
  - sem segredo em logs
  - sem acesso cross-tenant
  - sem endpoint público desnecessário
  - sem chamada direta do browser ao `ngrok`
  - sem leitura fora da `base_path`
  - sem dependência de layout rígido de pastas
  - sem dependência de env global do Vercel para escolher VM
  - sem branding exposto ou editável fora do escritório correto

## Assumptions
- O SaaS continuará centralizado em Vercel + Supabase.
- Cada escritório terá sua própria VM e seu próprio endpoint público fixo.
- A URL pública por escritório será armazenada em `office_servers.public_base_url`.
- A raiz de arquivos operacional da VM será armazenada em `office_servers.base_path`.
- O branding por escritório será armazenado em `office_branding`, com fallback para branding padrão da plataforma.
- O server da pasta `Servidor` será a base do conector de produção, endurecido para multi-tenant, autenticação forte e navegação robusta de filesystem.
- A URL do `ngrok` é fixa no setup atual.
- Em produção, o frontend não resolve endpoint de VM via `.env`; isso será sempre resolvido pelo backend/control-plane com base no `office_id`.
- Onde houver conflito entre conveniência e isolamento, vence o isolamento.
