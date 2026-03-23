-- Inventário: comentários em tabelas pouco ou nunca referenciadas pelo app (src/, edge, server-api).
-- Não remove objetos — apenas documenta no catálogo do Postgres (visível no Supabase Studio).

comment on table public.documents is
  'Legado (modelo com arquivos text[]): fluxo atual usa fiscal_documents.file_path. Sem uso em src/, edge ou server-api neste repositório (auditoria mar/2026). Avaliar dados antes de DROP.';

comment on table public.robot_schedules is
  'Possível legado de agendamento: sem referências em src/, supabase/functions ou Servidor/server-api (mar/2026). execution_requests é o fluxo ativo de jobs.';

comment on table public.robot_jobs is
  'Possível legado ligado a robot_schedules: sem referências em código de aplicação neste repositório (mar/2026).';

comment on table public.robot_job_logs is
  'Logs de robot_jobs: sem referências em código de aplicação neste repositório (mar/2026).';

comment on table public.folder_structure_templates is
  'Templates hierárquicos (seed SQL): nós efetivos em folder_structure_nodes são usados pelo server-api; templates não são consultados pelo app TypeScript (mar/2026).';

comment on table public.automation_data is
  'Dados flexíveis de automação: usado por robôs/scripts (ex. Sefaz XML em docs/), não pelo front web.';
