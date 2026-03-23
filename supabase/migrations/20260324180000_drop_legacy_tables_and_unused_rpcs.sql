-- Remove objetos legados não usados pelo app (cursor + fiscal_documents são a fonte de verdade).
-- Mantém copy_default_folder_structure para primeiro-escritório: árvore embutida (sem folder_structure_templates).
-- ATENÇÃO: apaga dados das tabelas listadas. Faça backup antes em produção.

-- ---------------------------------------------------------------------------
-- 1) Função usada por supabase/functions/primeiro-escritório — sem ler templates
-- ---------------------------------------------------------------------------
create or replace function public.copy_default_folder_structure(target_office_id uuid)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
  id_fiscal uuid;
  id_dp uuid;
  id_par uuid;
begin
  insert into public.folder_structure_nodes (office_id, parent_id, name, slug, date_rule, position)
  values (target_office_id, null, 'Fiscal', 'fiscal', null, 1)
  returning id into id_fiscal;

  insert into public.folder_structure_nodes (office_id, parent_id, name, slug, date_rule, position)
  values
    (target_office_id, id_fiscal, 'NFS', 'nfs', 'year_month_day', 1),
    (target_office_id, id_fiscal, 'NFE-NFC', 'nfe-nfc', 'year_month_day', 2),
    (target_office_id, id_fiscal, 'Certidoes', 'certidoes', 'year_month_day', 3);

  insert into public.folder_structure_nodes (office_id, parent_id, name, slug, date_rule, position)
  values (target_office_id, null, 'Departamento Pessoal', 'dp', null, 2)
  returning id into id_dp;

  insert into public.folder_structure_nodes (office_id, parent_id, name, slug, date_rule, position)
  values (target_office_id, id_dp, 'Guias', 'guias', 'year_month_day', 1);

  insert into public.folder_structure_nodes (office_id, parent_id, name, slug, date_rule, position)
  values (target_office_id, null, 'Paralegal', 'paralegal', null, 3)
  returning id into id_par;

  insert into public.folder_structure_nodes (office_id, parent_id, name, slug, date_rule, position)
  values (target_office_id, id_par, 'Taxas e Impostos', 'taxas-impostos', 'year_month_day', 1);
end;
$$;

-- ---------------------------------------------------------------------------
-- 2) RPCs legadas (substituídas por get_document_rows_cursor / get_fiscal_detail_documents_cursor)
-- ---------------------------------------------------------------------------
revoke all on function public.get_document_rows_page(uuid[], text, text, text, date, date, integer, integer) from public;
revoke all on function public.get_document_rows_page(uuid[], text, text, text, date, date, integer, integer) from authenticated;
revoke all on function public.get_document_rows_page(uuid[], text, text, text, date, date, integer, integer) from anon;
revoke all on function public.get_document_rows_page(uuid[], text, text, text, date, date, integer, integer) from service_role;

revoke all on function public.get_fiscal_detail_documents_page(text, uuid[], text, date, date, text, text, text, text, integer, integer) from public;
revoke all on function public.get_fiscal_detail_documents_page(text, uuid[], text, date, date, text, text, text, text, integer, integer) from authenticated;
revoke all on function public.get_fiscal_detail_documents_page(text, uuid[], text, date, date, text, text, text, text, integer, integer) from anon;
revoke all on function public.get_fiscal_detail_documents_page(text, uuid[], text, date, date, text, text, text, text, integer, integer) from service_role;

revoke all on function public.get_certidoes_overview_summary(uuid[]) from public;
revoke all on function public.get_certidoes_overview_summary(uuid[]) from authenticated;
revoke all on function public.get_certidoes_overview_summary(uuid[]) from anon;
revoke all on function public.get_certidoes_overview_summary(uuid[]) from service_role;

drop function if exists public.get_document_rows_page(uuid[], text, text, text, date, date, integer, integer);
drop function if exists public.get_fiscal_detail_documents_page(text, uuid[], text, date, date, text, text, text, text, integer, integer);
drop function if exists public.get_certidoes_overview_summary(uuid[]);

-- ---------------------------------------------------------------------------
-- 3) Tabelas legadas (ordem: dependentes primeiro)
-- ---------------------------------------------------------------------------
drop policy if exists robot_job_logs_select on public.robot_job_logs;
drop policy if exists robot_job_logs_write on public.robot_job_logs;
drop policy if exists robot_jobs_select on public.robot_jobs;
drop policy if exists robot_jobs_write on public.robot_jobs;
drop policy if exists robot_schedules_select on public.robot_schedules;
drop policy if exists robot_schedules_write on public.robot_schedules;
drop policy if exists documents_select on public.documents;
drop policy if exists documents_write on public.documents;
drop policy if exists folder_structure_templates_select on public.folder_structure_templates;
drop policy if exists folder_structure_templates_write on public.folder_structure_templates;

drop trigger if exists robot_job_logs_set_updated_at on public.robot_job_logs;
drop trigger if exists robot_jobs_set_updated_at on public.robot_jobs;
drop trigger if exists robot_schedules_set_updated_at on public.robot_schedules;
drop trigger if exists documents_set_updated_at on public.documents;
drop trigger if exists folder_structure_templates_set_updated_at on public.folder_structure_templates;

drop table if exists public.robot_job_logs cascade;
drop table if exists public.robot_jobs cascade;
drop table if exists public.robot_schedules cascade;
drop table if exists public.documents cascade;
drop table if exists public.folder_structure_templates cascade;

drop type if exists public.robot_job_status;
