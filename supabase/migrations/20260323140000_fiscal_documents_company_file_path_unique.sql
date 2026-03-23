-- Permite upsert/ON CONFLICT (company_id, file_path) usado pelo robot-json-runtime
-- e pelo bot NFE/NFC (result.json: upsert_rows + insert com fallback 23505 company_file_path_key).

with ranked as (
  select
    id,
    row_number() over (
      partition by company_id, file_path
      order by updated_at desc nulls last, created_at desc
    ) as rn
  from public.fiscal_documents
  where file_path is not null
)
delete from public.fiscal_documents fd
where fd.id in (select id from ranked where rn > 1);

do $$
begin
  if not exists (
    select 1
    from pg_constraint c
    join pg_class r on r.oid = c.conrelid
    join pg_namespace n on n.oid = r.relnamespace
    where n.nspname = 'public'
      and r.relname = 'fiscal_documents'
      and c.conname = 'company_file_path_key'
  ) then
    alter table public.fiscal_documents
      add constraint company_file_path_key unique (company_id, file_path);
  end if;
end $$;
