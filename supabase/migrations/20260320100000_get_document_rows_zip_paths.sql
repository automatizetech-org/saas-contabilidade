-- RPC leve para listar apenas file_path, empresa e category_key para montar ZIP de toda a lista filtrada.
-- Mesmos filtros de get_document_rows_cursor, sem cursor, limite 50000.

-- Índice parcial para acelerar listagens que exigem file_path (ZIP e primeiras cargas).
create index if not exists office_document_index_office_date_has_file_idx
  on public.office_document_index (office_id, coalesce(document_date, (created_at at time zone 'UTC')::date) desc, created_at desc, source_record_id desc)
  where file_path is not null and btrim(file_path) <> '';

create or replace function public.get_document_rows_zip_paths(
  company_ids uuid[] default null,
  category_filter text default null,
  file_kind text default null,
  search_text text default null,
  date_from date default null,
  date_to date default null,
  limit_count integer default 50000
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_office_id uuid := public.current_office_id();
  v_has_index boolean;
  v_payload jsonb;
begin
  select exists(
    select 1
    from public.office_document_index
    where office_id = v_office_id
    limit 1
  ) into v_has_index;

  if not v_has_index then
    perform public.refresh_office_document_index(v_office_id);
  end if;

  with params as (
    select
      coalesce(company_ids, '{}'::uuid[]) as company_ids,
      coalesce(array_length(company_ids, 1), 0) as company_count,
      lower(coalesce(nullif(btrim(category_filter), ''), 'todos')) as category_filter,
      lower(coalesce(nullif(btrim(file_kind), ''), 'todos')) as file_kind,
      public.normalize_search_text(search_text) as search_text,
      public.only_digits(search_text) as digits_search,
      least(get_document_rows_zip_paths.date_from, get_document_rows_zip_paths.date_to) as date_from,
      greatest(get_document_rows_zip_paths.date_from, get_document_rows_zip_paths.date_to) as date_to,
      least(greatest(coalesce(limit_count, 50000), 1), 50000) as limit_count
  ),
  base as (
    select
      i.file_path,
      i.empresa,
      i.category_key
    from public.office_document_index i
    join params p on true
    where i.office_id = v_office_id
      and i.file_path is not null
      and btrim(i.file_path) <> ''
      and (p.company_count = 0 or i.company_id = any (p.company_ids))
      and (p.category_filter = 'todos' or i.category_key = p.category_filter)
      and (
        p.file_kind = 'todos'
        or (p.file_kind = 'xml' and i.file_extension = 'xml')
        or (p.file_kind = 'pdf' and i.file_extension = 'pdf')
      )
      and (p.date_from is null or coalesce(i.document_date, i.created_at::date) >= p.date_from)
      and (p.date_to is null or coalesce(i.document_date, i.created_at::date) <= p.date_to)
      and (
        p.search_text = ''
        or i.search_text_normalized like '%' || p.search_text || '%'
        or (p.digits_search <> '' and public.only_digits(coalesce(i.cnpj, '')) like '%' || p.digits_search || '%')
        or coalesce(i.chave, '') like '%' || btrim(coalesce(get_document_rows_zip_paths.search_text, '')) || '%'
      )
    order by coalesce(i.document_date, i.created_at::date) desc, i.created_at desc, i.source_record_id desc
    limit (select p.limit_count from params p)
  )
  select coalesce(
    (select jsonb_agg(
      jsonb_build_object(
        'file_path', file_path,
        'empresa', empresa,
        'category_key', category_key
      )
    ) from base),
    '[]'::jsonb
  )
  into v_payload;

  return coalesce(v_payload, '[]'::jsonb);
end;
$$;
