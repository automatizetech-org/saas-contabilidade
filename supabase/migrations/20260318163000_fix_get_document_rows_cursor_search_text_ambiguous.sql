-- Corrige erro 42702: referências ambíguas na RPC get_document_rows_cursor.
-- Parâmetros search_text e limit_count conflitam com colunas do CTE params; qualificamos com o nome da função.

create or replace function public.get_document_rows_cursor(
  company_ids uuid[] default null,
  category_filter text default null,
  file_kind text default null,
  search_text text default null,
  date_from date default null,
  date_to date default null,
  cursor_sort_date date default null,
  cursor_created_at timestamptz default null,
  cursor_id uuid default null,
  limit_count integer default 25
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
      public.normalize_search_text(get_document_rows_cursor.search_text) as search_text,
      public.only_digits(get_document_rows_cursor.search_text) as digits_search,
      least(date_from, date_to) as date_from,
      greatest(date_from, date_to) as date_to,
      greatest(least(coalesce(get_document_rows_cursor.limit_count, 25), 200), 1) as limit_count
  ),
  base as (
    select
      i.*
    from public.office_document_index i
    join params p on true
    where i.office_id = v_office_id
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
        or coalesce(i.chave, '') like '%' || btrim(coalesce(get_document_rows_cursor.search_text, '')) || '%'
      )
      and (
        cursor_sort_date is null
        or (
          (coalesce(i.document_date, i.created_at::date), i.created_at, i.source_record_id)
          < (cursor_sort_date, coalesce(cursor_created_at, 'infinity'::timestamptz), coalesce(cursor_id, 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid))
        )
      )
    order by coalesce(i.document_date, i.created_at::date) desc, i.created_at desc, i.source_record_id desc
    limit (select p.limit_count + 1 from params p)
  ),
  visible as (
    select * from base
    limit (select p.limit_count from params p)
  ),
  cursor_row as (
    select
      coalesce(document_date, created_at::date) as sort_date,
      created_at,
      source_record_id
    from visible
    order by coalesce(document_date, created_at::date) asc, created_at asc, source_record_id asc
    limit 1
  )
  select jsonb_build_object(
    'items',
    coalesce((
      select jsonb_agg(
        jsonb_build_object(
          'id', source_record_id,
          'company_id', company_id,
          'empresa', empresa,
          'cnpj', cnpj,
          'source', source,
          'category_key', category_key,
          'type', type,
          'origem', origem,
          'status', status,
          'periodo', periodo,
          'document_date', document_date,
          'created_at', created_at,
          'file_path', file_path,
          'chave', chave
        )
        order by coalesce(document_date, created_at::date) desc, created_at desc, source_record_id desc
      )
      from visible
    ), '[]'::jsonb),
    'nextCursor',
    case
      when (select count(*) from base) > (select p.limit_count from params p) then (
        select jsonb_build_object(
          'sortDate', sort_date,
          'createdAt', created_at,
          'id', source_record_id
        )
        from cursor_row
      )
      else null
    end,
    'hasMore', ((select count(*) from base) > (select p.limit_count from params p)),
    'refreshAt', now()
  )
  into v_payload;

  return coalesce(v_payload, jsonb_build_object('items', '[]'::jsonb, 'nextCursor', null, 'hasMore', false, 'refreshAt', now()));
end;
$$;
