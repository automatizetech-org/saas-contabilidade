-- Cards NFE/NFC: nfeCount/nfcCount e disponíveis alinhados a documentos distintos (chave/id), não linhas duplicadas.
-- Lista NFE/NFC: dedupe por chave de acesso quando existir (evita 2 linhas para a mesma nota com file_path diferente).

create or replace function public.get_fiscal_detail_summary(
  detail_kind text,
  company_ids uuid[] default null,
  date_from date default null,
  date_to date default null
)
returns jsonb
language sql
stable
security definer
set search_path = public
as $$
with params as (
  select
    public.current_office_id() as office_id,
    upper(coalesce(detail_kind, 'NFS')) as detail_kind,
    coalesce(company_ids, '{}'::uuid[]) as company_ids,
    coalesce(array_length(company_ids, 1), 0) as company_count,
    least(date_from, date_to) as date_from,
    greatest(date_from, date_to) as date_to,
    current_date as today,
    to_char(current_date, 'YYYY-MM') as current_month
),
docs as (
  select
    d.id,
    d.company_id,
    d.type,
    d.periodo,
    d.status,
    d.file_path,
    d.chave,
    coalesce(d.document_date, d.created_at::date) as ref_date
  from public.fiscal_documents d
  join params p on p.office_id = d.office_id
  where (
      (p.detail_kind = 'NFS' and d.type = 'NFS')
      or (p.detail_kind = 'NFE' and d.type = 'NFE')
      or (p.detail_kind = 'NFC' and d.type = 'NFC')
      or (p.detail_kind = 'NFE_NFC' and d.type in ('NFE', 'NFC'))
    )
    and (p.company_count = 0 or d.company_id = any (p.company_ids))
    and (p.date_from is null or coalesce(d.document_date, d.created_at::date) >= p.date_from)
    and (p.date_to is null or coalesce(d.document_date, d.created_at::date) <= p.date_to)
),
month_bounds as (
  select
    coalesce(date_trunc('month', (select date_from from params))::date, date_trunc('month', current_date)::date - interval '11 month') as start_month,
    coalesce(date_trunc('month', (select date_to from params))::date, date_trunc('month', current_date)::date) as end_month
),
month_series as (
  select to_char(gs::date, 'YYYY-MM') as month_key
  from month_bounds b
  cross join generate_series(b.start_month, b.end_month, interval '1 month') as gs
),
docs_by_month as (
  select to_char(ref_date, 'YYYY-MM') as month_key, count(distinct coalesce(chave, id::text))::int as value
  from docs
  group by 1
),
summary as (
  select
    count(distinct coalesce(chave, id::text))::int as total_documents,
    count(distinct case
      when nullif(btrim(coalesce(file_path, '')), '') is not null
      then coalesce(chave, id::text)
    end)::int as available_documents,
    count(distinct coalesce(chave, id::text)) filter (where left(coalesce(periodo, ''), 7) = (select current_month from params))::int as this_month,
    count(distinct case when type = 'NFE' then coalesce(chave, id::text) end)::int as nfe_count,
    count(distinct case when type = 'NFC' then coalesce(chave, id::text) end)::int as nfc_count
  from docs
)
select jsonb_build_object(
  'cards',
  jsonb_build_object(
    'totalDocuments', coalesce((select total_documents from summary), 0),
    'availableDocuments', coalesce((select available_documents from summary), 0),
    'thisMonth', coalesce((select this_month from summary), 0),
    'nfeCount', coalesce((select nfe_count from summary), 0),
    'nfcCount', coalesce((select nfc_count from summary), 0)
  ),
  'byMonth',
  coalesce((
    select jsonb_agg(
      jsonb_build_object('key', ms.month_key, 'value', coalesce(dm.value, 0))
      order by ms.month_key
    )
    from month_series ms
    left join docs_by_month dm on dm.month_key = ms.month_key
  ), '[]'::jsonb)
);
$$;

create or replace function public.get_fiscal_detail_documents_cursor(
  detail_kind text,
  company_ids uuid[] default null,
  search_text text default null,
  date_from date default null,
  date_to date default null,
  file_kind text default null,
  origem_filter text default null,
  modelo_filter text default null,
  certidao_tipo_filter text default null,
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
  v_kind text := upper(coalesce(detail_kind, 'NFS'));
  v_has_index boolean;
  v_payload jsonb;
begin
  select exists(select 1 from public.office_document_index where office_id = v_office_id limit 1)
  into v_has_index;

  if not v_has_index then
    perform public.refresh_office_document_index(v_office_id);
  end if;

  with params as (
    select
      coalesce(company_ids, '{}'::uuid[]) as company_ids,
      coalesce(array_length(company_ids, 1), 0) as company_count,
      public.normalize_search_text(search_text) as search_text,
      public.only_digits(search_text) as digits_search,
      least(date_from, date_to) as date_from,
      greatest(date_from, date_to) as date_to,
      lower(coalesce(nullif(btrim(file_kind), ''), 'all')) as file_kind,
      lower(coalesce(nullif(btrim(origem_filter), ''), 'all')) as origem_filter,
      lower(coalesce(nullif(btrim(modelo_filter), ''), 'all')) as modelo_filter,
      lower(coalesce(nullif(btrim(certidao_tipo_filter), ''), 'all')) as certidao_tipo_filter,
      greatest(least(coalesce(limit_count, 25), 200), 1) as limit_count
  ),
  filtered as (
    select i.*
    from public.office_document_index i
    join params p on true
    where i.office_id = v_office_id
      and (
        (v_kind = 'CERTIDOES' and i.source = 'certidoes')
        or (v_kind = 'NFS' and i.source = 'fiscal' and i.type = 'NFS')
        or (v_kind = 'NFE' and i.source = 'fiscal' and i.type = 'NFE')
        or (v_kind = 'NFC' and i.source = 'fiscal' and i.type = 'NFC')
        or (v_kind = 'NFE_NFC' and i.source = 'fiscal' and i.type in ('NFE', 'NFC'))
      )
      and (p.company_count = 0 or i.company_id = any (p.company_ids))
      and (p.date_from is null or coalesce(i.document_date, i.created_at::date) >= p.date_from)
      and (p.date_to is null or coalesce(i.document_date, i.created_at::date) <= p.date_to)
      and (
        p.search_text = ''
        or i.search_text_normalized like '%' || p.search_text || '%'
        or (p.digits_search <> '' and public.only_digits(coalesce(i.cnpj, '')) like '%' || p.digits_search || '%')
        or coalesce(i.chave, '') like '%' || btrim(coalesce(search_text, '')) || '%'
      )
      and (
        p.file_kind = 'all'
        or (p.file_kind = 'xml' and i.file_extension = 'xml')
        or (p.file_kind = 'pdf' and i.file_extension = 'pdf')
      )
      and (
        p.origem_filter = 'all'
        or v_kind <> 'NFS'
        or coalesce(i.origem, '') = p.origem_filter
      )
      and (
        p.modelo_filter = 'all'
        or v_kind <> 'NFE_NFC'
        or coalesce(i.modelo, '') = p.modelo_filter
      )
      and (
        p.certidao_tipo_filter = 'all'
        or v_kind <> 'CERTIDOES'
        or coalesce(i.tipo_certidao, '') = p.certidao_tipo_filter
      )
    order by coalesce(i.document_date, i.created_at::date) desc, i.created_at desc, i.source_record_id desc
  ),
  deduped as (
    select distinct on (
      case
        when v_kind in ('NFE_NFC', 'NFE', 'NFC')
          and nullif(btrim(coalesce(i.chave, '')), '') is not null
        then 'c:' || lower(btrim(coalesce(i.chave, '')))
        else 'f:' || coalesce(nullif(btrim(coalesce(i.file_path, '')), ''), i.source_record_id::text)
      end
    )
      i.*
    from filtered i
    order by
      case
        when v_kind in ('NFE_NFC', 'NFE', 'NFC')
          and nullif(btrim(coalesce(i.chave, '')), '') is not null
        then 'c:' || lower(btrim(coalesce(i.chave, '')))
        else 'f:' || coalesce(nullif(btrim(coalesce(i.file_path, '')), ''), i.source_record_id::text)
      end,
      coalesce(i.document_date, i.created_at::date) desc, i.created_at desc, i.source_record_id desc
  ),
  deduped_ordered as (
    select *
    from deduped
    order by coalesce(document_date, created_at::date) desc, created_at desc, source_record_id desc
  ),
  base as (
    select *
    from deduped_ordered
    where (
      cursor_sort_date is null
      or (
        (coalesce(document_date, created_at::date), created_at, source_record_id)
        < (cursor_sort_date, coalesce(cursor_created_at, 'infinity'::timestamptz), coalesce(cursor_id, 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid))
      )
    )
    limit (select limit_count + 1 from params)
  ),
  visible as (
    select * from base
    limit (select limit_count from params)
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
          'type', type,
          'chave', chave,
          'periodo', periodo,
          'status', status,
          'document_date', document_date,
          'created_at', created_at,
          'file_path', file_path,
          'origem', origem,
          'modelo', modelo,
          'tipo_certidao', tipo_certidao
        )
        order by coalesce(document_date, created_at::date) desc, created_at desc, source_record_id desc
      )
      from visible
    ), '[]'::jsonb),
    'nextCursor',
    case
      when (select count(*) from base) > (select limit_count from params) then (
        select jsonb_build_object(
          'sortDate', sort_date,
          'createdAt', created_at,
          'id', source_record_id
        )
        from cursor_row
      )
      else null
    end,
    'hasMore', ((select count(*) from base) > (select limit_count from params)),
    'refreshAt', now()
  )
  into v_payload;

  return coalesce(v_payload, jsonb_build_object('items', '[]'::jsonb, 'nextCursor', null, 'hasMore', false, 'refreshAt', now()));
end;
$$;
