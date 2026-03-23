-- Lista e ZIP de NFE/NFC: mesma fonte que get_fiscal_detail_summary (fiscal_documents),
-- evitando descompasso com office_document_index (linhas extras / chaves vazias / índice defasado).

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
  if v_kind in ('NFE_NFC', 'NFE', 'NFC') then
    with params as (
      select
        coalesce(company_ids, '{}'::uuid[]) as company_ids,
        coalesce(array_length(company_ids, 1), 0) as company_count,
        public.normalize_search_text(search_text) as norm_q,
        public.only_digits(search_text) as digits_search,
        btrim(coalesce(search_text, '')) as raw_q,
        least(date_from, date_to) as date_from,
        greatest(date_from, date_to) as date_to,
        lower(coalesce(nullif(btrim(file_kind), ''), 'all')) as file_kind,
        lower(coalesce(nullif(btrim(modelo_filter), ''), 'all')) as modelo_filter,
        greatest(least(coalesce(limit_count, 25), 200), 1) as limit_count
    ),
    candidates as (
      select
        fd.id as source_record_id,
        fd.company_id,
        c.name as empresa,
        c.document as cnpj,
        fd.type,
        fd.chave,
        fd.periodo,
        fd.status::text as status,
        coalesce(fd.document_date, fd.created_at::date) as document_date,
        fd.created_at,
        fd.file_path,
        case
          when fd.type = 'NFE' then '55'
          when fd.type = 'NFC' then '65'
          else null
        end as modelo,
        null::text as tipo_certidao,
        case
          when lower(coalesce(fd.file_path, '')) like '%.pdf' then 'pdf'
          when lower(coalesce(fd.file_path, '')) like '%.xml' then 'xml'
          else null
        end as file_extension,
        case
          when fd.file_path ilike '%/Recebidas/%' then 'recebidas'
          when fd.file_path ilike '%/Emitidas/%' then 'emitidas'
          else null
        end as origem,
        public.office_document_search_blob(
          c.name,
          c.document,
          fd.type,
          fd.status::text,
          fd.periodo,
          fd.file_path,
          fd.chave,
          case
            when fd.file_path ilike '%/Recebidas/%' then 'recebidas'
            when fd.file_path ilike '%/Emitidas/%' then 'emitidas'
            else null
          end,
          case
            when fd.type = 'NFE' then '55'
            when fd.type = 'NFC' then '65'
            else null
          end,
          null
        ) as search_text_normalized
      from public.fiscal_documents fd
      join public.companies c on c.id = fd.company_id and c.office_id = fd.office_id
      join params p on true
      where fd.office_id = v_office_id
        and coalesce(btrim(fd.file_path), '') <> ''
        and (
          (v_kind = 'NFE' and fd.type = 'NFE')
          or (v_kind = 'NFC' and fd.type = 'NFC')
          or (v_kind = 'NFE_NFC' and fd.type in ('NFE', 'NFC'))
        )
        and (p.company_count = 0 or fd.company_id = any (p.company_ids))
        and (p.date_from is null or coalesce(fd.document_date, fd.created_at::date) >= p.date_from)
        and (p.date_to is null or coalesce(fd.document_date, fd.created_at::date) <= p.date_to)
        and (
          p.norm_q = ''
          or search_text_normalized like '%' || p.norm_q || '%'
          or (p.digits_search <> '' and public.only_digits(coalesce(c.document, '')) like '%' || p.digits_search || '%')
          or coalesce(fd.chave, '') like '%' || p.raw_q || '%'
        )
        and (
          p.file_kind = 'all'
          or (p.file_kind = 'xml' and lower(coalesce(fd.file_path, '')) like '%.xml')
          or (p.file_kind = 'pdf' and lower(coalesce(fd.file_path, '')) like '%.pdf')
        )
        and (
          p.modelo_filter = 'all'
          or v_kind <> 'NFE_NFC'
          or coalesce(
            case
              when fd.type = 'NFE' then '55'
              when fd.type = 'NFC' then '65'
              else null
            end,
            ''
          ) = p.modelo_filter
        )
    ),
    deduped as (
      select distinct on (coalesce(nullif(btrim(c.chave), ''), c.source_record_id::text))
        c.*
      from candidates c
      order by
        coalesce(nullif(btrim(c.chave), ''), c.source_record_id::text),
        coalesce(c.document_date, c.created_at::date) desc,
        c.created_at desc,
        c.source_record_id desc
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
          < (
            cursor_sort_date,
            coalesce(cursor_created_at, 'infinity'::timestamptz),
            coalesce(cursor_id, 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid)
          )
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
  end if;

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
        p.certidao_tipo_filter = 'all'
        or v_kind <> 'CERTIDOES'
        or coalesce(i.tipo_certidao, '') = p.certidao_tipo_filter
      )
    order by coalesce(i.document_date, i.created_at::date) desc, i.created_at desc, i.source_record_id desc
  ),
  deduped as (
    select distinct on (coalesce(i.file_path, ''))
      i.*
    from filtered i
    order by
      coalesce(i.file_path, ''),
      coalesce(i.document_date, i.created_at::date) desc,
      i.created_at desc,
      i.source_record_id desc
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
        < (
          cursor_sort_date,
          coalesce(cursor_created_at, 'infinity'::timestamptz),
          coalesce(cursor_id, 'ffffffff-ffff-ffff-ffff-ffffffffffff'::uuid)
        )
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

create or replace function public.get_fiscal_detail_document_zip_paths(
  detail_kind text,
  company_ids uuid[] default null,
  search_text text default null,
  date_from date default null,
  date_to date default null,
  file_kind text default null,
  origem_filter text default null,
  modelo_filter text default null,
  limit_count integer default 50000
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
  if v_kind not in ('NFS', 'NFE_NFC') then
    return '[]'::jsonb;
  end if;

  if v_kind = 'NFE_NFC' then
    with params as (
      select
        coalesce(company_ids, '{}'::uuid[]) as company_ids,
        coalesce(array_length(company_ids, 1), 0) as company_count,
        public.normalize_search_text(search_text) as norm_q,
        public.only_digits(search_text) as digits_search,
        btrim(coalesce(search_text, '')) as raw_q,
        least(date_from, date_to) as date_from,
        greatest(date_from, date_to) as date_to,
        lower(coalesce(nullif(btrim(file_kind), ''), 'all')) as file_kind,
        lower(coalesce(nullif(btrim(modelo_filter), ''), 'all')) as modelo_filter,
        least(greatest(coalesce(limit_count, 50000), 1), 50000) as limit_count
    ),
    candidates as (
      select
        fd.id,
        fd.file_path,
        c.name as empresa,
        fd.chave,
        coalesce(fd.document_date, fd.created_at::date) as sort_date,
        fd.created_at,
        public.office_document_search_blob(
          c.name,
          c.document,
          fd.type,
          fd.status::text,
          fd.periodo,
          fd.file_path,
          fd.chave,
          case
            when fd.file_path ilike '%/Recebidas/%' then 'recebidas'
            when fd.file_path ilike '%/Emitidas/%' then 'emitidas'
            else null
          end,
          case
            when fd.type = 'NFE' then '55'
            when fd.type = 'NFC' then '65'
            else null
          end,
          null
        ) as search_text_normalized
      from public.fiscal_documents fd
      join public.companies c on c.id = fd.company_id and c.office_id = fd.office_id
      join params p on true
      where fd.office_id = v_office_id
        and coalesce(btrim(fd.file_path), '') <> ''
        and fd.type in ('NFE', 'NFC')
        and (p.company_count = 0 or fd.company_id = any (p.company_ids))
        and (p.date_from is null or coalesce(fd.document_date, fd.created_at::date) >= p.date_from)
        and (p.date_to is null or coalesce(fd.document_date, fd.created_at::date) <= p.date_to)
        and (
          p.norm_q = ''
          or search_text_normalized like '%' || p.norm_q || '%'
          or (p.digits_search <> '' and public.only_digits(coalesce(c.document, '')) like '%' || p.digits_search || '%')
          or coalesce(fd.chave, '') like '%' || p.raw_q || '%'
        )
        and (
          p.file_kind = 'all'
          or (p.file_kind = 'xml' and lower(coalesce(fd.file_path, '')) like '%.xml')
          or (p.file_kind = 'pdf' and lower(coalesce(fd.file_path, '')) like '%.pdf')
        )
        and (
          p.modelo_filter = 'all'
          or coalesce(
            case
              when fd.type = 'NFE' then '55'
              when fd.type = 'NFC' then '65'
              else null
            end,
            ''
          ) = p.modelo_filter
        )
    ),
    deduped as (
      select distinct on (coalesce(nullif(btrim(c.chave), ''), c.id::text))
        c.file_path,
        c.empresa,
        c.sort_date,
        c.created_at,
        c.id
      from candidates c
      order by
        coalesce(nullif(btrim(c.chave), ''), c.id::text),
        c.sort_date desc,
        c.created_at desc,
        c.id desc
    )
    select coalesce(
      (
        select jsonb_agg(jsonb_build_object('file_path', file_path, 'empresa', empresa))
        from (select file_path, empresa from deduped limit (select limit_count from params)) s
      ),
      '[]'::jsonb
    )
    into v_payload;

    return coalesce(v_payload, '[]'::jsonb);
  end if;

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
      least(greatest(coalesce(limit_count, 50000), 1), 50000) as limit_count
  ),
  filtered as (
    select
      i.file_path,
      i.empresa,
      coalesce(i.document_date, i.created_at::date) as sort_date,
      i.created_at,
      i.source_record_id
    from public.office_document_index i
    join params p on true
    where i.office_id = v_office_id
      and i.file_path is not null
      and btrim(i.file_path) <> ''
      and i.source = 'fiscal'
      and i.type = 'NFS'
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
        or coalesce(i.origem, '') = p.origem_filter
      )
  ),
  deduped as (
    select distinct on (file_path)
      file_path,
      empresa
    from filtered
    order by file_path, sort_date desc, created_at desc, source_record_id desc
    limit (select limit_count from params)
  )
  select coalesce(
    (select jsonb_agg(jsonb_build_object('file_path', file_path, 'empresa', empresa)) from deduped),
    '[]'::jsonb
  )
  into v_payload;

  return coalesce(v_payload, '[]'::jsonb);
end;
$$;
